"""
Upgrade Readiness Report Workflow

Orchestrates all data sources (CMC, Cloud Portal, Qdrant, upgrade research,
Zendesk customer support tickets) to produce an opinionated readiness assessment
with a rating and Excel checklist data.

Output:
- Overall readiness: READY / READY WITH CAVEATS / NOT READY
- Blockers, warnings, considerations
- Known upgrade issues from customer support data
- Environment summary
- Version research
- Pre-upgrade checklist data (JSON for write_checklist_excel)
"""

import json
import os
import sys
from datetime import datetime
from typing import TypedDict

from langgraph.graph import StateGraph, END

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class ReadinessState(TypedDict):
    # Inputs
    cmc_cluster_name: str
    cloud_cluster_name: str
    from_version: str
    to_version: str

    # Collected data
    cluster_data: dict
    cluster_metadata: dict
    validation_checks: dict
    cloud_metadata: dict
    zendesk_issues: dict  # NEW — from Zendesk collection workflow
    upgrade_knowledge: list
    upgrade_research: dict
    checklist_cell_values: dict

    # Output
    readiness_assessment: dict
    report: str
    errors: list


# ---------------------------------------------------------------------------
# Node 1: Collect CMC data (metadata + validation checks)
# ---------------------------------------------------------------------------

def collect_cmc_data(state: ReadinessState) -> ReadinessState:
    """Fetch cluster data from CMC, extract metadata, and run validation checks."""
    errors = list(state.get("errors", []))
    try:
        from clients.cmc_client import CMCClient
        from tools.extract_cluster_metadata import extract_cluster_metadata
        from tools.validation_checks import (
            check_service_status,
            check_memory_status,
            check_cluster_configuration,
            check_infrastructure_services,
            check_node_topology,
            check_connectors,
            check_tenants,
            check_email_configuration,
            check_notebook_sqli_status,
            check_database_migration,
        )

        client = CMCClient()
        cluster_data = client.get_cluster(state["cmc_cluster_name"])
        metadata = extract_cluster_metadata(cluster_data)

        checks = {
            "Service Status": check_service_status(cluster_data),
            "Memory Status": check_memory_status(cluster_data),
            "Cluster Configuration": check_cluster_configuration(cluster_data),
            "Infrastructure Services": check_infrastructure_services(cluster_data),
            "Node Topology": check_node_topology(cluster_data),
            "Connectors": check_connectors(cluster_data),
            "Tenants": check_tenants(cluster_data),
            "Email Configuration": check_email_configuration(cluster_data),
            "Notebook & SQLi": check_notebook_sqli_status(cluster_data),
            "Database Migration": check_database_migration(cluster_data),
        }

        return {
            **state,
            "cluster_data": cluster_data,
            "cluster_metadata": metadata,
            "validation_checks": checks,
        }
    except Exception as e:
        errors.append(f"CMC data collection failed: {e}")
        return {
            **state,
            "cluster_data": {},
            "cluster_metadata": {},
            "validation_checks": {},
            "errors": errors,
        }


# ---------------------------------------------------------------------------
# Node 2: Collect Zendesk customer support data (fault-tolerant)
# ---------------------------------------------------------------------------

def collect_zendesk_data(state: ReadinessState) -> ReadinessState:
    """Automatically gather upgrade-related customer support issues from Zendesk."""
    errors = list(state.get("errors", []))
    from_v = state.get("from_version", "")
    to_v = state.get("to_version", "")

    if not from_v or not to_v:
        errors.append("Zendesk collection skipped: upgrade versions not yet available")
        return {**state, "zendesk_issues": {}, "errors": errors}

    try:
        from workflows.collect_zendesk_issues import run_zendesk_collection

        findings = run_zendesk_collection(from_v, to_v)
        return {**state, "zendesk_issues": findings}
    except Exception as e:
        errors.append(f"Zendesk collection failed: {e}")
        return {**state, "zendesk_issues": {}, "errors": errors}


# ---------------------------------------------------------------------------
# Node 3: Collect Cloud Portal data (fault-tolerant)
# ---------------------------------------------------------------------------

def collect_cloud_data(state: ReadinessState) -> ReadinessState:
    """Fetch cloud metadata from Cloud Portal API."""
    errors = list(state.get("errors", []))
    cloud_cluster_name = state.get("cloud_cluster_name", "")

    if not cloud_cluster_name:
        errors.append("Cloud cluster name not available — skipping cloud data")
        return {**state, "cloud_metadata": {}, "errors": errors}

    try:
        from clients.cloud_portal_client import CloudPortalClient

        cloud_client = CloudPortalClient()
        user_id = cloud_client.get_user_id()
        cluster = cloud_client.find_cluster(user_id, cloud_cluster_name)

        if not cluster:
            errors.append(f"Cloud cluster '{cloud_cluster_name}' not found in Cloud Portal")
            return {**state, "cloud_metadata": {}, "errors": errors}

        cloud_meta = {
            "spark_version": cluster.get("incortaSparkVersion"),
            "python_version": cluster.get("pythonVersion"),
            "build": cluster.get("customBuild"),
            "build_id": cluster.get("customBuildID"),
            "platform": cluster.get("platform"),
            "region": cluster.get("region"),
            "status": cluster.get("status"),
            "data_size_gb": cluster.get("dsize"),
            "loader_size_gb": cluster.get("dsizeLoader"),
            "cmc_size_gb": cluster.get("dsizeCmc"),
            "available_disk_gb": cluster.get("availableDisk"),
            "consumed_data_gb": cluster.get("consumedData"),
            "sqli_enabled": cluster.get("sqliEnabled", False),
            "data_agent_enabled": cluster.get("enableDataAgent", False),
            "last_upgrade": cluster.get("initiatedUpgradeAt"),
            "min_executors": cluster.get("minExecutors"),
            "max_executors": cluster.get("maxExecutors"),
        }
        # Auto-detect from_version from cloud build if not already provided
        from_version = state.get("from_version", "")
        if not from_version and cloud_meta.get("build"):
            from_version = cloud_meta["build"]

        return {**state, "cloud_metadata": cloud_meta, "from_version": from_version}
    except Exception as e:
        errors.append(f"Cloud data collection failed: {e}")
        return {**state, "cloud_metadata": {}, "errors": errors}


# ---------------------------------------------------------------------------
# Node 3: Collect upgrade knowledge from Qdrant
# ---------------------------------------------------------------------------

def collect_upgrade_knowledge(state: ReadinessState) -> ReadinessState:
    """Search knowledge base for upgrade considerations between versions."""
    errors = list(state.get("errors", []))
    from_v = state.get("from_version", "")
    to_v = state.get("to_version", "")

    if not from_v or not to_v:
        return {**state, "upgrade_knowledge": []}

    try:
        from tools.qdrant_tool import search_knowledge_base

        query = f"upgrade considerations from {from_v} to {to_v}"
        result = search_knowledge_base({"query": query, "limit": 10})
        knowledge = result.get("results", []) if isinstance(result, dict) else []
        return {**state, "upgrade_knowledge": knowledge}
    except Exception as e:
        errors.append(f"Upgrade knowledge search failed: {e}")
        return {**state, "upgrade_knowledge": [], "errors": errors}


# ---------------------------------------------------------------------------
# Node 4: Collect upgrade research (release notes, known issues, community)
# ---------------------------------------------------------------------------

def collect_upgrade_research(state: ReadinessState) -> ReadinessState:
    """Run the full upgrade path research workflow."""
    errors = list(state.get("errors", []))
    from_v = state.get("from_version", "")
    to_v = state.get("to_version", "")

    if not from_v or not to_v:
        return {**state, "upgrade_research": {}}

    try:
        from workflows.upgrade_research import research_upgrade_path

        report = research_upgrade_path(from_v, to_v)
        return {**state, "upgrade_research": {"report": report}}
    except Exception as e:
        errors.append(f"Upgrade research failed: {e}")
        return {**state, "upgrade_research": {}, "errors": errors}


# ---------------------------------------------------------------------------
# Node 5: Collect checklist data for Excel (always runs)
# ---------------------------------------------------------------------------

def collect_checklist_data(state: ReadinessState) -> ReadinessState:
    """Map collected data to Excel cell values using the checklist workflow."""
    try:
        from workflows.checklist_workflow import map_data_to_cells

        # Build a state-like dict compatible with map_data_to_cells
        checklist_input = {
            "cmc_cluster_name": state.get("cmc_cluster_name", ""),
            "cloud_cluster_name": state.get("cloud_cluster_name", ""),
            "from_version": state.get("from_version", ""),
            "to_version": state.get("to_version", ""),
            "cluster_metadata": state.get("cluster_metadata", {}),
            "validation_checks": state.get("validation_checks", {}),
            "cloud_metadata": state.get("cloud_metadata", {}),
            "upgrade_knowledge": state.get("upgrade_knowledge", []),
            "cell_values": {},
            "errors": [],
        }
        result = map_data_to_cells(checklist_input)
        return {**state, "checklist_cell_values": result.get("cell_values", {})}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Checklist data mapping failed: {e}")
        return {**state, "checklist_cell_values": {}, "errors": errors}


# ---------------------------------------------------------------------------
# Node 6: Assess readiness (the core new logic)
# ---------------------------------------------------------------------------

def assess_readiness(state: ReadinessState) -> ReadinessState:
    """Synthesize all collected data into an opinionated readiness assessment."""
    metadata = state.get("cluster_metadata", {})
    checks = state.get("validation_checks", {})
    cloud = state.get("cloud_metadata", {})
    knowledge = state.get("upgrade_knowledge", [])
    zendesk = state.get("zendesk_issues", {})
    errors = list(state.get("errors", []))

    # --- 1. Collect blockers and warnings from validation checks ---
    blockers = []
    warnings = []
    for check_name, result in checks.items():
        status = result.get("status", "")
        details = result.get("details", [])
        if status == "FAIL":
            # Take the first few meaningful details
            fail_details = [d for d in details if d.startswith("\u2717") or d.startswith("FAIL")][:3]
            if not fail_details:
                fail_details = details[:3]
            blockers.append(f"[{check_name}] {'; '.join(fail_details)}")
        elif status == "WARNING":
            warn_details = [d for d in details if d.startswith("\u26a0") or d.startswith("\u2717")][:3]
            if not warn_details:
                warn_details = details[:2]
            warnings.append(f"[{check_name}] {'; '.join(warn_details)}")

    # --- 2. Add blockers/warnings from risk assessment ---
    risks = metadata.get("risks", {})
    for b in risks.get("blockers", []):
        blockers.append(f"[Risk Assessment] {b}")
    for w in risks.get("warnings", []):
        warnings.append(f"[Risk Assessment] {w}")

    # --- 2b. Add blockers/warnings/considerations from Zendesk data ---
    for b in zendesk.get("blockers", []):
        blockers.append(b)
    for w in zendesk.get("warnings", []):
        warnings.append(w)

    # --- 3. Key upgrade considerations ---
    considerations = []

    # Zendesk considerations
    for c in zendesk.get("considerations", []):
        considerations.append(c)

    # Database migration
    db = metadata.get("database", {})
    if db.get("migration_needed"):
        considerations.append(
            f"MAJOR: Oracle-to-MySQL migration required. Current DB type is {db.get('db_type', 'Oracle')}. "
            "This is a significant pre-upgrade task that must be completed before upgrading."
        )

    # HA topology
    topo = metadata.get("topology", {})
    if topo.get("is_ha"):
        considerations.append(
            f"HA cluster with {topo.get('node_count', '?')} nodes. "
            "Upgrade requires coordinated rolling restart. Plan for extended maintenance window."
        )
    elif topo.get("node_count", 1) > 1:
        considerations.append(
            f"Multi-node cluster ({topo.get('node_count')} nodes). "
            "All nodes must be upgraded. Coordinate downtime."
        )

    # Knowledge base version-specific notes
    if knowledge:
        high_relevance = [k for k in knowledge if k.get("score", 0) > 0.5]
        for item in high_relevance[:5]:
            title = item.get("title", "Untitled")
            text = item.get("text", "")[:200]
            considerations.append(f"Version Note: {title} -- {text}")

    # Data collection errors as considerations
    if errors:
        for err in errors:
            considerations.append(f"Data Gap: {err}")

    # --- 4. Compute overall readiness rating ---
    if blockers:
        rating = "NOT READY"
        rating_detail = (
            f"{len(blockers)} blocker(s) must be resolved before upgrade can proceed."
        )
    elif warnings or errors:
        rating = "READY WITH CAVEATS"
        caveat_count = len(warnings) + len(errors)
        rating_detail = (
            f"{caveat_count} item(s) to review before proceeding. "
            "Upgrade can proceed but review warnings and data gaps below."
        )
    else:
        rating = "READY"
        rating_detail = "All checks passed. Cluster appears ready for upgrade."

    assessment = {
        "rating": rating,
        "rating_detail": rating_detail,
        "blockers": blockers,
        "warnings": warnings,
        "considerations": considerations,
        "environment_summary": {
            "cluster_name": metadata.get("cluster_name", state.get("cmc_cluster_name", "Unknown")),
            "deployment": metadata.get("deployment_type", {}).get("deployment_type", "Unknown"),
            "cloud_provider": metadata.get("deployment_type", {}).get("cloud_provider", "Unknown"),
            "db_type": db.get("db_type", "Unknown"),
            "topology": topo.get("topology_type", "Unknown"),
            "node_count": topo.get("node_count", "?"),
            "is_ha": topo.get("is_ha", False),
            "spark_version": cloud.get("spark_version", "Unknown"),
            "python_version": cloud.get("python_version", "Unknown"),
        },
        "from_version": state.get("from_version", "Unknown"),
        "to_version": state.get("to_version", "Unknown"),
        "checks_summary": {
            name: result.get("status", "Unknown") for name, result in checks.items()
        },
        "risk_level": risks.get("risk_level", "UNKNOWN"),
        "data_gaps": errors,
        "zendesk_findings": zendesk,
    }

    return {**state, "readiness_assessment": assessment}


# ---------------------------------------------------------------------------
# Node 7: Generate markdown report
# ---------------------------------------------------------------------------

def generate_report(state: ReadinessState) -> ReadinessState:
    """Format the readiness assessment into a final markdown report."""
    assessment = state.get("readiness_assessment", {})
    research = state.get("upgrade_research", {})
    checklist = state.get("checklist_cell_values", {})

    lines = []

    # --- Header ---
    lines.append("# Upgrade Readiness Report")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(
        f"**Upgrade Path:** {assessment.get('from_version', '?')} "
        f"\u2192 {assessment.get('to_version', '?')}"
    )
    lines.append("")

    # --- Overall Rating ---
    rating = assessment.get("rating", "UNKNOWN")
    lines.append(f"## Overall Readiness: {rating}")
    lines.append(f"_{assessment.get('rating_detail', '')}_")
    lines.append("")

    # --- Environment Summary ---
    env = assessment.get("environment_summary", {})
    lines.append("## Environment Summary")
    lines.append("| Property | Value |")
    lines.append("|----------|-------|")
    lines.append(f"| Cluster | {env.get('cluster_name', '?')} |")
    lines.append(f"| Deployment | {env.get('deployment', '?')} ({env.get('cloud_provider', '?')}) |")
    lines.append(f"| Database | {env.get('db_type', '?')} |")
    lines.append(f"| Topology | {env.get('topology', '?')} ({env.get('node_count', '?')} nodes) |")
    lines.append(f"| HA | {'Yes' if env.get('is_ha') else 'No'} |")
    lines.append(f"| Spark | {env.get('spark_version', '?')} |")
    lines.append(f"| Python | {env.get('python_version', '?')} |")
    lines.append(f"| Risk Level | {assessment.get('risk_level', '?')} |")
    lines.append("")

    # --- Blockers ---
    blockers = assessment.get("blockers", [])
    if blockers:
        lines.append("## Blockers (Must Resolve Before Upgrade)")
        for i, b in enumerate(blockers, 1):
            lines.append(f"{i}. {b}")
        lines.append("")

    # --- Warnings ---
    warnings_list = assessment.get("warnings", [])
    if warnings_list:
        lines.append("## Warnings (Review Before Upgrade)")
        for i, w in enumerate(warnings_list, 1):
            lines.append(f"{i}. {w}")
        lines.append("")

    # --- Validation Checks Summary ---
    checks_summary = assessment.get("checks_summary", {})
    if checks_summary:
        lines.append("## Validation Checks")
        lines.append("| Check | Status |")
        lines.append("|-------|--------|")
        for name, status in checks_summary.items():
            lines.append(f"| {name} | {status} |")
        lines.append("")

    # --- Known Upgrade Issues (from Customer Support Data) ---
    zendesk = assessment.get("zendesk_findings", {})
    if zendesk.get("data_available"):
        lines.append("## Known Upgrade Issues (from Customer Support Data)")
        lines.append("")

        # Version-Specific Issues
        vp = zendesk.get("version_pair_issues", {})
        pairs = vp.get("version_pairs", [])
        if pairs:
            lines.append("### Version-Specific Issues")
            for p in pairs:
                lines.append(
                    f"- **{p.get('from', '?')} \u2192 {p.get('to', '?')}**: "
                    f"{p.get('issue_count', 0)} issue(s), "
                    f"{p.get('affected_accounts', 0)} account(s) affected, "
                    f"{p.get('resolved_count', 0)} resolved"
                )
            lines.append("")

        # Risk Assessment
        risk = zendesk.get("risk_patterns", {})
        if risk.get("risk_level") and risk["risk_level"] != "UNKNOWN":
            lines.append("### Risk Assessment")
            lines.append(
                f"- **Risk Level:** {risk['risk_level']} "
                f"({risk.get('total_issues', 0)} total, "
                f"{risk.get('critical_issues', 0)} critical)"
            )
            if risk.get("avg_resolution_days"):
                lines.append(
                    f"- **Avg Resolution:** {risk['avg_resolution_days']} days "
                    f"(max {risk.get('max_resolution_days', '?')} days)"
                )
            for w in risk.get("warnings", []):
                lines.append(f"- {w}")
            lines.append("")

        # Environment-Specific Considerations
        env_issues = zendesk.get("environment_issues", {})
        by_env = env_issues.get("by_environment", {})
        if by_env:
            lines.append("### Environment-Specific Considerations")
            for env_name, env_data in by_env.items():
                lines.append(
                    f"- **{env_name}**: {env_data.get('issue_count', 0)} issue(s), "
                    f"{env_data.get('affected_accounts', 0)} account(s)"
                )
            lines.append("")

        # Customer Impact & Satisfaction
        sat = zendesk.get("satisfaction_data", {})
        if sat.get("total_tickets", 0) > 0:
            lines.append("### Customer Impact & Satisfaction")
            lines.append(f"- **Total Tickets:** {sat.get('total_tickets', 0)}")
            if sat.get("rated_count", 0) > 0:
                lines.append(
                    f"- **Avg Satisfaction:** {sat.get('avg_satisfaction', 0)}/5 "
                    f"({sat.get('rated_count', 0)} ratings)"
                )
            lines.append(f"- **Resolved:** {sat.get('resolved_count', 0)}")
            if sat.get("avg_resolution_days"):
                lines.append(
                    f"- **Avg Resolution Time:** {sat['avg_resolution_days']} days"
                )
            lines.append("")

        # Top Issues (from complete details)
        complete = zendesk.get("complete_issues", {})
        issues = complete.get("issues", [])
        if issues:
            lines.append("### Top Reported Issues")
            lines.append("| # | Subject | Priority | Status | Environment | Days |")
            lines.append("|---|---------|----------|--------|-------------|------|")
            for i, issue in enumerate(issues[:10], 1):
                lines.append(
                    f"| {i} | {issue.get('subject', '?')[:60]} "
                    f"| {issue.get('priority', '?')} "
                    f"| {issue.get('status', '?')} "
                    f"| {issue.get('environment', '?')} "
                    f"| {issue.get('days_to_resolution', '?')} |"
                )
            if len(issues) > 10:
                lines.append(f"_... and {len(issues) - 10} more_")
            lines.append("")
    elif zendesk:
        # Schema was not ready or no data found
        lines.append("## Known Upgrade Issues (from Customer Support Data)")
        lines.append("_Customer support data not available._")
        zendesk_gaps = zendesk.get("data_gaps", [])
        if zendesk_gaps:
            for gap in zendesk_gaps:
                lines.append(f"- {gap}")
        lines.append("")

    # --- Key Considerations ---
    considerations = assessment.get("considerations", [])
    if considerations:
        lines.append("## Key Upgrade Considerations")
        for i, c in enumerate(considerations, 1):
            lines.append(f"{i}. {c}")
        lines.append("")

    # --- Version Research ---
    research_report = research.get("report", "")
    if research_report:
        lines.append("## Version Research")
        lines.append(research_report)
        lines.append("")

    # --- Data Gaps ---
    data_gaps = assessment.get("data_gaps", [])
    if data_gaps:
        lines.append("## Data Gaps")
        lines.append("_The following data sources could not be queried:_")
        for gap in data_gaps:
            lines.append(f"- {gap}")
        lines.append("")

    # --- Checklist Data (always included) ---
    if checklist:
        serializable = {str(k): v for k, v in checklist.items()}
        lines.append("---")
        lines.append("")
        lines.append("## Pre-Upgrade Checklist Data")
        lines.append(
            "_Pass the JSON below to `write_checklist_excel` to generate the Excel report._"
        )
        lines.append(
            f"\n<checklist_data>\n{json.dumps(serializable, indent=2)}\n</checklist_data>"
        )

    return {**state, "report": "\n".join(lines)}


# ---------------------------------------------------------------------------
# Workflow graph
# ---------------------------------------------------------------------------

def _build_workflow():
    workflow = StateGraph(ReadinessState)

    workflow.add_node("collect_cloud", collect_cloud_data)
    workflow.add_node("collect_cmc", collect_cmc_data)
    workflow.add_node("collect_zendesk", collect_zendesk_data)
    workflow.add_node("collect_knowledge", collect_upgrade_knowledge)
    workflow.add_node("collect_research", collect_upgrade_research)
    workflow.add_node("collect_checklist", collect_checklist_data)
    workflow.add_node("assess", assess_readiness)
    workflow.add_node("report", generate_report)

    workflow.set_entry_point("collect_cloud")
    workflow.add_edge("collect_cloud", "collect_cmc")
    workflow.add_edge("collect_cmc", "collect_zendesk")
    workflow.add_edge("collect_zendesk", "collect_knowledge")
    workflow.add_edge("collect_knowledge", "collect_research")
    workflow.add_edge("collect_research", "collect_checklist")
    workflow.add_edge("collect_checklist", "assess")
    workflow.add_edge("assess", "report")
    workflow.add_edge("report", END)

    return workflow.compile()


# ---------------------------------------------------------------------------
# Public entry function
# ---------------------------------------------------------------------------

def run_readiness_report(
    cmc_cluster_name: str,
    to_version: str,
    cloud_cluster_name: str = "",
) -> str:
    """Run the full readiness report workflow.

    Args:
        cmc_cluster_name: CMC cluster name (e.g., 'customCluster')
        to_version: Target Incorta version (e.g., '2024.7.0')
        cloud_cluster_name: Cloud Portal cluster name (optional, inferred from CMC_URL)

    Returns:
        Markdown report with readiness assessment + checklist data JSON
    """
    initial_state = {
        "cmc_cluster_name": cmc_cluster_name,
        "cloud_cluster_name": cloud_cluster_name,
        "from_version": "",  # Auto-detected from Cloud Portal in collect_cloud node
        "to_version": to_version,
        "cluster_data": {},
        "cluster_metadata": {},
        "validation_checks": {},
        "cloud_metadata": {},
        "zendesk_issues": {},
        "upgrade_knowledge": [],
        "upgrade_research": {},
        "checklist_cell_values": {},
        "readiness_assessment": {},
        "report": "",
        "errors": [],
    }

    workflow = _build_workflow()
    result = workflow.invoke(initial_state)
    return result["report"]
