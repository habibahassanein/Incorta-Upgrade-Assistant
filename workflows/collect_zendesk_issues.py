"""
Zendesk Issue Collection Workflow

Automatically gathers upgrade-related customer support data from ZendeskTickets.
Runs a validate-first pattern: checks schema availability once, then executes
6 specialized queries in sequence, and synthesises results into unified findings.

Called automatically by the readiness report workflow — no user action required.
"""

import logging
import os
import sys
from typing import TypedDict

from langgraph.graph import StateGraph, END

# add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.incorta_tools import get_zendesk_schema
from tools.zendesk_helpers import (
    get_upgrade_issues_by_version_pair,
    get_high_risk_upgrade_patterns,
    get_environment_specific_issues,
    get_common_issue_types,
    get_complete_upgrade_issues,
    assess_upgrade_satisfaction,
    get_linked_jira_keys,
    get_customer_jira_links,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class ZendeskCollectionState(TypedDict):
    # Inputs
    from_version: str
    to_version: str
    customer_name: str

    # Schema validation
    schema_ready: bool

    # Collection results (one per helper)
    version_pair_issues: dict
    risk_patterns: dict
    environment_issues: dict
    common_issues: dict
    complete_issues: dict
    satisfaction_data: dict
    linked_jira_data: dict
    customer_jira_data: dict

    # Aggregated output
    zendesk_findings: dict
    errors: list


# ---------------------------------------------------------------------------
# Node 0: Validate schema (gate node — short-circuits if not ready)
# ---------------------------------------------------------------------------

def validate_schema(state: ZendeskCollectionState) -> ZendeskCollectionState:
    """Check schema readiness once. Populates cache for all subsequent queries."""
    errors = list(state.get("errors", []))
    try:
        schema = get_zendesk_schema({"fetch_schema": True})
        ready = schema.get("upgrade_analysis_ready", False)
        if not ready:
            missing = schema.get("missing_upgrade_tables", [])
            error_msg = schema.get("error", "")
            if error_msg:
                errors.append(f"Zendesk schema: {error_msg}")
            elif missing:
                errors.append(f"Zendesk missing tables: {missing}")
        return {**state, "schema_ready": ready, "errors": errors}
    except Exception as e:
        errors.append(f"Zendesk schema validation failed: {e}")
        return {**state, "schema_ready": False, "errors": errors}


# ---------------------------------------------------------------------------
# Nodes 1-6: Collect data (each fault-tolerant)
# ---------------------------------------------------------------------------

def collect_version_pair_issues(state: ZendeskCollectionState) -> ZendeskCollectionState:
    """Query A: Issues by version pair."""
    if not state.get("schema_ready"):
        return {**state, "version_pair_issues": {}}
    try:
        result = get_upgrade_issues_by_version_pair(
            state["from_version"], state["to_version"]
        )
        return {**state, "version_pair_issues": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Zendesk version pair query failed: {e}")
        return {**state, "version_pair_issues": {}, "errors": errors}


def collect_risk_patterns(state: ZendeskCollectionState) -> ZendeskCollectionState:
    """Query B: High-risk upgrade patterns."""
    if not state.get("schema_ready"):
        return {**state, "risk_patterns": {}}
    try:
        result = get_high_risk_upgrade_patterns(
            state["from_version"], state["to_version"]
        )
        return {**state, "risk_patterns": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Zendesk risk patterns query failed: {e}")
        return {**state, "risk_patterns": {}, "errors": errors}


def collect_environment_issues(state: ZendeskCollectionState) -> ZendeskCollectionState:
    """Query C: Environment-specific issues."""
    if not state.get("schema_ready"):
        return {**state, "environment_issues": {}}
    try:
        result = get_environment_specific_issues(
            state["from_version"], state["to_version"]
        )
        return {**state, "environment_issues": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Zendesk environment issues query failed: {e}")
        return {**state, "environment_issues": {}, "errors": errors}


def collect_common_types(state: ZendeskCollectionState) -> ZendeskCollectionState:
    """Query D: Common issue types across all versions."""
    if not state.get("schema_ready"):
        return {**state, "common_issues": {}}
    try:
        result = get_common_issue_types()
        return {**state, "common_issues": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Zendesk common types query failed: {e}")
        return {**state, "common_issues": {}, "errors": errors}


def collect_complete_details(state: ZendeskCollectionState) -> ZendeskCollectionState:
    """Master query: complete issue details with enrichment."""
    if not state.get("schema_ready"):
        return {**state, "complete_issues": {}}
    try:
        result = get_complete_upgrade_issues(
            state["from_version"], state["to_version"]
        )
        return {**state, "complete_issues": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Zendesk complete issues query failed: {e}")
        return {**state, "complete_issues": {}, "errors": errors}


def collect_satisfaction(state: ZendeskCollectionState) -> ZendeskCollectionState:
    """Query E: Customer satisfaction assessment."""
    if not state.get("schema_ready"):
        return {**state, "satisfaction_data": {}}
    try:
        result = assess_upgrade_satisfaction(
            state["from_version"], state["to_version"]
        )
        return {**state, "satisfaction_data": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Zendesk satisfaction query failed: {e}")
        return {**state, "satisfaction_data": {}, "errors": errors}


def collect_linked_jira_keys(state: ZendeskCollectionState) -> ZendeskCollectionState:
    """Query F: Extract linked Jira issue keys for the Zendesk→Jira bridge."""
    if not state.get("schema_ready"):
        return {**state, "linked_jira_data": {}}
    try:
        result = get_linked_jira_keys(
            state["from_version"], state["to_version"]
        )
        return {**state, "linked_jira_data": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Zendesk linked Jira keys query failed: {e}")
        return {**state, "linked_jira_data": {}, "errors": errors}


def collect_customer_jira_links_node(state: ZendeskCollectionState) -> ZendeskCollectionState:
    """Query G: Extract Jira keys linked to ALL of this customer's Zendesk tickets.

    Unlike collect_linked_jira_keys which only finds upgrade-tagged tickets,
    this finds ALL Jira links for the customer's org — covering regular support
    tickets too. Uses fuzzy org name matching.
    """
    if not state.get("schema_ready"):
        return {**state, "customer_jira_data": {}}
    customer_name = state.get("customer_name", "")
    if not customer_name:
        return {**state, "customer_jira_data": {
            "jira_keys": [], "found": False, "error": None, "data_gaps": []}}
    try:
        result = get_customer_jira_links(customer_name)
        return {**state, "customer_jira_data": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Zendesk customer Jira links query failed: {e}")
        return {**state, "customer_jira_data": {}, "errors": errors}


# ---------------------------------------------------------------------------
# Node 9: Synthesise findings
# ---------------------------------------------------------------------------

def synthesize_zendesk_findings(state: ZendeskCollectionState) -> ZendeskCollectionState:
    """Aggregate all query results into a unified findings dict."""
    version_pair = state.get("version_pair_issues", {})
    risk = state.get("risk_patterns", {})
    env = state.get("environment_issues", {})
    common = state.get("common_issues", {})
    complete = state.get("complete_issues", {})
    satisfaction = state.get("satisfaction_data", {})
    linked_jira = state.get("linked_jira_data", {})
    customer_jira = state.get("customer_jira_data", {})
    errors = list(state.get("errors", []))

    # Determine if we have any useful data
    data_available = (
        state.get("schema_ready", False)
        and not all(
            d.get("error") for d in [version_pair, risk, env, common, complete, satisfaction] if d
        )
    )

    # Collect blockers from Zendesk data
    blockers: list[str] = []
    warnings: list[str] = []
    considerations: list[str] = []

    # Risk-based blockers/warnings
    risk_level = risk.get("risk_level", "UNKNOWN")
    if risk_level == "HIGH":
        blockers.append(
            f"[Zendesk] HIGH risk upgrade path ({risk.get('critical_issues', 0)} critical issues, "
            f"{risk.get('total_issues', 0)} total reported for {risk.get('upgrade_path', '?')})"
        )
    elif risk_level == "MEDIUM":
        warnings.append(
            f"[Zendesk] MEDIUM risk: {risk.get('total_issues', 0)} issues reported for "
            f"{risk.get('upgrade_path', '?')} ({risk.get('critical_issues', 0)} critical)"
        )

    # Add risk warnings
    for w in risk.get("warnings", []):
        warnings.append(f"[Zendesk] {w}")

    # Version pair issues
    pairs = version_pair.get("version_pairs", [])
    for p in pairs:
        if p.get("issue_count", 0) > 0:
            considerations.append(
                f"[Zendesk] {p['issue_count']} issue(s) reported for "
                f"{p.get('from', '?')} -> {p.get('to', '?')}, "
                f"affecting {p.get('affected_accounts', 0)} account(s), "
                f"{p.get('resolved_count', 0)} resolved"
            )

    # Environment considerations
    by_env = env.get("by_environment", {})
    for env_name, env_data in by_env.items():
        count = env_data.get("issue_count", 0)
        if count > 0:
            considerations.append(
                f"[Zendesk] {count} issue(s) in {env_name} environment, "
                f"affecting {env_data.get('affected_accounts', 0)} account(s)"
            )

    # Satisfaction considerations
    avg_sat = satisfaction.get("avg_satisfaction", 0)
    rated = satisfaction.get("rated_count", 0)
    if rated > 0 and avg_sat < 3.0:
        warnings.append(
            f"[Zendesk] Low customer satisfaction ({avg_sat}/5) for this upgrade path "
            f"({rated} ratings)"
        )
    elif rated > 0:
        considerations.append(
            f"[Zendesk] Customer satisfaction: {avg_sat}/5 ({rated} ratings), "
            f"avg resolution {satisfaction.get('avg_resolution_days', 0)} days"
        )

    # Collect data gaps from all sub-queries
    data_gaps: list[str] = []
    for d in [version_pair, risk, env, common, complete, satisfaction,
              linked_jira, customer_jira]:
        if d and d.get("data_gaps"):
            data_gaps.extend(d["data_gaps"])
    if not state.get("schema_ready"):
        data_gaps.append("Zendesk schema not available — all ticket analysis skipped")

    # Merge upgrade-tagged Jira keys + customer-specific Jira keys (deduplicate)
    upgrade_jira_keys = set(linked_jira.get("jira_keys", []))
    customer_jira_keys = set(customer_jira.get("jira_keys", []))
    all_jira_keys = list(upgrade_jira_keys | customer_jira_keys)

    findings = {
        "data_available": data_available,
        "version_pair_issues": version_pair,
        "risk_patterns": risk,
        "environment_issues": env,
        "common_issues": common,
        "complete_issues": complete,
        "satisfaction_data": satisfaction,
        "linked_jira_keys": all_jira_keys,
        "blockers": blockers,
        "warnings": warnings,
        "considerations": considerations,
        "data_gaps": data_gaps,
    }

    return {**state, "zendesk_findings": findings}


# ---------------------------------------------------------------------------
# Workflow graph
# ---------------------------------------------------------------------------

def _build_zendesk_workflow():
    workflow = StateGraph(ZendeskCollectionState)

    workflow.add_node("validate_schema", validate_schema)
    workflow.add_node("version_pair", collect_version_pair_issues)
    workflow.add_node("risk_patterns", collect_risk_patterns)
    workflow.add_node("environment", collect_environment_issues)
    workflow.add_node("common_types", collect_common_types)
    workflow.add_node("complete_details", collect_complete_details)
    workflow.add_node("satisfaction", collect_satisfaction)
    workflow.add_node("linked_jira", collect_linked_jira_keys)
    workflow.add_node("customer_jira", collect_customer_jira_links_node)
    workflow.add_node("synthesize", synthesize_zendesk_findings)

    # Sequential: validate schema first, then run all 8 queries, then synthesize
    workflow.set_entry_point("validate_schema")
    workflow.add_edge("validate_schema", "version_pair")
    workflow.add_edge("version_pair", "risk_patterns")
    workflow.add_edge("risk_patterns", "environment")
    workflow.add_edge("environment", "common_types")
    workflow.add_edge("common_types", "complete_details")
    workflow.add_edge("complete_details", "satisfaction")
    workflow.add_edge("satisfaction", "linked_jira")
    workflow.add_edge("linked_jira", "customer_jira")
    workflow.add_edge("customer_jira", "synthesize")
    workflow.add_edge("synthesize", END)

    return workflow.compile()


# ---------------------------------------------------------------------------
# Public entry function
# ---------------------------------------------------------------------------

def run_zendesk_collection(
    from_version: str,
    to_version: str,
    customer_name: str = "",
) -> dict:
    """Run the full Zendesk issue collection workflow.

    Args:
        from_version: Current Incorta version (e.g., '2024.1.0')
        to_version: Target Incorta version (e.g., '2024.7.0')
        customer_name: Customer name for fuzzy org matching (optional).
                       When provided, also extracts Jira keys linked to ALL
                       of this customer's Zendesk tickets (not just upgrade-tagged).

    Returns:
        dict: Unified zendesk_findings with blockers, warnings, considerations,
              and data from all queries.
    """
    initial_state: ZendeskCollectionState = {
        "from_version": from_version,
        "to_version": to_version,
        "customer_name": customer_name,
        "schema_ready": False,
        "version_pair_issues": {},
        "risk_patterns": {},
        "environment_issues": {},
        "common_issues": {},
        "complete_issues": {},
        "satisfaction_data": {},
        "linked_jira_data": {},
        "customer_jira_data": {},
        "zendesk_findings": {},
        "errors": [],
    }

    wf = _build_zendesk_workflow()
    result = wf.invoke(initial_state)
    return result.get("zendesk_findings", {})
