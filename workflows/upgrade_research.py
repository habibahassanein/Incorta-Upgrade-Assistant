"""
Upgrade Research Workflow

Uses semantic search and Incorta queries to research upgrade paths.
When cluster metadata (from CMC) and cloud metadata (from Cloud Portal) are
provided, the workflow builds context-aware queries targeting the customer's
specific environment — database type, topology, Spark mode, enabled features,
connectors, and deployment platform.

When metadata is NOT provided, the workflow falls back to generic version-only
queries (backward compatible for standalone / independent use).
"""

import os
import sys
from collections import defaultdict
from typing import List, Optional, Tuple, TypedDict

from langgraph.graph import StateGraph, END

# add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.qdrant_tool import search_knowledge_base


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class UpgradeResearchState(TypedDict):
    from_version: str
    to_version: str
    cluster_metadata: dict       # From extract_cluster_metadata()
    cloud_metadata: dict         # From Cloud Portal API
    docs_results: list
    known_issues: list
    community_insights: list
    context_results: list        # Results from context-aware queries
    report: str


# ---------------------------------------------------------------------------
# Context-aware query builder (pure function — importable by other modules)
# ---------------------------------------------------------------------------

# Human-readable labels for each context category
CATEGORY_LABELS = {
    "database": "Database",
    "topology": "Cluster Topology",
    "spark": "Spark Infrastructure",
    "python": "Python Version",
    "notebook": "Notebook",
    "sqli": "SQLi",
    "kyuubi": "Kyuubi",
    "mlflow": "MLflow",
    "data_agent": "Data Agent",
    "incorta_x": "Incorta X",
    "data_studio": "Data Studio",
    "connectors": "Connectors",
    "platform": "Platform / Deployment",
}


def _build_context_queries(
    from_v: str,
    to_v: str,
    cluster_metadata: dict,
    cloud_metadata: dict,
) -> List[Tuple[str, str, int]]:
    """Build context-aware search queries from cluster metadata.

    Examines the customer's actual cluster configuration and generates
    targeted search queries for each relevant aspect of their environment.

    Args:
        from_v: Current Incorta version (e.g. '2024.1.0')
        to_v: Target Incorta version (e.g. '2024.7.0')
        cluster_metadata: Dict from extract_cluster_metadata() (may be empty)
        cloud_metadata: Dict from Cloud Portal collect_cloud_data() (may be empty)

    Returns:
        List of (category, query_string, limit) tuples.
        Empty list if both metadata dicts are empty/missing.
    """
    queries: List[Tuple[str, str, int]] = []

    if not cluster_metadata and not cloud_metadata:
        return queries

    # --- Database ---
    db = cluster_metadata.get("database", {})
    db_type = db.get("db_type", "").upper()
    if db_type == "ORACLE":
        queries.append(("database", f"Oracle database migration upgrade Incorta {to_v}", 3))
        queries.append(("database", f"Oracle to MySQL migration Incorta {to_v}", 3))
    elif db_type == "MYSQL":
        queries.append(("database", f"MySQL upgrade issues {from_v} to {to_v} Incorta", 3))

    # --- Topology ---
    topo = cluster_metadata.get("topology", {})
    if topo.get("is_ha"):
        queries.append(("topology", f"HA high availability cluster upgrade Incorta {to_v}", 3))
    if topo.get("node_count", 1) > 1:
        queries.append(("topology", f"multi-node clustered upgrade considerations Incorta {to_v}", 3))

    # --- Spark infrastructure ---
    infra = cluster_metadata.get("infrastructure", {})
    spark_deploy = infra.get("spark_deployment", "")
    if spark_deploy == "Kubernetes":
        queries.append(("spark", f"Spark on Kubernetes upgrade Incorta {to_v}", 3))
    elif spark_deploy == "External":
        queries.append(("spark", f"external Spark upgrade considerations Incorta {to_v}", 3))

    spark_version = cloud_metadata.get("spark_version", "")
    if spark_version:
        queries.append(("spark", f"Spark {spark_version} compatibility Incorta {to_v}", 3))

    # --- Python version ---
    python_version = cloud_metadata.get("python_version", "")
    if python_version:
        major_minor = ".".join(python_version.split(".")[:2])
        queries.append(("python", f"Python {major_minor} upgrade Incorta {to_v}", 3))

    # --- Features: Notebook, SQLi, Kyuubi ---
    features = cluster_metadata.get("features", {})
    if features.get("notebook"):
        queries.append(("notebook", f"Notebook upgrade breaking changes Incorta {to_v}", 3))
    if features.get("sqli"):
        queries.append(("sqli", f"SQLi upgrade considerations Incorta {to_v}", 3))
    if features.get("kyuubi"):
        queries.append(("kyuubi", f"Kyuubi upgrade Incorta {to_v}", 3))

    # --- Cloud Portal features ---
    if cloud_metadata.get("mlflow_enabled"):
        queries.append(("mlflow", f"MLflow upgrade Incorta {to_v}", 3))
    if cloud_metadata.get("data_agent_enabled"):
        queries.append(("data_agent", f"Data Agent upgrade Incorta {to_v}", 3))
    if cloud_metadata.get("incorta_x_enabled"):
        queries.append(("incorta_x", f"Incorta X upgrade {to_v}", 3))
    if cloud_metadata.get("data_studio_enabled"):
        queries.append(("data_studio", f"Data Studio upgrade Incorta {to_v}", 3))

    # --- Connectors ---
    connectors = features.get("connectors", [])
    if connectors:
        connector_names = " ".join(connectors[:5])
        queries.append(("connectors", f"{connector_names} connector upgrade Incorta {to_v}", 3))

    # --- Platform / Deployment ---
    deploy = cluster_metadata.get("deployment_type", {})
    if deploy.get("is_cloud"):
        provider = deploy.get("cloud_provider", "")
        if "GCP" in provider or "Google" in provider:
            queries.append(("platform", f"GCP cloud upgrade Incorta {to_v}", 3))
        elif "AWS" in provider or "Amazon" in provider:
            queries.append(("platform", f"AWS cloud upgrade Incorta {to_v}", 3))
        elif "Azure" in provider or "Microsoft" in provider:
            queries.append(("platform", f"Azure cloud upgrade Incorta {to_v}", 3))
    else:
        if cluster_metadata:  # Only add on-prem query if we actually have metadata
            queries.append(("platform", f"on-premises upgrade Incorta {to_v}", 3))

    return queries


# ---------------------------------------------------------------------------
# Workflow Nodes
# ---------------------------------------------------------------------------

def search_release_notes(state: UpgradeResearchState) -> UpgradeResearchState:
    """Node 1: Search for release notes and upgrade guides."""
    query = f"upgrade from {state['from_version']} to {state['to_version']} release notes"
    result = search_knowledge_base({"query": query, "limit": 10})
    return {**state, "docs_results": result.get("results", [])}


def search_known_issues(state: UpgradeResearchState) -> UpgradeResearchState:
    """Node 2: Search for known upgrade issues."""
    query = f"{state['to_version']} upgrade known issues problems"
    result = search_knowledge_base({"query": query, "limit": 8})
    return {**state, "known_issues": result.get("results", [])}


def search_community_experiences(state: UpgradeResearchState) -> UpgradeResearchState:
    """Node 3: Search for community upgrade experiences."""
    query = f"upgrade {state['from_version']} to {state['to_version']} experience tips"
    result = search_knowledge_base({"query": query, "limit": 5})
    return {**state, "community_insights": result.get("results", [])}


def search_context_issues(state: UpgradeResearchState) -> UpgradeResearchState:
    """Node 4: Search for context-specific upgrade issues based on cluster metadata.

    Builds targeted queries from the customer's actual environment configuration
    (database type, topology, Spark mode, enabled features, connectors, platform)
    and runs each against the knowledge base. Results are deduplicated by URL and
    tagged with their source category for grouped display in the report.

    When metadata is empty, returns an empty list (no-op).
    """
    cluster_meta = state.get("cluster_metadata", {})
    cloud_meta = state.get("cloud_metadata", {})
    from_v = state["from_version"]
    to_v = state["to_version"]

    queries = _build_context_queries(from_v, to_v, cluster_meta, cloud_meta)

    if not queries:
        return {**state, "context_results": []}

    all_results = []
    seen_urls: set = set()

    for category, query_text, limit in queries:
        try:
            result = search_knowledge_base({"query": query_text, "limit": limit})
            for item in result.get("results", []):
                url = item.get("url", "")
                if url and url in seen_urls:
                    continue
                seen_urls.add(url)
                item["context_category"] = category
                item["context_query"] = query_text
                all_results.append(item)
        except Exception:
            continue  # Skip failed individual queries — graceful degradation

    # Sort by relevance score descending
    all_results.sort(key=lambda x: x.get("score", 0), reverse=True)

    return {**state, "context_results": all_results}


def synthesize_research(state: UpgradeResearchState) -> UpgradeResearchState:
    """Node 5: Combine all findings into a comprehensive report."""

    report_parts = []

    # Header
    report_parts.append(f"# Upgrade Path Research: {state['from_version']} \u2192 {state['to_version']}\n")

    # Official Documentation
    report_parts.append("## Official Documentation & Release Notes\n")
    if state["docs_results"]:
        for i, doc in enumerate(state["docs_results"][:5], 1):
            report_parts.append(f"{i}. **[{doc['title']}]({doc['url']})**")
            report_parts.append(f"   - Score: {doc['score']:.3f}")
            report_parts.append(f"   - {doc.get('text', '')[:250]}...\n")
    else:
        report_parts.append("No documentation found.\n")

    # Known Issues
    report_parts.append("\n## Known Issues & Considerations\n")
    if state["known_issues"]:
        for i, issue in enumerate(state["known_issues"][:5], 1):
            report_parts.append(f"{i}. **[{issue['title']}]({issue['url']})**")
            report_parts.append(f"   - Score: {issue['score']:.3f}")
            report_parts.append(f"   - {issue.get('text', '')[:250]}...\n")
    else:
        report_parts.append("No known issues found.\n")

    # Community Insights
    report_parts.append("\n## Community Experiences & Tips\n")
    if state["community_insights"]:
        for i, insight in enumerate(state["community_insights"][:5], 1):
            report_parts.append(f"{i}. **[{insight['title']}]({insight['url']})**")
            report_parts.append(f"   - Score: {insight['score']:.3f}")
            report_parts.append(f"   - {insight.get('text', '')[:250]}...\n")
    else:
        report_parts.append("No community discussions found.\n")

    # Context-Specific Findings (NEW)
    context_results = state.get("context_results", [])
    if context_results:
        report_parts.append("\n## Environment-Specific Findings\n")
        report_parts.append(
            "_Based on your cluster configuration (database, topology, Spark, "
            "features, connectors, platform):_\n"
        )

        # Group by category
        by_category: dict = defaultdict(list)
        for item in context_results:
            cat = item.get("context_category", "general")
            by_category[cat].append(item)

        for cat, items in by_category.items():
            label = CATEGORY_LABELS.get(cat, cat.replace("_", " ").title())
            report_parts.append(f"### {label}\n")
            for i, item in enumerate(items[:3], 1):
                report_parts.append(f"{i}. **[{item['title']}]({item['url']})**")
                report_parts.append(f"   - Score: {item['score']:.3f}")
                report_parts.append(f"   - {item.get('text', '')[:250]}...\n")
    else:
        report_parts.append("\n## Environment-Specific Findings\n")
        report_parts.append("_No cluster metadata available for context-specific search._\n")

    # Summary
    report_parts.append("\n## Summary\n")
    report_parts.append(f"Total documents found: {len(state['docs_results'])}\n")
    report_parts.append(f"Total known issues: {len(state['known_issues'])}\n")
    report_parts.append(f"Community insights: {len(state['community_insights'])}\n")
    report_parts.append(f"Context-specific findings: {len(context_results)}\n")

    report = "\n".join(report_parts)

    return {**state, "report": report}


# ---------------------------------------------------------------------------
# Build the Workflow
# ---------------------------------------------------------------------------

def build_upgrade_research_workflow():
    """Build the LangGraph workflow for upgrade research."""
    workflow = StateGraph(UpgradeResearchState)

    # Add nodes
    workflow.add_node("search_docs", search_release_notes)
    workflow.add_node("search_issues", search_known_issues)
    workflow.add_node("search_community", search_community_experiences)
    workflow.add_node("search_context", search_context_issues)
    workflow.add_node("synthesize", synthesize_research)

    # Define flow
    workflow.set_entry_point("search_docs")
    workflow.add_edge("search_docs", "search_issues")
    workflow.add_edge("search_issues", "search_community")
    workflow.add_edge("search_community", "search_context")
    workflow.add_edge("search_context", "synthesize")
    workflow.add_edge("synthesize", END)

    return workflow.compile()


# ---------------------------------------------------------------------------
# Public entry function
# ---------------------------------------------------------------------------

def research_upgrade_path(
    from_version: str,
    to_version: str,
    cluster_metadata: Optional[dict] = None,
    cloud_metadata: Optional[dict] = None,
) -> str:
    """Run the upgrade research workflow.

    When cluster_metadata and cloud_metadata are provided, the workflow
    generates targeted search queries based on the customer's actual
    environment (database, topology, Spark mode, features, connectors,
    platform). When omitted, falls back to generic version-only queries.

    Args:
        from_version: Current Incorta version (e.g., '2024.1.0')
        to_version: Target Incorta version (e.g., '2024.7.0')
        cluster_metadata: Optional dict from extract_cluster_metadata()
        cloud_metadata: Optional dict from Cloud Portal API

    Returns:
        Formatted markdown report with research findings
    """
    workflow = build_upgrade_research_workflow()

    initial_state = {
        "from_version": from_version,
        "to_version": to_version,
        "cluster_metadata": cluster_metadata or {},
        "cloud_metadata": cloud_metadata or {},
        "docs_results": [],
        "known_issues": [],
        "community_insights": [],
        "context_results": [],
        "report": "",
    }

    result = workflow.invoke(initial_state)

    return result["report"]


# Example usage for testing
if __name__ == "__main__":
    # Test the workflow (no metadata = backward-compatible generic queries)
    report = research_upgrade_path("2024.1.0", "2024.7.0")
    print(report)
