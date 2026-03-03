"""
Jira Issue Collection Workflow

Automatically gathers customer bug data from Jira_F and classifies each bug's
fix status relative to the target upgrade version.
Runs a validate-first pattern: checks schema availability once, then executes
specialized queries in sequence, classifies bugs, and synthesises results.

Called automatically by the readiness report workflow — no user action required.
Runs AFTER Zendesk collection so that linked Jira keys from Zendesk tickets
are available for targeted queries.
"""

import logging
import os
import sys
from typing import List, TypedDict

from langgraph.graph import StateGraph, END

# add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.incorta_tools import get_jira_schema
from tools.jira_helpers import (
    get_customer_bugs,
    get_linked_jira_issues,
    get_upgrade_path_bugs,
    classify_bug_fix_status,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class JiraCollectionState(TypedDict):
    # Inputs
    customer_name: str
    to_version: str
    linked_jira_keys: List[str]  # From Zendesk ticket_jira_links

    # Schema validation
    schema_ready: bool

    # Collection results (one per helper)
    customer_bugs: dict
    linked_issues: dict
    upgrade_path_bugs: dict

    # Classification result
    bug_classification: dict

    # Aggregated output
    jira_findings: dict
    errors: list


# ---------------------------------------------------------------------------
# Node 0: Validate schema (gate node — short-circuits if not ready)
# ---------------------------------------------------------------------------

def validate_schema(state: JiraCollectionState) -> JiraCollectionState:
    """Check schema readiness once. Populates cache for all subsequent queries."""
    errors = list(state.get("errors", []))
    try:
        schema = get_jira_schema({"fetch_schema": True})
        ready = schema.get("bug_analysis_ready", False)
        if not ready:
            missing = schema.get("missing_bug_tables", [])
            error_msg = schema.get("error", "")
            if error_msg:
                errors.append(f"Jira schema: {error_msg}")
            elif missing:
                errors.append(f"Jira missing tables: {missing}")
        return {**state, "schema_ready": ready, "errors": errors}
    except Exception as e:
        errors.append(f"Jira schema validation failed: {e}")
        return {**state, "schema_ready": False, "errors": errors}


# ---------------------------------------------------------------------------
# Nodes 1-3: Collect data (each fault-tolerant)
# ---------------------------------------------------------------------------

def collect_customer_bugs_node(state: JiraCollectionState) -> JiraCollectionState:
    """Collect all bugs reported by the customer with fix version joins."""
    if not state.get("schema_ready"):
        return {**state, "customer_bugs": {}}
    customer_name = state.get("customer_name", "")
    if not customer_name:
        return {**state, "customer_bugs": {"bugs": [], "total_bugs": 0, "found": False,
                                           "error": None, "data_gaps": []}}
    try:
        result = get_customer_bugs(customer_name)
        return {**state, "customer_bugs": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Jira customer bugs query failed: {e}")
        return {**state, "customer_bugs": {}, "errors": errors}


def collect_linked_issues_node(state: JiraCollectionState) -> JiraCollectionState:
    """Collect Jira issues linked from Zendesk tickets."""
    if not state.get("schema_ready"):
        return {**state, "linked_issues": {}}
    jira_keys = state.get("linked_jira_keys", [])
    if not jira_keys:
        return {**state, "linked_issues": {"issues": [], "total_issues": 0, "found": False,
                                           "error": None, "data_gaps": []}}
    try:
        result = get_linked_jira_issues(jira_keys)
        return {**state, "linked_issues": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Jira linked issues query failed: {e}")
        return {**state, "linked_issues": {}, "errors": errors}


def collect_upgrade_path_bugs_node(state: JiraCollectionState) -> JiraCollectionState:
    """Collect bugs from other customers affecting versions in the upgrade path."""
    if not state.get("schema_ready"):
        return {**state, "upgrade_path_bugs": {}}
    try:
        # Use the to_version as both boundaries for now (prefix matching in the query)
        result = get_upgrade_path_bugs("", state["to_version"])
        return {**state, "upgrade_path_bugs": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Jira upgrade path bugs query failed: {e}")
        return {**state, "upgrade_path_bugs": {}, "errors": errors}


# ---------------------------------------------------------------------------
# Node 4: Classify bug fix status
# ---------------------------------------------------------------------------

def classify_bugs_node(state: JiraCollectionState) -> JiraCollectionState:
    """Classify all collected bugs as fixed/open/requires dot release."""
    if not state.get("schema_ready"):
        return {**state, "bug_classification": {}}

    # Merge bugs from all sources into a single list for classification
    all_bugs = []

    customer = state.get("customer_bugs", {})
    for bug in customer.get("bugs", []):
        all_bugs.append(bug)

    linked = state.get("linked_issues", {})
    for issue in linked.get("issues", []):
        all_bugs.append(issue)

    if not all_bugs:
        return {**state, "bug_classification": {
            "fixed_in_target": [], "still_open": [], "requires_later_release": [],
            "summary": {"fixed_count": 0, "open_count": 0, "later_release_count": 0, "total": 0},
            "error": None, "data_gaps": [],
        }}

    try:
        result = classify_bug_fix_status(all_bugs, state["to_version"])
        return {**state, "bug_classification": result}
    except Exception as e:
        errors = list(state.get("errors", []))
        errors.append(f"Jira bug classification failed: {e}")
        return {**state, "bug_classification": {}, "errors": errors}


# ---------------------------------------------------------------------------
# Node 5: Synthesise findings
# ---------------------------------------------------------------------------

def synthesize_jira_findings(state: JiraCollectionState) -> JiraCollectionState:
    """Aggregate all query results into a unified findings dict."""
    customer = state.get("customer_bugs", {})
    linked = state.get("linked_issues", {})
    path_bugs = state.get("upgrade_path_bugs", {})
    classification = state.get("bug_classification", {})
    errors = list(state.get("errors", []))

    # Determine if we have any useful data
    data_available = (
        state.get("schema_ready", False)
        and not all(
            d.get("error") for d in [customer, linked, path_bugs] if d
        )
    )

    blockers: list[str] = []
    warnings: list[str] = []
    considerations: list[str] = []

    summary = classification.get("summary", {})
    open_count = summary.get("open_count", 0)
    fixed_count = summary.get("fixed_count", 0)
    later_count = summary.get("later_release_count", 0)
    total = summary.get("total", 0)

    # Critical open bugs are blockers
    still_open = classification.get("still_open", [])
    critical_open = [b for b in still_open if b.get("priority") in ("Blocker", "Critical")]
    if critical_open:
        blockers.append(
            f"[Jira] {len(critical_open)} critical/blocker bug(s) still open and NOT fixed "
            f"in target version: {', '.join(b['key'] for b in critical_open[:5])}"
        )

    # Open non-critical bugs are warnings
    if open_count > len(critical_open):
        warnings.append(
            f"[Jira] {open_count - len(critical_open)} customer-reported bug(s) still open "
            f"with no fix version assigned"
        )

    # Bugs requiring later release are warnings
    if later_count > 0:
        later_bugs = classification.get("requires_later_release", [])
        warnings.append(
            f"[Jira] {later_count} bug(s) require a release later than the target version: "
            f"{', '.join(b['key'] + ' (fix: ' + str(b.get('fix_version', '?')) + ')' for b in later_bugs[:5])}"
        )

    # Bugs fixed in target are positive considerations
    if fixed_count > 0:
        considerations.append(
            f"[Jira] {fixed_count} customer-reported bug(s) are fixed in the target version"
        )

    # Upgrade path bugs from other customers
    path_count = path_bugs.get("total_bugs", 0)
    if path_count > 0:
        considerations.append(
            f"[Jira] {path_count} bug(s) from other customers affect versions in the upgrade path"
        )

    # Collect data gaps
    data_gaps: list[str] = []
    for d in [customer, linked, path_bugs, classification]:
        if d and d.get("data_gaps"):
            data_gaps.extend(d["data_gaps"])
    if not state.get("schema_ready"):
        data_gaps.append("Jira schema not available — all bug analysis skipped")

    findings = {
        "data_available": data_available,
        "customer_bugs": customer,
        "linked_issues": linked,
        "upgrade_path_bugs": path_bugs,
        "bug_classification": classification,
        "blockers": blockers,
        "warnings": warnings,
        "considerations": considerations,
        "data_gaps": data_gaps,
    }

    return {**state, "jira_findings": findings}


# ---------------------------------------------------------------------------
# Workflow graph
# ---------------------------------------------------------------------------

def _build_jira_workflow():
    workflow = StateGraph(JiraCollectionState)

    workflow.add_node("validate_schema", validate_schema)
    workflow.add_node("customer_bugs", collect_customer_bugs_node)
    workflow.add_node("linked_issues", collect_linked_issues_node)
    workflow.add_node("upgrade_path_bugs", collect_upgrade_path_bugs_node)
    workflow.add_node("classify_bugs", classify_bugs_node)
    workflow.add_node("synthesize", synthesize_jira_findings)

    # Sequential: validate schema first, then collect, classify, synthesize
    workflow.set_entry_point("validate_schema")
    workflow.add_edge("validate_schema", "customer_bugs")
    workflow.add_edge("customer_bugs", "linked_issues")
    workflow.add_edge("linked_issues", "upgrade_path_bugs")
    workflow.add_edge("upgrade_path_bugs", "classify_bugs")
    workflow.add_edge("classify_bugs", "synthesize")
    workflow.add_edge("synthesize", END)

    return workflow.compile()


# ---------------------------------------------------------------------------
# Public entry function
# ---------------------------------------------------------------------------

def run_jira_collection(
    customer_name: str,
    to_version: str,
    linked_jira_keys: List[str] | None = None,
) -> dict:
    """Run the full Jira bug collection workflow.

    Args:
        customer_name: Customer name for bug filtering (e.g., 'Acme Corp').
        to_version: Target Incorta version (e.g., '2024.7.0').
        linked_jira_keys: Jira issue keys from Zendesk ticket_jira_links (optional).

    Returns:
        dict: Unified jira_findings with blockers, warnings, considerations,
              bug classification, and data from all queries.
    """
    initial_state: JiraCollectionState = {
        "customer_name": customer_name,
        "to_version": to_version,
        "linked_jira_keys": linked_jira_keys or [],
        "schema_ready": False,
        "customer_bugs": {},
        "linked_issues": {},
        "upgrade_path_bugs": {},
        "bug_classification": {},
        "jira_findings": {},
        "errors": [],
    }

    wf = _build_jira_workflow()
    result = wf.invoke(initial_state)
    return result.get("jira_findings", {})
