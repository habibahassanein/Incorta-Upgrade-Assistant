"""
in this folder, we will define the langraph workflows, where the large sequence actions will be as nodes inside this workflow 
with ability to add agentic decisions on those outputs, and further with more complex endpoints we can deal with it's output internally


"""

import os
import sys
from typing import TypedDict
from langgraph.graph import StateGraph, END

# add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients.cmc_client import CMCClient
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
    generate_report
)
from tools.test_connection import test_all_connections


class ValidationState(TypedDict):
    cluster_name: str
    cluster_data: dict
    checks: dict
    report: str
    error: str


# --- Workflow Nodes ---

def fetch_cluster_data(state: ValidationState) -> ValidationState:
    """Node 1: Fetch cluster data from CMC"""
    try:
        client = CMCClient()
        cluster_data = client.get_cluster(state["cluster_name"])
        return {**state, "cluster_data": cluster_data}
    except Exception as e:
        return {**state, "error": str(e)}


def check_datasource_connectivity() -> dict:
    """Check datasource connectivity using cached Incorta Analytics session.

    Returns a validation check result dict with status and details.
    Skips gracefully if no Analytics session is available.
    """
    # Import the cached session from server module
    try:
        from server import _incorta_session_cache
    except ImportError:
        _incorta_session_cache = {}

    if not _incorta_session_cache.get("authorization"):
        return {
            "status": "WARN",
            "details": [
                "Datasource connectivity not checked — Incorta Analytics login required.",
                "Call the **test_datasource_connections** tool to authenticate and test connections.",
            ],
        }

    try:
        result = test_all_connections(_incorta_session_cache)
    except Exception as e:
        return {
            "status": "WARN",
            "details": [f"Failed to test datasource connections: {str(e)}"],
        }

    if "error" in result:
        return {
            "status": "WARN",
            "details": [result["error"]],
        }

    details = [
        f"Tested {result['tested']}/{result['total']} datasources "
        f"({result['skipped']} skipped — no test support)",
    ]

    if result["failed"] == 0:
        status = "PASS"
        details.append(f"All {result['passed']} tested datasources connected successfully.")
    else:
        status = "FAIL"
        details.append(f"{result['passed']} passed, {result['failed']} failed:")
        for r in result["results"]:
            mark = "OK" if r["success"] else "FAIL"
            details.append(f"  [{mark}] {r['name']} ({r.get('type', 'unknown')}): {r['message']}")

    if result["skipped_datasources"]:
        details.append(f"Skipped: {', '.join(result['skipped_datasources'])}")

    return {"status": status, "details": details}


def validate_services(state: ValidationState) -> ValidationState:
    """Node 2: Run all validation checks"""
    if state.get("error"):
        return state

    checks = {
        # Core service checks (already implemented)
        "Service Status": check_service_status(state["cluster_data"]),
        "Memory Status": check_memory_status(state["cluster_data"]),

        # NEW: Cluster configuration settings
        "Cluster Configuration": check_cluster_configuration(state["cluster_data"]),

        # NEW: Infrastructure services (Requirement D - partial, P)
        "Infrastructure Services": check_infrastructure_services(state["cluster_data"]),

        # NEW: Node topology (Requirement D - partial)
        "Node Topology": check_node_topology(state["cluster_data"]),

        # NEW: Connectors (Requirement F - partial)
        "Connectors": check_connectors(state["cluster_data"]),

        # NEW: Tenants configuration
        "Tenants": check_tenants(state["cluster_data"]),

        # NEW: Email/SMTP configuration
        "Email Configuration": check_email_configuration(state["cluster_data"]),

        # NEW: Notebook & SQLi status
        "Notebook & SQLi": check_notebook_sqli_status(state["cluster_data"]),

        # NEW: Database migration status (Requirement E - partial)
        "Database Migration": check_database_migration(state["cluster_data"]),
    }

    # Datasource connectivity check (requires Incorta Analytics login)
    checks["Data Source Connectivity"] = check_datasource_connectivity()

    # --- Future checks that require additional API endpoints ---
    # checks["Top Workloads"] = check_top_workloads(state["cluster_data"])  # Req M
    # checks["Scheduled Jobs"] = check_scheduled_jobs(state["cluster_data"])  # Req O
    # checks["Inspector Tool"] = check_inspector_results(state["cluster_data"])  # Req R

    return {**state, "checks": checks}


def generate_validation_report(state: ValidationState) -> ValidationState:
    """Node 3: Generate markdown report"""
    if state.get("error"):
        report = f"# Validation Failed\n\nError: {state['error']}"
    else:
        report = generate_report(
            state["cluster_name"],
            state["cluster_data"],
            state["checks"]
        )
    return {**state, "report": report}


# --- ####### placeholder nodes, expand as needed in future 

def check_prerequisites(state: ValidationState) -> ValidationState:
    """placeholder: Check version compatibility, disk space, etc."""
    # TODO: Implement in Phase 2
    return state


# --- Build Workflow ---

def build_workflow():
    """build, compile the validation workflow"""
    workflow = StateGraph(ValidationState)
    
    # Add nodes
    workflow.add_node("fetch_cluster", fetch_cluster_data)
    workflow.add_node("validate", validate_services)
    workflow.add_node("report", generate_validation_report)
    
    # Set flow
    workflow.set_entry_point("fetch_cluster")
    workflow.add_edge("fetch_cluster", "validate")
    workflow.add_edge("validate", "report")
    workflow.add_edge("report", END)

    
    return workflow.compile()


# Compiled workflow instance
pre_upgrade_workflow = build_workflow()


def run_validation(cluster_name: str) -> str:
    # ---- run 
    initial_state = {
        "cluster_name": cluster_name,
        "cluster_data": {},
        "checks": {},
        "report": "",
        "error": "",
    }
    
    result = pre_upgrade_workflow.invoke(initial_state)
    return result["report"]
