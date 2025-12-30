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
from tools.validation_checks import check_service_status, check_memory_status, generate_report


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


def validate_services(state: ValidationState) -> ValidationState:
    """Node 2: Run all validation checks"""
    if state.get("error"):
        return state
    
    checks = {
        "Service Status": check_service_status(state["cluster_data"]),
        "Memory Status": check_memory_status(state["cluster_data"]),
    }
    
    # --- ############ more checks here in future ---
    # checks["Disk Space"] = check_disk_space(state["cluster_data"])
    # ......
    # ....
 
    
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
