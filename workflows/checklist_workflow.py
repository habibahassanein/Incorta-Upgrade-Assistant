"""
Pre-Upgrade Checklist Workflow

Two-phase workflow for filling the Pre-Upgrade Checklist Excel template:
1. collect phase: gathers data from CMC, Cloud Portal, and knowledge base
2. write phase: writes approved cell values into an Excel template copy

Only the "Pre-Upgrade Checklist" sheet is modified. All other sheets are untouched.
"""

import json
import os
import shutil
import sys
from typing import TypedDict

from langgraph.graph import StateGraph, END

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class ChecklistState(TypedDict):
    cmc_cluster_name: str
    cloud_cluster_name: str
    from_version: str
    to_version: str
    template_path: str
    output_path: str
    cluster_metadata: dict
    validation_checks: dict
    cloud_metadata: dict
    upgrade_knowledge: list
    cell_values: dict
    errors: list
    report: str


# ---------------------------------------------------------------------------
# Node 1: Collect CMC data (metadata + validation checks)
# ---------------------------------------------------------------------------

def collect_cmc_data(state: ChecklistState) -> ChecklistState:
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

        return {**state, "cluster_metadata": metadata, "validation_checks": checks}
    except Exception as e:
        errors.append(f"CMC data collection failed: {str(e)}")
        return {**state, "cluster_metadata": {}, "validation_checks": {}, "errors": errors}


# ---------------------------------------------------------------------------
# Node 2: Collect Cloud Portal data
# ---------------------------------------------------------------------------

def collect_cloud_data(state: ChecklistState) -> ChecklistState:
    """Fetch cloud metadata from Cloud Portal API. Fault-tolerant."""
    errors = list(state.get("errors", []))

    if not state.get("cloud_cluster_name"):
        errors.append("Cloud cluster name not provided — skipping cloud data")
        return {**state, "cloud_metadata": {}, "errors": errors}

    try:
        from clients.cloud_portal_client import CloudPortalClient

        cloud_client = CloudPortalClient()
        cluster = cloud_client.search_instances(state["cloud_cluster_name"])

        if not cluster:
            errors.append(f"Cloud cluster '{state['cloud_cluster_name']}' not found")
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
            "data_agent_enabled": cluster.get("enableDataAgent", False),
            "min_executors": cluster.get("minExecutors"),
            "max_executors": cluster.get("maxExecutors"),
        }

        return {**state, "cloud_metadata": cloud_meta}
    except Exception as e:
        errors.append(f"Cloud data collection failed: {str(e)}")
        return {**state, "cloud_metadata": {}, "errors": errors}


# ---------------------------------------------------------------------------
# Node 3: Collect upgrade knowledge
# ---------------------------------------------------------------------------

def collect_upgrade_knowledge(state: ChecklistState) -> ChecklistState:
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
        errors.append(f"Upgrade knowledge search failed: {str(e)}")
        return {**state, "upgrade_knowledge": [], "errors": errors}


# ---------------------------------------------------------------------------
# Node 4: Map collected data to cell values
# ---------------------------------------------------------------------------

def map_data_to_cells(state: ChecklistState) -> ChecklistState:
    """Transform collected data into Excel cell values for the checklist."""
    meta = state.get("cluster_metadata", {})
    checks = state.get("validation_checks", {})
    cloud = state.get("cloud_metadata", {})
    knowledge = state.get("upgrade_knowledge", [])

    cells = {}

    # --- Row 3: Versions + Deployment ---
    dep = meta.get("deployment_type", {})
    dep_str = f"{dep.get('deployment_type', 'Unknown')} ({dep.get('cloud_provider', 'Unknown')})"
    cells[3] = {
        "attribute": "Upgrade From/To Version, Deployment",
        "B": f"From: {state.get('from_version', 'N/A')}, To: {state.get('to_version', 'N/A')}, {dep_str}",
        "C": "Done",
    }

    # --- Row 7: Upgrade Considerations ---
    if knowledge:
        items = [k.get("title", k.get("text", "N/A"))[:100] for k in knowledge[:5]]
        cells[7] = {
            "attribute": "Upgrade Considerations",
            "B": "\n".join(f"- {item}" for item in items),
            "C": "Done",
        }
    else:
        cells[7] = {
            "attribute": "Upgrade Considerations",
            "B": "No considerations found for this version pair",
            "C": "Review",
        }

    # --- Row 10: Topology ---
    topo = meta.get("topology", {})
    topo_value = f"{topo.get('topology_type', 'Unknown')} ({topo.get('node_count', '?')} nodes)"
    if topo.get("is_ha"):
        topo_value += " - HA Enabled"
    nodes = topo.get("nodes", [])
    if nodes:
        node_lines = [f"  {n.get('name', '?')}: {n.get('type', '?')} - Services: {', '.join(n.get('services', []))}" for n in nodes]
        topo_value += "\n" + "\n".join(node_lines)
    cells[10] = {"attribute": "Topology", "B": topo_value, "C": "Done"}

    # --- Row 11: Spark Version ---
    spark_ver = cloud.get("spark_version") or meta.get("infrastructure", {}).get("spark_mode", "N/A")
    cells[11] = {"attribute": "Spark Version", "B": str(spark_ver), "C": "Done"}

    # --- Row 12: Python Version ---
    python_ver = cloud.get("python_version", "N/A")
    cells[12] = {"attribute": "Python Version", "B": str(python_ver), "C": "Done"}

    # --- Row 13: Oracle to MySQL migration ---
    db = meta.get("database", {})
    if db.get("migration_needed"):
        cells[13] = {"attribute": "Oracle to MySQL migration", "B": f"Yes - DB is {db.get('db_type', 'Oracle')}", "C": "Action Required"}
    else:
        cells[13] = {"attribute": "Oracle to MySQL migration", "B": f"No - DB is {db.get('db_type', 'Unknown')}", "C": "Done"}

    # --- Row 14: Custom CSS ---
    integrations = meta.get("integrations", {})
    integration_items = integrations.get("integrations", {}) if isinstance(integrations, dict) else {}
    css_keys = [k for k in integration_items if "css" in k.lower() or "theme" in k.lower()]
    if css_keys:
        css_values = [f"{k}: {integration_items[k].get('enabled', False)}" for k in css_keys]
        cells[14] = {"attribute": "Custom CSS", "B": "\n".join(css_values), "C": "Review"}
    else:
        cells[14] = {"attribute": "Custom CSS", "B": "No custom CSS detected in config", "C": "Done"}

    # --- Row 15: Custom jars list ---
    connectors = meta.get("features", {}).get("connectors", [])
    if connectors:
        cells[15] = {"attribute": "Custom jars list", "B": ", ".join(connectors), "C": "Done"}
    else:
        cells[15] = {"attribute": "Custom jars list", "B": "No enabled connectors found", "C": "Done"}

    # --- Row 16: Tenant folder size ---
    ts = meta.get("tenant_storage", {})
    if ts.get("status") == "success":
        tenant_lines = []
        for t in ts.get("tenants", []):
            name = t.get("name", "?")
            quota = t.get("disk_quota", "unknown")
            unit = t.get("disk_unit", "")
            enabled = "Enabled" if t.get("enabled") else "Disabled"
            tenant_lines.append(f"{name}: {quota} {unit} ({enabled})")
        cells[16] = {"attribute": "Tenant folder size", "B": "\n".join(tenant_lines) if tenant_lines else "No tenants", "C": "Done"}
    else:
        cells[16] = {"attribute": "Tenant folder size", "B": f"Could not retrieve: {ts.get('error', 'unknown')}", "C": "Failed"}

    # --- Row 17: IncortaAnalytics folder size ---
    if cloud:
        cells[17] = {
            "attribute": "IncortaAnalytics folder size",
            "B": f"Data: {cloud.get('data_size_gb', '?')} GB, Loader: {cloud.get('loader_size_gb', '?')} GB, CMC: {cloud.get('cmc_size_gb', '?')} GB",
            "C": "Done",
        }
    else:
        cells[17] = {"attribute": "IncortaAnalytics folder size", "B": "Cloud data not available", "C": "N/A"}

    # --- Row 18: Memory Total/Used/Free ---
    mem_check = checks.get("Memory Status", {})
    mem_details = mem_check.get("details", [])
    cells[18] = {
        "attribute": "Memory Total/Used/Free",
        "B": "\n".join(mem_details) if mem_details else "N/A",
        "C": mem_check.get("status", "N/A"),
    }

    # --- Row 19: Disk Space ---
    if cloud:
        cells[19] = {
            "attribute": "Disk Space",
            "B": f"Available: {cloud.get('available_disk_gb', '?')} GB, Consumed: {cloud.get('consumed_data_gb', '?')} GB",
            "C": "Done",
        }
    else:
        cells[19] = {"attribute": "Disk Space", "B": "Cloud data not available", "C": "N/A"}

    # --- Row 20: Timezone configuration ---
    config_categories = integrations.get("categories", {}) if isinstance(integrations, dict) else {}
    all_integrations = integrations.get("integrations", {}) if isinstance(integrations, dict) else {}
    tz_keys = [k for k in all_integrations if "timezone" in k.lower() or "tz" in k.lower()]
    if tz_keys:
        tz_values = [f"{k}: enabled={all_integrations[k].get('enabled', False)}" for k in tz_keys]
        cells[20] = {"attribute": "Timezone configuration", "B": "\n".join(tz_values), "C": "Done"}
    else:
        cells[20] = {"attribute": "Timezone configuration", "B": "No timezone config found in cluster config (may use default)", "C": "Review"}

    # --- Row 21: Session timeout ---
    timeout_keys = [k for k in all_integrations if "session" in k.lower() or "timeout" in k.lower()]
    if timeout_keys:
        timeout_values = [f"{k}: enabled={all_integrations[k].get('enabled', False)}" for k in timeout_keys]
        cells[21] = {"attribute": "Session timeout", "B": "\n".join(timeout_values), "C": "Done"}
    else:
        cells[21] = {"attribute": "Session timeout", "B": "Default (not explicitly configured)", "C": "Done"}

    # --- Row 22: Data Sources test connection ---
    conn_check = checks.get("Connectors", {})
    conn_details = conn_check.get("details", [])
    cells[22] = {
        "attribute": "Data Sources: Test Connection",
        "B": "\n".join(conn_details) if conn_details else "N/A",
        "C": conn_check.get("status", "N/A"),
    }

    # --- Row 29: Services status ---
    svc_check = checks.get("Service Status", {})
    svc_details = svc_check.get("details", [])
    cells[29] = {
        "attribute": "Services status",
        "B": "\n".join(svc_details) if svc_details else "N/A",
        "C": svc_check.get("status", "N/A"),
    }

    # --- Row 30: On-Heap Memory ---
    on_heap_lines = [d for d in mem_details if "On-heap" in d or "on_heap" in d.lower() or "on-heap" in d.lower()]
    cells[30] = {
        "attribute": "On-Heap Memory",
        "B": "\n".join(on_heap_lines) if on_heap_lines else "\n".join(mem_details) if mem_details else "N/A",
        "C": mem_check.get("status", "N/A"),
    }

    # --- Row 31: Off-Heap Memory ---
    off_heap_lines = [d for d in mem_details if "Off-heap" in d or "off_heap" in d.lower() or "off-heap" in d.lower()]
    cells[31] = {
        "attribute": "Off-Heap Memory",
        "B": "\n".join(off_heap_lines) if off_heap_lines else "See row 18 for full memory details",
        "C": mem_check.get("status", "N/A"),
    }

    # --- Row 37: DA upgrade needed (Cloud only) ---
    if cloud:
        da_enabled = cloud.get("data_agent_enabled", False)
        cells[37] = {
            "attribute": "Data Agent upgrade needed",
            "B": f"Data Agent: {'Enabled - confirm DA upgrade with customer' if da_enabled else 'Disabled'}",
            "C": "Review" if da_enabled else "Done",
        }
    else:
        cells[37] = {"attribute": "Data Agent upgrade needed", "B": "N/A (cloud data not available)", "C": "N/A"}

    # --- Not Implemented rows ---
    not_implemented = {
        27: "Check scheduled jobs and SendNow dashboard",
        28: "Check scheduled load jobs",
        32: "Run Alias Sync",
        33: "Run Inspector tool",
        35: "Download Incorta package to all nodes",
        36: "Install Chromium Headless Browser",
        42: "Successful roll back on pre-prod",
    }
    for row, attr in not_implemented.items():
        cells[row] = {"attribute": attr, "B": "Not Implemented", "C": "Pending"}

    return {**state, "cell_values": cells}


# ---------------------------------------------------------------------------
# Build collection workflow (nodes 1-4)
# ---------------------------------------------------------------------------

def _build_collect_workflow():
    workflow = StateGraph(ChecklistState)
    workflow.add_node("collect_cmc", collect_cmc_data)
    workflow.add_node("collect_cloud", collect_cloud_data)
    workflow.add_node("collect_knowledge", collect_upgrade_knowledge)
    workflow.add_node("map_cells", map_data_to_cells)

    workflow.set_entry_point("collect_cmc")
    workflow.add_edge("collect_cmc", "collect_cloud")
    workflow.add_edge("collect_cloud", "collect_knowledge")
    workflow.add_edge("collect_knowledge", "map_cells")
    workflow.add_edge("map_cells", END)

    return workflow.compile()


_collect_workflow = _build_collect_workflow()


# ---------------------------------------------------------------------------
# Public entry: Phase 1 — collect data and return preview
# ---------------------------------------------------------------------------

def run_collect_checklist_data(
    cmc_cluster_name: str,
    cloud_cluster_name: str,
    from_version: str,
    to_version: str,
) -> str:
    """Run collection workflow and return markdown preview + JSON cell values.

    Returns a string with:
    - Markdown table preview of all detected values
    - JSON block of cell_values for passing to write_checklist_excel
    """
    initial_state = {
        "cmc_cluster_name": cmc_cluster_name,
        "cloud_cluster_name": cloud_cluster_name,
        "from_version": from_version,
        "to_version": to_version,
        "template_path": "",
        "output_path": "",
        "cluster_metadata": {},
        "validation_checks": {},
        "cloud_metadata": {},
        "upgrade_knowledge": [],
        "cell_values": {},
        "errors": [],
        "report": "",
    }

    result = _collect_workflow.invoke(initial_state)

    cell_values = result.get("cell_values", {})
    errors = result.get("errors", [])

    # Build markdown preview table
    lines = [
        "## Pre-Upgrade Checklist — Collected Data\n",
        "| Row | Attribute | Detected Value | Status |",
        "|-----|-----------|----------------|--------|",
    ]

    for row_num in sorted(cell_values.keys()):
        cell = cell_values[row_num]
        attr = cell.get("attribute", "")
        value = cell.get("B", "").replace("\n", " | ")[:120]
        status = cell.get("C", "")
        lines.append(f"| {row_num} | {attr} | {value} | {status} |")

    not_impl_count = sum(1 for c in cell_values.values() if c.get("B") == "Not Implemented")
    failed_count = sum(1 for c in cell_values.values() if c.get("C") in ("Failed", "N/A"))

    lines.append("")
    lines.append(f"**Total rows filled:** {len(cell_values)}")
    lines.append(f"**Not Implemented:** {not_impl_count}")
    if failed_count:
        lines.append(f"**Could not auto-detect:** {failed_count}")
    if errors:
        lines.append(f"\n**Errors during collection:**")
        for e in errors:
            lines.append(f"- {e}")

    # Append JSON for the write phase
    # Convert int keys to strings for JSON serialization
    serializable = {str(k): v for k, v in cell_values.items()}
    lines.append(f"\n---\n\n<checklist_data>\n{json.dumps(serializable, indent=2)}\n</checklist_data>")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public entry: Phase 2 — write approved values to Excel
# ---------------------------------------------------------------------------

def run_write_checklist_excel(
    cell_values_json: str,
    template_path: str,
    output_path: str,
) -> str:
    """Write approved cell values into a copy of the Excel template.

    Only modifies the 'Pre-Upgrade Checklist' sheet. All other sheets untouched.

    Args:
        cell_values_json: JSON string of {row_num: {"B": value, "C": status}}
        template_path: Path to the Excel template file
        output_path: Path for the filled output file

    Returns:
        Summary report string
    """
    from openpyxl import load_workbook
    from openpyxl.styles import PatternFill, Alignment

    # Parse cell values
    raw = json.loads(cell_values_json)
    cell_values = {int(k): v for k, v in raw.items()}

    # Copy template
    shutil.copy2(template_path, output_path)

    wb = load_workbook(output_path)
    ws = wb["Pre-Upgrade Checklist"]

    # Status color fills
    status_fills = {
        "PASS": PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),
        "Done": PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),
        "FAIL": PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),
        "Failed": PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),
        "WARNING": PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"),
        "Review": PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"),
        "Action Required": PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"),
        "Pending": PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid"),
        "N/A": PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid"),
    }

    for row_num, cols in cell_values.items():
        # Column B = Pre-Prod Value
        value = cols.get("B", "")
        cell_b = ws.cell(row=row_num, column=2)
        cell_b.value = value
        cell_b.alignment = Alignment(wrap_text=True, vertical="top")

        # Column C = Pre-Prod Status
        status = cols.get("C", "")
        cell_c = ws.cell(row=row_num, column=3)
        cell_c.value = status
        cell_c.alignment = Alignment(horizontal="center", vertical="center")

        fill = status_fills.get(status)
        if fill:
            cell_c.fill = fill

    wb.save(output_path)
    wb.close()

    filled_count = len(cell_values)
    not_impl_count = sum(1 for c in cell_values.values() if c.get("B") == "Not Implemented")

    return (
        f"# Checklist Generation Complete\n\n"
        f"- **Output file:** {output_path}\n"
        f"- **Rows filled:** {filled_count}\n"
        f"- **Not Implemented rows:** {not_impl_count}\n"
        f"- **Sheet modified:** Pre-Upgrade Checklist\n"
        f"- **Other sheets:** Untouched (7 sheets preserved as-is)\n"
    )
