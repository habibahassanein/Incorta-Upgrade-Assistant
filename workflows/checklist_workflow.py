"""
Pre-Upgrade Checklist Workflow

Workflow for filling the Pre-Upgrade Checklist Excel template.
Writes approved cell values (from generate_upgrade_readiness_report) into an Excel template copy.

Only the "Pre-Upgrade Checklist" sheet is modified. All other sheets are untouched.
"""

import base64
import json
import os
import shutil
import sys
import tempfile
from typing import TypedDict

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
# Map collected data to cell values (used internally by readiness_report.py)
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
# Public entry: Write approved values to Excel, return as base64
# ---------------------------------------------------------------------------

def run_write_checklist_excel(
    cell_values_json: str,
    template_path: str,
    filename: str = "pre_upgrade_checklist_filled.xlsx",
) -> dict:
    """Write approved cell values into a copy of the Excel template.

    Only modifies the 'Pre-Upgrade Checklist' sheet. All other sheets untouched.
    Writes to a temporary file, encodes as base64, and returns the encoded bytes
    so the caller can offer it as a download — no output path needed.

    Args:
        cell_values_json: JSON string of {row_num: {"B": value, "C": status}}
        template_path: Path to the Excel template file
        filename: Suggested filename for the download

    Returns:
        dict with keys:
            - "type": "excel"
            - "filename": suggested download filename
            - "base64": base64-encoded .xlsx bytes
            - "summary": human-readable summary string
    """
    from openpyxl import load_workbook
    from openpyxl.styles import PatternFill, Alignment

    # Parse cell values
    raw = json.loads(cell_values_json)
    cell_values = {int(k): v for k, v in raw.items()}

    # Write into a temp file so we never need a caller-supplied output path
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        shutil.copy2(template_path, tmp_path)

        wb = load_workbook(tmp_path)
        ws = wb["Pre-Upgrade Checklist"]

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
            value = cols.get("B", "")
            cell_b = ws.cell(row=row_num, column=2)
            cell_b.value = value
            cell_b.alignment = Alignment(wrap_text=True, vertical="top")

            status = cols.get("C", "")
            cell_c = ws.cell(row=row_num, column=3)
            cell_c.value = status
            cell_c.alignment = Alignment(horizontal="center", vertical="center")

            fill = status_fills.get(status)
            if fill:
                cell_c.fill = fill

        wb.save(tmp_path)
        wb.close()

        with open(tmp_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    filled_count = len(cell_values)
    not_impl_count = sum(1 for c in cell_values.values() if c.get("B") == "Not Implemented")

    summary = (
        f"# Checklist Generation Complete\n\n"
        f"- **Rows filled:** {filled_count}\n"
        f"- **Not Implemented rows:** {not_impl_count}\n"
        f"- **Sheet modified:** Pre-Upgrade Checklist\n"
        f"- **Other sheets:** Untouched (7 sheets preserved as-is)\n"
        f"- **File:** {filename} (ready for download)\n"
    )

    return {
        "type": "excel",
        "filename": filename,
        "base64": encoded,
        "summary": summary,
    }
