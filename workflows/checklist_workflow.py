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
    jira_issues = state.get("jira_issues", {})
    zendesk_issues = state.get("zendesk_issues", {})

    cells = {}
    is_cloud = meta.get("deployment_type", {}).get("is_cloud", False)

    # Helper to append a source reference tag to a cell value
    def _tag(value, source):
        """Append [Source: ...] to the value so the Excel sheet shows data provenance."""
        return f"{value}\n[Source: {source}]"

    # --- Row 3: Versions + Deployment ---
    dep = meta.get("deployment_type", {})
    dep_str = f"{dep.get('deployment_type', 'Unknown')} ({dep.get('cloud_provider', 'Unknown')})"
    cells[3] = {
        "attribute": "Upgrade From/To Version, Deployment",
        "B": _tag(f"From: {state.get('from_version', 'N/A')}, To: {state.get('to_version', 'N/A')}, {dep_str}", "CMC"),
        "C": "Done",
    }

    # --- Row 7: Upgrade Considerations ---
    if knowledge:
        items = [k.get("title", k.get("text", "N/A"))[:100] for k in knowledge[:5]]
        cells[7] = {
            "attribute": "Upgrade Considerations",
            "B": _tag("\n".join(f"- {item}" for item in items), "Knowledge Base"),
            "C": "Done",
        }
    else:
        cells[7] = {
            "attribute": "Upgrade Considerations",
            "B": _tag("No considerations found for this version pair", "Knowledge Base"),
            "C": "Review",
        }

    # --- Row 9: Auto-Suspend / Idle Time (Cloud only) ---
    if cloud:
        sleeppable = cloud.get("sleeppable")
        idle_hours = cloud.get("idle_time_hours")
        if sleeppable is not None:
            suspend_status = "Enabled" if sleeppable else "Disabled"
            idle_str = f"{idle_hours} hours" if idle_hours is not None else "N/A"
            cells[9] = {
                "attribute": "Environment Type",
                "B": _tag(f"Auto-Suspend: {suspend_status}\nIdle Time: {idle_str}", "Cloud Portal"),
                "C": "Done",
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
    cells[10] = {"attribute": "Topology", "B": _tag(topo_value, "CMC"), "C": "Done"}

    # --- Row 11: Spark Version ---
    spark_from_cloud = cloud.get("spark_version")
    spark_ver = spark_from_cloud or meta.get("infrastructure", {}).get("spark_mode", "N/A")
    spark_source = "Cloud Portal" if spark_from_cloud else "CMC"
    cells[11] = {"attribute": "Spark Version", "B": _tag(str(spark_ver), spark_source), "C": "Done"}

    # --- Row 12: Python Version ---
    python_ver = cloud.get("python_version", "N/A")
    cells[12] = {"attribute": "Python Version", "B": _tag(str(python_ver), "Cloud Portal"), "C": "Done"}

    # --- Row 13: Oracle to MySQL migration ---
    db = meta.get("database", {})
    if db.get("migration_needed"):
        cells[13] = {"attribute": "Oracle to MySQL migration", "B": _tag(f"Yes - DB is {db.get('db_type', 'Oracle')}", "CMC"), "C": "Action Required"}
    else:
        cells[13] = {"attribute": "Oracle to MySQL migration", "B": _tag(f"No - DB is {db.get('db_type', 'Unknown')}", "CMC"), "C": "Done"}

    # --- Row 14: Custom CSS ---
    integrations = meta.get("integrations", {})
    integration_items = integrations.get("integrations", {}) if isinstance(integrations, dict) else {}
    css_keys = [k for k in integration_items if "css" in k.lower() or "theme" in k.lower()]
    if css_keys:
        css_values = [f"{k}: {integration_items[k].get('enabled', False)}" for k in css_keys]
        cells[14] = {"attribute": "Custom CSS", "B": _tag("\n".join(css_values), "CMC"), "C": "Review"}
    else:
        cells[14] = {"attribute": "Custom CSS", "B": _tag("No custom CSS detected in config", "CMC"), "C": "Done"}

    # --- Row 15: Custom jars list (connectors) ---
    features = meta.get("features", {})
    enabled_connectors = features.get("connectors", [])
    disabled_connectors = features.get("disabled_connectors", [])
    row15_lines = []
    if enabled_connectors:
        row15_lines.append(f"Enabled ({len(enabled_connectors)}): {', '.join(enabled_connectors)}")
    if disabled_connectors:
        row15_lines.append(f"Disabled ({len(disabled_connectors)}): {', '.join(disabled_connectors)}")
    if not row15_lines:
        row15_lines.append("No connectors found")
    # Pull compatibility info from validation checks if available
    conn_check = checks.get("Connectors", {})
    conn_details = conn_check.get("details", [])
    compat_lines = [d for d in conn_details if any(kw in d for kw in ["JDK", "INCOMPATIBLE", "requires update", "Unknown compat", "compatible"])]
    if compat_lines:
        row15_lines.append("--- Compatibility ---")
        row15_lines.extend(compat_lines)
    row15_status = {"PASS": "Done", "WARNING": "Review", "FAIL": "Action Required"}.get(
        conn_check.get("status", "PASS"), "Done"
    )
    cells[15] = {"attribute": "Custom jars list", "B": _tag("\n".join(row15_lines), "CMC"), "C": row15_status}

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
        cells[16] = {"attribute": "Tenant folder size", "B": _tag("\n".join(tenant_lines) if tenant_lines else "No tenants", "CMC"), "C": "Done"}
    else:
        cells[16] = {"attribute": "Tenant folder size", "B": _tag(f"Could not retrieve: {ts.get('error', 'unknown')}", "CMC"), "C": "Failed"}

    # --- Row 17: IncortaAnalytics folder size (per-pod breakdown) ---
    if cloud:
        pod_lines = [
            f"Analytics Pod: {cloud.get('data_size_gb', '?')} GB",
            f"Loader Pod: {cloud.get('loader_size_gb', '?')} GB",
            f"CMC Pod: {cloud.get('cmc_size_gb', '?')} GB",
        ]
        cells[17] = {
            "attribute": "IncortaAnalytics folder size",
            "B": _tag("\n".join(pod_lines), "Cloud Portal"),
            "C": "Done",
        }
    else:
        cells[17] = {"attribute": "IncortaAnalytics folder size", "B": _tag("Cloud data not available", "Cloud Portal"), "C": "N/A"}

    # --- Row 18: Memory Total/Used/Free ---
    mem_check = checks.get("Memory Status", {})
    mem_details = mem_check.get("details", [])
    cells[18] = {
        "attribute": "Memory Total/Used/Free",
        "B": _tag("\n".join(mem_details) if mem_details else "N/A", "CMC"),
        "C": mem_check.get("status", "N/A"),
    }

    # --- Row 19: Disk Space (per-pod breakdown) ---
    if cloud:
        disk_lines = [
            f"Analytics Pod: {cloud.get('data_size_gb', '?')} GB (allocated)",
            f"Loader Pod: {cloud.get('loader_size_gb', '?')} GB (allocated)",
            f"CMC Pod: {cloud.get('cmc_size_gb', '?')} GB (allocated)",
            "Note: Per-pod utilization not available (API limited)",
            f"Tenant Folder Size: {cloud.get('consumed_data_gb', '?')} GB, Available Disk: {cloud.get('available_disk_gb', '?')} GB",
        ]
        cells[19] = {
            "attribute": "Disk Space",
            "B": _tag("\n".join(disk_lines), "Cloud Portal"),
            "C": "Done",
        }
    else:
        cells[19] = {"attribute": "Disk Space", "B": _tag("Cloud data not available", "Cloud Portal"), "C": "N/A"}

    # --- Row 20: Timezone configuration ---
    # Prefer Cloud Portal timezone (direct field) over CMC integrations search
    cp_timezone = cloud.get("timezone") if cloud else None
    all_integrations = integrations.get("integrations", {}) if isinstance(integrations, dict) else {}
    if cp_timezone:
        cells[20] = {"attribute": "Timezone configuration", "B": _tag(f"Cluster Timezone: {cp_timezone}", "Cloud Portal"), "C": "Done"}
    else:
        tz_keys = [k for k in all_integrations if "timezone" in k.lower() or "tz" in k.lower()]
        if tz_keys:
            tz_values = [f"{k}: enabled={all_integrations[k].get('enabled', False)}" for k in tz_keys]
            cells[20] = {"attribute": "Timezone configuration", "B": _tag("\n".join(tz_values), "CMC"), "C": "Done"}
        else:
            cells[20] = {"attribute": "Timezone configuration", "B": _tag("No timezone config found in cluster config (may use default)", "CMC"), "C": "Review"}

    # --- Row 21: Session timeout ---
    timeout_keys = [k for k in all_integrations if "session" in k.lower() or "timeout" in k.lower()]
    if timeout_keys:
        timeout_values = [f"{k}: enabled={all_integrations[k].get('enabled', False)}" for k in timeout_keys]
        cells[21] = {"attribute": "Session timeout", "B": _tag("\n".join(timeout_values), "CMC"), "C": "Done"}
    else:
        cells[21] = {"attribute": "Session timeout", "B": _tag("Default (not explicitly configured)", "CMC"), "C": "Done"}

    # --- Row 22: Data Sources test connection ---
    ds_check = checks.get("Connectors", {})
    ds_details = ds_check.get("details", [])
    cells[22] = {
        "attribute": "Data Sources: Test Connection",
        "B": _tag("\n".join(ds_details) if ds_details else "N/A", "CMC"),
        "C": ds_check.get("status", "N/A"),
    }

    # --- Row 26: Zendesk Open Tickets with Workaround/Fix info ---
    complete = zendesk_issues.get("complete_issues", {})
    zd_tickets = complete.get("issues", [])
    if zd_tickets:
        ticket_lines = []
        for t in zd_tickets[:10]:
            tid = t.get("ticket_id", "?")
            subj = t.get("subject", "?")[:60]
            status = t.get("status", "?")
            fix = t.get("fixed_in", "")
            workaround = "Yes" if t.get("has_workaround") else "No"
            line = f"#{tid} [{status}] {subj}"
            line += f" | Workaround: {workaround}"
            if fix:
                line += f" | Fix: {fix}"
            ticket_lines.append(line)
        if len(zd_tickets) > 10:
            ticket_lines.append(f"... and {len(zd_tickets) - 10} more")
        ticket_lines.append("")
        ticket_lines.append("Note: See Claude chat for detailed analysis and workarounds")
        cells[26] = {
            "attribute": "Open tickets on target version",
            "B": _tag("\n".join(ticket_lines), "Zendesk"),
            "C": "Review",
        }
    else:
        cells[26] = {
            "attribute": "Open tickets on target version",
            "B": _tag("No Zendesk tickets found for this upgrade path\n\nNote: See Claude chat for detailed analysis", "Zendesk"),
            "C": "Done",
        }

    # --- Row 28: Jira Bugs with Fix Versions ---
    jira_bugs = jira_issues.get("bugs", [])
    if jira_bugs:
        bug_lines = []
        fixed = [b for b in jira_bugs if b.get("category") == "fixed_in_target"]
        still_open = [b for b in jira_bugs if b.get("category") == "still_open"]
        later = [b for b in jira_bugs if b.get("category") == "requires_later_release"]
        if fixed:
            bug_lines.append(f"Fixed in target ({len(fixed)}):")
            for b in fixed[:5]:
                bug_lines.append(f"  {b.get('key', '?')} - {b.get('summary', '?')[:60]} (fix: {b.get('fix_version', '?')})")
        if still_open:
            bug_lines.append(f"Still open ({len(still_open)}):")
            for b in still_open[:5]:
                bug_lines.append(f"  {b.get('key', '?')} - {b.get('summary', '?')[:60]}")
        if later:
            bug_lines.append(f"Requires later release ({len(later)}):")
            for b in later[:3]:
                bug_lines.append(f"  {b.get('key', '?')} - fix: {b.get('fix_version', '?')}")
        cells[28] = {
            "attribute": "Check scheduled load jobs",
            "B": _tag("\n".join(bug_lines), "Jira"),
            "C": "Review" if still_open else "Done",
        }
    else:
        cells[28] = {"attribute": "Check scheduled load jobs", "B": _tag("No Jira bugs found for this upgrade path", "Jira"), "C": "Done"}

    # --- Row 29: Services status ---
    svc_check = checks.get("Service Status", {})
    svc_details = svc_check.get("details", [])
    cells[29] = {
        "attribute": "Services status",
        "B": _tag("\n".join(svc_details) if svc_details else "N/A", "CMC"),
        "C": svc_check.get("status", "N/A"),
    }

    # --- Row 30: On-Heap Memory ---
    on_heap_lines = [d for d in mem_details if "On-heap" in d or "on_heap" in d.lower() or "on-heap" in d.lower()]
    cells[30] = {
        "attribute": "On-Heap Memory",
        "B": _tag("\n".join(on_heap_lines) if on_heap_lines else "\n".join(mem_details) if mem_details else "N/A", "CMC"),
        "C": mem_check.get("status", "N/A"),
    }

    # --- Row 31: Off-Heap Memory ---
    off_heap_lines = [d for d in mem_details if "Off-heap" in d or "off_heap" in d.lower() or "off-heap" in d.lower()]
    cells[31] = {
        "attribute": "Off-Heap Memory",
        "B": _tag("\n".join(off_heap_lines) if off_heap_lines else "See row 18 for full memory details", "CMC"),
        "C": mem_check.get("status", "N/A"),
    }

    # --- Row 37: DA upgrade needed (Cloud only) ---
    if cloud:
        da_enabled = cloud.get("data_agent_enabled", False)
        cells[37] = {
            "attribute": "Data Agent upgrade needed",
            "B": _tag(f"Data Agent: {'Enabled - confirm DA upgrade with customer' if da_enabled else 'Disabled'}", "Cloud Portal"),
            "C": "Review" if da_enabled else "Done",
        }
    else:
        cells[37] = {"attribute": "Data Agent upgrade needed", "B": _tag("N/A (cloud data not available)", "Cloud Portal"), "C": "N/A"}

    # --- Not Implemented rows (conditionally exclude cloud-irrelevant items) ---
    not_implemented = {
        27: "Check scheduled jobs and SendNow dashboard",
        33: "Run Inspector tool",
        42: "Successful roll back on pre-prod",
    }
    # On-prem only rows — skip for cloud deployments
    if not is_cloud:
        not_implemented[32] = "Run Alias Sync"
        not_implemented[35] = "Download Incorta package to all nodes"
        not_implemented[36] = "Install Chromium Headless Browser"
    else:
        # Mark cloud-irrelevant rows as N/A
        cells[32] = {"attribute": "Run Alias Sync", "B": "N/A (Cloud deployment)", "C": "N/A"}
        cells[35] = {"attribute": "Download Incorta package to all nodes", "B": "N/A (Cloud deployment)", "C": "N/A"}
        cells[36] = {"attribute": "Install Chromium Headless Browser", "B": "N/A (Cloud deployment)", "C": "N/A"}

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
