"""
An initial tool, for checking validation in the "pre-upgrade" part, this may be transformed to a stage based tools especially when we have an agent that will deal with the full revision cycle 
so tools has to be optimized to shorter range and expand the actions inside each tool internal workflow.. 
"""


def check_service_status(cluster_data: dict) -> dict:
    """Check if all services are Started"""
    details = []
    all_ok = True
    
    # Core services
    core_services = [
        ("Spark", cluster_data.get("spark_status")),
        ("Zookeeper", cluster_data.get("zookeeper_status")),
        ("Database", cluster_data.get("db_status")),
        ("Analytics", cluster_data.get("analytics_status", {}).get("statusType") if isinstance(cluster_data.get("analytics_status"), dict) else cluster_data.get("analytics_status")),
        ("Loader", cluster_data.get("loader_status", {}).get("statusType") if isinstance(cluster_data.get("loader_status"), dict) else cluster_data.get("loader_status")),
    ]
    
    for name, status in core_services:
        if status == "Started":
            details.append(f"✓ {name}: Started")
        else:
            details.append(f"✗ {name}: {status}")
            all_ok = False
    
    # Node services
    for node in cluster_data.get("nodes", []):
        node_name = node.get("name", "unknown")
        for service in node.get("services", []):
            svc_name = service.get("name")
            svc_status = service.get("status", {}).get("statusType", "Unknown")
            
            if svc_status == "Started":
                details.append(f"✓ {node_name}/{svc_name}: Started")
            else:
                details.append(f"✗ {node_name}/{svc_name}: {svc_status}")
                all_ok = False
    
    return {
        "status": "PASS" if all_ok else "FAIL",
        "details": details
    }


def check_memory_status(cluster_data: dict, threshold: float = 80.0) -> dict:
    """Check if memory usage is below threshold (default 80%)."""
    details = []
    warnings = []
    
    for node in cluster_data.get("nodes", []):
        node_name = node.get("name", "unknown")
        
        for service in node.get("services", []):
            svc_name = service.get("name")
            
            # On-heap memory
            on_assigned = service.get("assigned_on_heap_memory", 0)
            on_used = service.get("used_on_heap_memory", 0)
            on_pct = (on_used / on_assigned * 100) if on_assigned > 0 else 0
            
            # Off-heap memory
            off_assigned = service.get("assigned_off_heap_memory", 0)
            off_used = service.get("used_off_heap_memory", 0)
            off_pct = (off_used / off_assigned * 100) if off_assigned > 0 else 0
            
            detail = f"{node_name}/{svc_name}: On-heap {on_pct:.0f}% ({on_used}/{on_assigned} GB), Off-heap {off_pct:.0f}% ({off_used}/{off_assigned} GB)"
            
            if on_pct >= threshold or off_pct >= threshold:
                warnings.append(detail)
            else:
                details.append(f"✓ {detail}")
    
    if warnings:
        for w in warnings:
            details.append(f"⚠ {w}")
        return {"status": "WARNING", "details": details}
    
    return {"status": "PASS", "details": details}


def generate_report(cluster_name: str, cluster_data: dict, checks: dict) -> str:
    """Generate markdown report from check results."""
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Overall status
    statuses = [c["status"] for c in checks.values()]
    if "FAIL" in statuses:
        overall = "❌ NOT READY"
    elif "WARNING" in statuses:
        overall = "⚠️ READY WITH WARNINGS"
    else:
        overall = "✅ READY"
    
    lines = [
        f"# Pre-Upgrade Validation Report",
        f"**Cluster:** {cluster_name}",
        f"**Timestamp:** {timestamp}",
        f"",
        f"## Overall Status: {overall}",
        f"",
        f"| Check | Status |",
        f"|-------|--------|",
    ]
    
    for check_name, result in checks.items():
        status_icon = {"PASS": "✅", "FAIL": "❌", "WARNING": "⚠️"}.get(result["status"], "❓")
        lines.append(f"| {check_name} | {status_icon} {result['status']} |")
    
    for check_name, result in checks.items():
        lines.append(f"")
        lines.append(f"### {check_name}")
        for detail in result["details"]:
            lines.append(f"- {detail}")
    
    return "\n".join(lines)
