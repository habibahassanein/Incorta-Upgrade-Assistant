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


def check_cluster_configuration(cluster_data: dict, is_cloud: bool = False) -> dict:
    """Check cluster configuration settings (auto-start, scheduler, upgrade flags)

    Args:
        cluster_data: Cluster JSON from CMC API.
        is_cloud: If True, auto-start warning is suppressed (managed by Cloud Portal).
    """
    details = []
    warnings = []

    # Auto-start services — only warn for on-prem; cloud manages this via the portal
    auto_start = cluster_data.get("auto_start_services", "Unknown")
    if auto_start == "Enabled":
        details.append(f"✓ Auto-start services: {auto_start}")
    elif is_cloud:
        details.append(f"Auto-start services: {auto_start} (cloud — managed by portal)")
    else:
        warnings.append(f"Auto-start services: {auto_start} (recommended: Enabled for on-prem)")

    # Admin alerts
    admin_alerts = cluster_data.get("admin_alerts", "Unknown")
    details.append(f"Admin alerts: {admin_alerts}")

    # Scheduler status
    scheduler = cluster_data.get("scheduler_started", None)
    if scheduler is True:
        details.append("✓ Scheduler: Started")
    elif scheduler is False:
        details.append("✓ Scheduler: Stopped")
    else:
        details.append("Scheduler: Unknown")

    # Upgrade needed flag
    needs_upgrade = cluster_data.get("need_upgrade", False)
    if needs_upgrade:
        warnings.append(f"⚠ Cluster flagged as needing upgrade")
    else:
        details.append("✓ No upgrade flag set")

    # Validation messages
    val_msg = cluster_data.get("validation_message")
    if val_msg:
        warnings.append(f"Validation message: {val_msg}")

    if warnings:
        for w in warnings:
            details.append(f"⚠ {w}")
        return {"status": "WARNING", "details": details}

    return {"status": "PASS", "details": details}


def check_infrastructure_services(cluster_data: dict) -> dict:
    """Check infrastructure services (Zookeeper, Spark, Database)"""
    details = []
    all_ok = True

    # Zookeeper
    zk_mode = cluster_data.get("zookeeper_mode", "Unknown")
    zk_status = cluster_data.get("zookeeper_status", "Unknown")
    zk_conn = cluster_data.get("zookeeper", "Not configured")

    if zk_status == "Started":
        details.append(f"✓ Zookeeper ({zk_mode}): {zk_status}")
        details.append(f"  Connection: {zk_conn}")
    else:
        details.append(f"✗ Zookeeper ({zk_mode}): {zk_status}")
        all_ok = False

    # Spark
    spark_enabled = cluster_data.get("enable_spark", False)
    spark_mode = cluster_data.get("spark_mode", "Unknown")
    spark_status = cluster_data.get("spark_status", "Unknown")
    spark_master = cluster_data.get("spark_master", "Not configured")

    if spark_enabled:
        if spark_status == "Started":
            details.append(f"✓ Spark ({spark_mode}): {spark_status}")
            details.append(f"  Master: {spark_master}")
        else:
            details.append(f"✗ Spark ({spark_mode}): {spark_status}")
            all_ok = False
    else:
        details.append("Spark: Disabled")

    # Database
    db_type = cluster_data.get("db_type", "Unknown")
    db_status = cluster_data.get("db_status", "Unknown")
    db_conn = cluster_data.get("db_connection", "Not configured")

    if db_status == "Started":
        details.append(f"✓ Metadata Database ({db_type}): {db_status}")
        details.append(f"  Connection: {db_conn}")
    else:
        details.append(f"✗ Metadata Database ({db_type}): {db_status}")
        all_ok = False

    return {"status": "PASS" if all_ok else "FAIL", "details": details}


def check_node_topology(cluster_data: dict) -> dict:
    """Check node configuration and topology"""
    details = []
    warnings = []

    cluster_type = cluster_data.get("type", "Unknown")
    details.append(f"Cluster type: {cluster_type}")

    nodes = cluster_data.get("nodes", [])
    details.append(f"Total nodes: {len(nodes)}")

    for node in nodes:
        node_name = node.get("name", "unknown")
        node_type = node.get("type", "Unknown")
        node_status = node.get("status", "Unknown")
        node_handshake = node.get("node_handshake_status", "Unknown")

        # Node status
        if node_status == "online" and node_handshake == "HANDSHAKE_OK":
            details.append(f"✓ {node_name} ({node_type}): {node_status}")
        else:
            warnings.append(f"{node_name} ({node_type}): status={node_status}, handshake={node_handshake}")

        # Services on this node
        services = node.get("services", [])
        service_names = [s.get("name", "unknown") for s in services]
        if service_names:
            details.append(f"  Services: {', '.join(service_names)}")

        # Additional components
        if node.get("notebook"):
            details.append(f"  Components: notebook")
        if node.get("sqli"):
            details.append(f"  Components: sqli")

    # HA rolling restart concern: only flag when 2+ nodes run the same service
    # (both analytics or both loader), since a rolling restart would take down that service.
    # Note: node "type" is "HA" for all HA nodes — use services list instead.
    if len(nodes) >= 2:
        from collections import Counter
        service_counts = Counter()
        for n in nodes:
            for svc in n.get("services", []):
                svc_name = svc.get("name", "").lower()
                if svc_name in ("analytics", "loader"):
                    service_counts[svc_name] += 1
        shared_services = [svc for svc, count in service_counts.items() if count >= 2]
        if shared_services:
            details.append(f"HA topology: multiple nodes run {', '.join(shared_services)} — rolling restart coordination needed")

    if warnings:
        for w in warnings:
            details.append(f"⚠ {w}")
        return {"status": "WARNING", "details": details}

    return {"status": "PASS", "details": details}


def check_connectors(cluster_data: dict) -> dict:
    """Check all connectors (enabled and disabled)."""
    details = []
    status = "PASS"

    connectors = cluster_data.get("connectors", [])
    enabled_connectors = [c["connectorName"] for c in connectors if c.get("connectorEnabled", False)]
    disabled_connectors = [c["connectorName"] for c in connectors if not c.get("connectorEnabled", False)]

    details.append(f"Total connectors: {len(connectors)} ({len(enabled_connectors)} enabled, {len(disabled_connectors)} disabled)")

    if enabled_connectors:
        details.append("Enabled:")
        for conn in enabled_connectors:
            details.append(f"  ✓ {conn}")

    if disabled_connectors:
        details.append(f"Disabled ({len(disabled_connectors)} total):")
        for conn in disabled_connectors:
            details.append(f"  - {conn}")

    return {"status": status, "details": details}


def check_tenants(cluster_data: dict) -> dict:
    """Check tenant configuration"""
    details = []
    warnings = []

    tenants = cluster_data.get("tenants", [])
    details.append(f"Total tenants: {len(tenants)}")

    for tenant in tenants:
        name = tenant.get("name", "unknown")
        enabled = tenant.get("enabled", False)
        synced = tenant.get("isMSSynced", False)
        path = tenant.get("path", "Not configured")
        disk_quota = tenant.get("diskSpace", {}).get("diskSpace", "Unknown")

        status_icon = "✓" if enabled and synced else "⚠"
        details.append(f"{status_icon} Tenant: {name}")
        details.append(f"  Enabled: {enabled}, MS Synced: {synced}")
        details.append(f"  Path: {path}")
        details.append(f"  Disk quota: {disk_quota}")

        if not synced:
            warnings.append(f"Tenant '{name}' not synced with metadata store")

    if warnings:
        for w in warnings:
            details.append(f"⚠ {w}")
        return {"status": "WARNING", "details": details}

    return {"status": "PASS", "details": details}


def check_email_configuration(cluster_data: dict, is_cloud: bool = False) -> dict:
    """Check SMTP/Email configuration"""
    details = []
    warnings = []

    config = cluster_data.get("config", {})

    # Required SMTP fields
    mail_host = config.get("MAIL_HOST")
    mail_port = config.get("MAIL_PORT")
    mail_protocol = config.get("MAIL_PROTOCOL")
    mail_ssl = config.get("MAIL_SSL_ENABLED")
    service_mail = config.get("SERVICE_MAIL_ADDRESS")

    if mail_host and mail_port:
        details.append(f"✓ SMTP configured")
        details.append(f"  Host: {mail_host}:{mail_port}")
        details.append(f"  Protocol: {mail_protocol}")
        details.append(f"  SSL Enabled: {mail_ssl}")
        details.append(f"  Service email: {service_mail}")
    elif is_cloud:
        # Cloud clusters: SMTP is configured at tenant level, not cluster level
        details.append("ℹ Cloud deployment: SMTP configured at tenant level via CMC Tenant Configuration")
        if mail_host:
            details.append(f"  Cluster-level host: {mail_host}")
        if service_mail:
            details.append(f"  Service email: {service_mail}")
        return {"status": "PASS", "details": details}
    else:
        warnings.append("SMTP not fully configured")
        details.append(f"  Host: {mail_host or 'Not set'}")
        details.append(f"  Port: {mail_port or 'Not set'}")

    if warnings:
        for w in warnings:
            details.append(f"⚠ {w}")
        return {"status": "WARNING", "details": details}

    return {"status": "PASS", "details": details}


def check_notebook_sqli_status(cluster_data: dict) -> dict:
    """Check Notebook and SQL Interface status"""
    details = []
    all_ok = True

    # Overall notebook status
    has_notebook = cluster_data.get("has_notebook", False)
    notebook_status = cluster_data.get("notebook_status", "Unknown")
    sqli_status = cluster_data.get("sqli_status", "Unknown")

    details.append(f"Notebook capability: {has_notebook}")
    details.append(f"Overall notebook status: {notebook_status}")
    details.append(f"Overall SQLi status: {sqli_status}")

    # Per-node status
    nodes = cluster_data.get("nodes", [])
    for node in nodes:
        node_name = node.get("name", "unknown")

        # Notebook
        notebook = node.get("notebook")
        if notebook:
            nb_status = notebook.get("status", "Unknown")
            nb_handshake = notebook.get("notebook_handshake_status", "Unknown")

            if nb_status == "Started" or nb_handshake == "HANDSHAKE_OK":
                details.append(f"✓ {node_name}/notebook: {nb_status}")
            else:
                details.append(f"✗ {node_name}/notebook: {nb_status} (handshake: {nb_handshake})")
                if nb_status not in ["N/A", "Unknown"]:
                    all_ok = False

        # SQLi
        sqli = node.get("sqli")
        if sqli:
            sqli_st = sqli.get("status", "Unknown")

            if sqli_st == "Started":
                details.append(f"✓ {node_name}/sqli: {sqli_st}")
            else:
                details.append(f"✗ {node_name}/sqli: {sqli_st}")
                if sqli_st not in ["N/A", "Unknown"]:
                    all_ok = False

    return {"status": "PASS" if all_ok else "WARNING", "details": details}


def check_database_migration(cluster_data: dict) -> dict:
    """Check database type for Oracle -> MySQL migration status"""
    details = []

    db_type = cluster_data.get("db_type", "Unknown")
    db_connection = cluster_data.get("db_connection", "")

    details.append(f"Database type: {db_type}")

    if "oracle" in db_type.lower() or "oracle" in db_connection.lower():
        details.append("⚠ Oracle database detected - MySQL migration may be needed")
        return {"status": "WARNING", "details": details}
    elif "mysql" in db_type.lower() or "mysql" in db_connection.lower():
        details.append("✓ MySQL database - no migration needed")
    else:
        details.append(f"Database type: {db_type}")

    # Check migration user/password fields
    migration_user = cluster_data.get("migrations_user")
    migration_pw = cluster_data.get("migrations_pw")

    if migration_user or migration_pw:
        details.append("Migration credentials configured")

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
