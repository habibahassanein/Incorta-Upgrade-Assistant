"""
Cluster Metadata Extractor
Automatically extracts upgrade-relevant information from CMC cluster data.
Eliminates need for questionnaire - infer everything from cluster JSON!
"""

from typing import Dict, Any, List, Optional
import os


def extract_cluster_metadata(cluster_data: Dict[str, Any], 
                            include_api_calls: bool = True) -> Dict[str, Any]:
    """
    Extract all upgrade-relevant metadata from cluster JSON.
    
    Args:
        cluster_data: Cluster data from CMC API
        include_api_calls: If True, makes additional API calls for tenant storage and integrations
    
    Returns a structured dict with all auto-detected information.
    """
    cluster_name = cluster_data.get("name", "Unknown")
    
    metadata = {
        # Basic Info
        "cluster_name": cluster_name,
        
        # Deployment Type Detection
        "deployment_type": detect_deployment_type(cluster_data),
        
        # Database Detection
        "database": detect_database_info(cluster_data),
        
        # Topology Detection
        "topology": detect_topology(cluster_data),
        
        # Feature Detection
        "features": detect_features(cluster_data),
        
        # Infrastructure Detection
        "infrastructure": detect_infrastructure(cluster_data),
        
        # Service Status
        "service_status": detect_service_status(cluster_data),
        
        # Risk Assessment
        "risks": assess_upgrade_risks(cluster_data),
        
        # Version Information (Enhanced)
        "version_info": detect_version_info(cluster_data),
    }
    
    # Optional: Make additional API calls for tenant storage and integrations
    if include_api_calls:
        try:
            from clients.cmc_client import CMCClient
            from context.user_context import user_context
            ctx = user_context.get()
            client = CMCClient(
                url=ctx.get("cmc_url"),
                user=ctx.get("cmc_user"),
                password=ctx.get("cmc_password"),
                cluster_name=ctx.get("cmc_cluster_name"),
            )

            # Tenant Storage (from /api/v1/clusters/{cluster}/tenants)
            metadata["tenant_storage"] = detect_tenant_storage(cluster_name, client)

            # Integrations (from /api/v1/clusters/{cluster}/config)
            metadata["integrations"] = detect_integrations(cluster_name, client)
        except Exception as e:
            metadata["tenant_storage"] = {"error": f"Could not retrieve: {str(e)}"}
            metadata["integrations"] = {"error": f"Could not retrieve: {str(e)}"}
    
    return metadata


def detect_deployment_type(cluster_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Auto-detect: Cloud vs On-Prem vs Managed
    
    Detection logic:
    - path starts with 'gs://' = Google Cloud
    - path starts with 's3://' = AWS Cloud
    - path starts with 'wasbs://' or 'abfss://' = Azure Cloud
    - path starts with 'file://' = On-Premises
    - Otherwise = Cloud (generic)
    """
    path = cluster_data.get("path", "")
    
    if path.startswith("gs://"):
        provider = "Google Cloud (GCP)"
        deployment = "Cloud"
    elif path.startswith("s3://"):
        provider = "Amazon Web Services (AWS)"
        deployment = "Cloud"
    elif path.startswith(("wasbs://", "abfss://")):
        provider = "Microsoft Azure"
        deployment = "Cloud"
    elif path.startswith("file://"):
        provider = "On-Premises"
        deployment = "On-Prem"
    else:
        provider = "Unknown Cloud Provider"
        deployment = "Cloud"
    
    return {
        "deployment_type": deployment,
        "cloud_provider": provider,
        "storage_path": path,
        "is_cloud": deployment == "Cloud"
    }


def detect_database_info(cluster_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Auto-detect: Database type, migration needs, metadata service
    
    Detection logic:
    - db_type field = mysql/oracle/postgres
    - db_connection contains JDBC URL
    - IsMSEnabled = Metadata service enabled
    """
    db_type = cluster_data.get("db_type", "unknown")
    db_connection = cluster_data.get("db_connection", "")
    is_ms_enabled = cluster_data.get("IsMSEnabled", False)
    
    # Parse JDBC URL for more details
    db_host = "unknown"
    if "jdbc:" in db_connection:
        # Extract host from JDBC URL
        parts = db_connection.split("//")
        if len(parts) > 1:
            host_port = parts[1].split("/")[0].split(":")[0]
            db_host = host_port
    
    return {
        "db_type": db_type.upper(),
        "db_connection": db_connection,
        "metadata_service_enabled": is_ms_enabled,
        "migration_needed": db_type.lower() != "mysql",  # Oracle requires migration
        "db_host": db_host,
    }


def detect_topology(cluster_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Auto-detect: Typical vs Clustered, node count, node types
    
    Detection logic:
    - 1 node = Typical
    - 2+ nodes = Clustered/Custom
    - Node type from 'type' field (HA, Typical, etc.)
    """
    nodes = cluster_data.get("nodes", [])
    node_count = len(nodes)
    
    # Determine cluster type
    if node_count == 1:
        topology_type = "Typical"
    else:
        topology_type = "Clustered/Custom"
    
    # Extract node details
    node_details = []
    for node in nodes:
        node_details.append({
            "name": node.get("name"),
            "type": node.get("type", "Unknown"),
            "host": node.get("host"),
            "status": node.get("status"),
            "services": [s.get("name") for s in node.get("services", [])],
        })
    
    return {
        "topology_type": topology_type,
        "node_count": node_count,
        "nodes": node_details,
        "is_ha": any(n.get("type") == "HA" for n in nodes),
    }


def detect_features(cluster_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Auto-detect: Enabled features (Notebook, Spark, SQLi, etc.)
    
    Detection logic:
    - has_notebook field
    - enable_spark field
    - Check for notebook/sqli/kyuubi in nodes
    """
    features = {
        "notebook": cluster_data.get("has_notebook", False),
        "spark": cluster_data.get("enable_spark", False),
        "sqli": False,
        "kyuubi": False,
        "distributed_session": cluster_data.get("distributed_session") is not None,
    }
    
    # Check nodes for SQLi and Kyuubi
    for node in cluster_data.get("nodes", []):
        if node.get("sqli"):
            features["sqli"] = True
        if node.get("kyuubi"):
            features["kyuubi"] = True
    
    # List enabled connectors
    connectors = cluster_data.get("connectors", [])
    enabled_connectors = [
        c.get("connectorName") 
        for c in connectors 
        if c.get("connectorEnabled", False)
    ]
    
    features["connectors"] = enabled_connectors
    features["connector_count"] = len(enabled_connectors)
    
    return features


def detect_infrastructure(cluster_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Auto-detect: Spark mode, Zookeeper mode, K8s usage
    
    Detection logic:
    - spark_mode field (External/Embedded)
    - zookeeper_mode field (External/Embedded)
    - SPARK_MASTER_URL contains 'k8s://' = Kubernetes
    """
    config = cluster_data.get("config", {})
    spark_master_url = config.get("SPARK_MASTER_URL", "")
    
    # Detect Spark deployment
    if "k8s://" in spark_master_url:
        spark_deployment = "Kubernetes"
    elif cluster_data.get("spark_mode") == "External":
        spark_deployment = "External"
    else:
        spark_deployment = "Embedded"
    
    return {
        "zookeeper_mode": cluster_data.get("zookeeper_mode", "Unknown"),
        "zookeeper_status": cluster_data.get("zookeeper_status", "Unknown"),
        "spark_mode": cluster_data.get("spark_mode", "Unknown"),
        "spark_deployment": spark_deployment,
        "spark_master_url": spark_master_url,
        "spark_status": cluster_data.get("spark_status", "Unknown"),
        "db_status": cluster_data.get("db_status", "Unknown"),
    }


def detect_service_status(cluster_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Auto-detect: Which services are running/stopped/error
    
    Detection logic:
    - Parse analytics_status, loader_status, etc.
    - Check each node's services status
    """
    overall_status = {
        "analytics": cluster_data.get("analytics_status", {}).get("statusType", "Unknown"),
        "loader": cluster_data.get("loader_status", {}).get("statusType", "Unknown"),
        "notebook": cluster_data.get("notebook_status", "Unknown"),
        "sqli": cluster_data.get("sqli_status", "Unknown"),
        "kyuubi": cluster_data.get("kyuubi_status", "Unknown"),
    }
    
    # Node-level service status
    node_services = []
    for node in cluster_data.get("nodes", []):
        for service in node.get("services", []):
            node_services.append({
                "node": node.get("name"),
                "service": service.get("name"),
                "status": service.get("status", {}).get("statusType", "Unknown"),
                "memory_assigned": service.get("assigned_on_heap_memory", 0),
            })
    
    return {
        "overall": overall_status,
        "node_level": node_services,
        "all_healthy": all(
            status in ["Running", "Started"] 
            for status in overall_status.values()
        ),
    }


def assess_upgrade_risks(cluster_data: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Auto-detect upgrade risks based on cluster state
    
    Returns risks categorized as: blockers, warnings, info
    """
    blockers = []
    warnings = []
    info = []
    
    # Check service status
    analytics_status = cluster_data.get("analytics_status", {}).get("statusType", "Unknown")
    loader_status = cluster_data.get("loader_status", {}).get("statusType", "Unknown")
    
    if analytics_status in ["Error", "Stopped"]:
        blockers.append(f"Analytics service is {analytics_status} - must be running for upgrade")
    
    if loader_status in ["Error", "Stopped"]:
        blockers.append(f"Loader service is {loader_status} - must be running for upgrade")
    
    # Check infrastructure
    zk_status = cluster_data.get("zookeeper_status", "Unknown")
    if zk_status not in ["Started", "Running"]:
        blockers.append(f"Zookeeper is {zk_status} - required for upgrade")
    
    spark_status = cluster_data.get("spark_status", "Unknown")
    if spark_status not in ["Started", "Running"]:
        warnings.append(f"Spark is {spark_status} - may be needed for upgrade")
    
    db_status = cluster_data.get("db_status", "Unknown")
    if db_status not in ["Started", "Running"]:
        blockers.append(f"Database is {db_status} - critical for upgrade")
    
    # Check if upgrade flag is set
    if cluster_data.get("need_upgrade", False):
        warnings.append("Cluster has 'need_upgrade' flag set - check version compatibility")
    
    # Database migration check
    db_type = cluster_data.get("db_type", "").lower()
    if db_type == "oracle":
        warnings.append("Oracle database - migration to MySQL may be required for newer versions")
    
    # Node status check
    nodes = cluster_data.get("nodes", [])
    for node in nodes:
        if node.get("status") == "offline":
            blockers.append(f"Node '{node.get('name')}' is offline - all nodes must be online")
    
    # Info items
    info.append(f"Cluster type: {cluster_data.get('type', 'Unknown')}")
    info.append(f"Database: {cluster_data.get('db_type', 'Unknown').upper()}")
    info.append(f"Nodes: {len(nodes)}")
    
    return {
        "blockers": blockers,
        "warnings": warnings,
        "info": info,
        "risk_level": "HIGH" if blockers else ("MEDIUM" if warnings else "LOW")
    }


def detect_version_info(cluster_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhanced version detection with warnings.
    
    Note: The 'version' field in cluster data is the schema version (1 or 2),
    NOT the actual Incorta version. Actual version must be retrieved from file system.
    """
    version_field = cluster_data.get("version")  # This is 1 or 2, NOT actual version
    need_upgrade = cluster_data.get("need_upgrade", False)
    
    nodes = cluster_data.get("nodes", [])
    node_versions = {}
    for node in nodes:
        node_name = node.get("name")
        node_version = node.get("node_version")
        if node_version:
            node_versions[node_name] = node_version
    
    # Determine if all nodes have same version
    unique_versions = set(node_versions.values()) if node_versions else set()
    
    return {
        "cluster_version_field": version_field,  # Schema version, NOT Incorta version
        "node_versions": node_versions,
        "version_consistent": len(unique_versions) <= 1,
        "needs_upgrade_flag": need_upgrade,
        "actual_version_available": False,  # Must get from file system
        "recommendation": "⚠️ Retrieve actual Incorta version from <INCORTA_HOME>/IncortaNode/version.txt or CMC UI",
    }


def detect_tenant_storage(cluster_name: str, client) -> Dict[str, Any]:
    """
    Get tenant storage details from /api/v1/clusters/{cluster}/tenants endpoint.
    
    Provides:
    - Tenant names and IDs
    - Storage paths
    - Disk quotas
    - Enabled status
    """
    import requests
    
    try:
        verify_ssl = os.getenv("VERIFY_SSL", "true").lower() in ("1", "true", "yes")
        
        r = requests.get(
            f"{client.url}/api/v1/clusters/{cluster_name}/tenants",
            headers=client._headers(),
            verify=verify_ssl,
            timeout=30
        )
        
        if r.status_code == 200:
            body = r.text.strip()
            if not body or body.startswith("<!") or body.startswith("<html") or body.startswith("<HTML"):
                return {"error": "Tenants API returned non-JSON response", "status": "failed"}
            tenants = r.json()

            tenant_details = []
            unlimited_count = 0
            
            for t in tenants:
                disk_space_obj = t.get("diskSpace", {})
                disk_quota = disk_space_obj.get("diskSpace", "unknown")
                
                if disk_quota == "unlimited":
                    unlimited_count += 1
                
                tenant_details.append({
                    "name": t.get("name"),
                    "id": t.get("id"),
                    "path": t.get("path"),
                    "enabled": t.get("enabled", False),
                    "disk_quota": disk_quota,
                    "disk_unit": disk_space_obj.get("unit", ""),
                    "ms_synced": t.get("isMSSynced"),
                })
            
            return {
                "total_tenants": len(tenants),
                "tenants": tenant_details,
                "unlimited_quota_count": unlimited_count,
                "status": "success",
            }
        else:
            return {
                "error": f"API returned status {r.status_code}",
                "status": "failed",
            }
    
    except Exception as e:
        return {
            "error": str(e),
            "status": "error",
        }


def detect_integrations(cluster_name: str, client) -> Dict[str, Any]:
    """
    Get configured integrations from /api/v1/clusters/{cluster}/config endpoint.
    
    Provides:
    - Enabled integrations (Box, Slack, Mapbox, CMS, etc.)
    - Which services require restart
    - Configuration categories
    """
    import requests
    
    try:
        verify_ssl = os.getenv("VERIFY_SSL", "true").lower() in ("1", "true", "yes")
        
        r = requests.get(
            f"{client.url}/api/v1/clusters/{cluster_name}/config",
            headers=client._headers(),
            verify=verify_ssl,
            timeout=30
        )
        
        if r.status_code == 200:
            body = r.text.strip()
            if not body or body.startswith("<!") or body.startswith("<html") or body.startswith("<HTML"):
                return {"error": "Config API returned non-JSON response", "status": "failed"}
            data = r.json()
            config_items = data.get("config", [])
            
            integrations = {}
            restart_required_items = []
            categories = {}
            
            for item in config_items:
                category = item.get("category", "Other")
                key = item.get("key")
                value = item.get("value")
                requires_restart = item.get("requiresRestart", False)
                service_to_restart = item.get("serviceToRestart", "none")
                
                # Track by category
                if category not in categories:
                    categories[category] = []
                categories[category].append(key)
                
                # Track integrations specifically
                if category == "Integration":
                    # Determine if enabled (not None, empty, or false)
                    enabled = value not in [None, "", "false", False, "False"]
                    
                    integrations[key] = {
                        "enabled": enabled,
                        "requires_restart": requires_restart,
                        "service_to_restart": service_to_restart,
                        "value_set": value is not None and value != "",
                    }
                    
                    if requires_restart and enabled:
                        restart_required_items.append({
                            "key": key,
                            "service": service_to_restart,
                        })
            
            return {
                "total_config_items": len(config_items),
                "total_integrations": len(integrations),
                "integrations": integrations,
                "enabled_integrations": [k for k, v in integrations.items() if v["enabled"]],
                "restart_required_items": restart_required_items,
                "restart_required_count": len(restart_required_items),
                "categories": {cat: len(items) for cat, items in categories.items()},
                "status": "success",
            }
        else:
            return {
                "error": f"API returned status {r.status_code}",
                "status": "failed",
            }
    
    except Exception as e:
        return {
            "error": str(e),
            "status": "error",
        }


def format_metadata_report(metadata: Dict[str, Any]) -> str:
    """
    Format extracted metadata into readable markdown report.
    """
    report = []
    
    report.append(f"# Cluster Metadata: {metadata['cluster_name']}\n")
    
    # Deployment
    dep = metadata["deployment_type"]
    report.append(f"## Deployment Information")
    report.append(f"- **Type**: {dep['deployment_type']}")
    report.append(f"- **Provider**: {dep['cloud_provider']}")
    report.append(f"- **Storage Path**: `{dep['storage_path']}`\n")
    
    # Database
    db = metadata["database"]
    report.append(f"## Database Configuration")
    report.append(f"- **Type**: {db['db_type']}")
    report.append(f"- **Metadata Service**: {'Enabled' if db['metadata_service_enabled'] else 'Disabled'}")
    report.append(f"- **Migration Needed**: {'⚠️ Yes (Oracle → MySQL)' if db['migration_needed'] else '✅ No'}\n")
    
    # Topology
    topo = metadata["topology"]
    report.append(f"## Cluster Topology")
    report.append(f"- **Type**: {topo['topology_type']}")
    report.append(f"- **Node Count**: {topo['node_count']}")
    report.append(f"- **HA Enabled**: {'✅ Yes' if topo['is_ha'] else '❌ No'}")
    for node in topo["nodes"]:
        report.append(f"  - **{node['name']}**: {node['type']} ({node['status']}) - Services: {', '.join(node['services'])}")
    report.append("")
    
    # Features
    feat = metadata["features"]
    report.append(f"## Enabled Features")
    report.append(f"- **Notebook**: {'✅ Enabled' if feat['notebook'] else '❌ Disabled'}")
    report.append(f"- **Spark**: {'✅ Enabled' if feat['spark'] else '❌ Disabled'}")
    report.append(f"- **SQLi**: {'✅ Enabled' if feat['sqli'] else '❌ Disabled'}")
    report.append(f"- **Connectors**: {feat['connector_count']} enabled")
    if feat['connectors']:
        report.append(f"  - {', '.join(feat['connectors'])}")
    report.append("")
    
    # Infrastructure
    infra = metadata["infrastructure"]
    report.append(f"## Infrastructure")
    report.append(f"- **Zookeeper**: {infra['zookeeper_mode']} ({infra['zookeeper_status']})")
    report.append(f"- **Spark**: {infra['spark_deployment']} ({infra['spark_status']})")
    report.append(f"- **Database**: {infra['db_status']}\n")
    
    # Service Status
    status = metadata["service_status"]
    report.append(f"## Service Status")
    for service, state in status["overall"].items():
        icon = "✅" if state in ["Running", "Started"] else ("⚠️" if state == "Stopped" else "❌")
        report.append(f"- **{service.capitalize()}**: {icon} {state}")
    report.append("")
    
    # Version Information
    if "version_info" in metadata:
        ver = metadata["version_info"]
        report.append(f"## Version Information")
        report.append(f"- **Schema Version**: {ver.get('cluster_version_field', 'Unknown')}")
        report.append(f"- **Version Consistent Across Nodes**: {'✅ Yes' if ver.get('version_consistent') else '❌ No'}")
        if ver.get("node_versions"):
            report.append(f"- **Node Versions**: {ver['node_versions']}")
        report.append(f"- **Needs Upgrade Flag**: {'⚠️ Yes' if ver.get('needs_upgrade_flag') else '✅ No'}")
        report.append(f"- **Recommendation**: {ver.get('recommendation', 'N/A')}\n")
    
    # Tenant Storage
    if "tenant_storage" in metadata:
        ts = metadata["tenant_storage"]
        if ts.get("status") == "success":
            report.append(f"## Tenant Storage")
            report.append(f"- **Total Tenants**: {ts.get('total_tenants', 0)}")
            report.append(f"- **Unlimited Quota Tenants**: {ts.get('unlimited_quota_count', 0)}")
            
            for tenant in ts.get("tenants", []):
                enabled_icon = "✅" if tenant.get("enabled") else "❌"
                quota_display = tenant.get("disk_quota")
                if quota_display == "unlimited":
                    quota_display = "♾️ Unlimited"
                elif quota_display != "unknown":
                    quota_display = f"{quota_display} {tenant.get('disk_unit', '')}"
                
                report.append(f"  - **{tenant.get('name')}**: {enabled_icon} Enabled, Quota: {quota_display}")
            report.append("")
    
    # Integrations
    if "integrations" in metadata:
        integ = metadata["integrations"]
        if integ.get("status") == "success":
            report.append(f"## Configured Integrations")
            report.append(f"- **Total Configuration Items**: {integ.get('total_config_items', 0)}")
            report.append(f"- **Total Integrations**: {integ.get('total_integrations', 0)}")
            report.append(f"- **Enabled Integrations**: {len(integ.get('enabled_integrations', []))}")
            
            enabled_integ = integ.get("enabled_integrations", [])
            if enabled_integ:
                report.append(f"\n**Enabled Integration Services:**")
                for key in enabled_integ:
                    integ_info = integ.get("integrations", {}).get(key, {})
                    restart_icon = "🔄" if integ_info.get("requires_restart") else "  "
                    report.append(f"  {restart_icon} {key}")
            
            if integ.get("restart_required_count", 0) > 0:
                report.append(f"\n⚠️ **{integ['restart_required_count']} integration(s) require service restart if modified**")
            
            # Categories summary
            categories = integ.get("categories", {})
            if categories:
                report.append(f"\n**Configuration Categories:**")
                for cat, count in categories.items():
                    report.append(f"  - {cat}: {count} items")
            report.append("")
    
    # Risks
    risks = metadata["risks"]
    report.append(f"## Upgrade Risk Assessment: {risks['risk_level']}")
    
    if risks["blockers"]:
        report.append(f"\n### ❌ Blockers (Must Fix Before Upgrade)")
        for blocker in risks["blockers"]:
            report.append(f"- {blocker}")
    
    if risks["warnings"]:
        report.append(f"\n### ⚠️ Warnings")
        for warning in risks["warnings"]:
            report.append(f"- {warning}")
    
    if risks["info"]:
        report.append(f"\n### ℹ️ Information")
        for item in risks["info"]:
            report.append(f"- {item}")
    
    return "\n".join(report)


# Example usage
if __name__ == "__main__":
    import json
    
    # Load cluster data
    with open("cluster_data_full.json", "r") as f:
        cluster_data = json.load(f)
    
    # Extract metadata
    metadata = extract_cluster_metadata(cluster_data)
    
    # Generate report
    report = format_metadata_report(metadata)
    print(report)
    
    # Also output as JSON
    with open("cluster_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
