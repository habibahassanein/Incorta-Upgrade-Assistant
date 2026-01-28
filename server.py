
import json
import os
import sys
from typing import Literal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from mcp.server.fastmcp import FastMCP

from workflows.pre_upgrade_validation import run_validation
from workflows.upgrade_research import research_upgrade_path
from tools.qdrant_tool import search_knowledge_base
from tools.incorta_tools import query_zendesk, query_jira, get_zendesk_schema, get_jira_schema
from tools.extract_cluster_metadata import extract_cluster_metadata, format_metadata_report
from clients.cloud_portal_client import CloudPortalClient


app = FastMCP("incorta-upgrade-agent", stateless_http=True)


# ==========================================
# CUSTOMER UPGRADE RECOMMENDATION WORKFLOW
# ==========================================


@app.tool()
def run_pre_upgrade_validation(cluster_name: str | None = None) -> str:
    """
    [STEP 1 - ALWAYS RUN FIRST] Validates Incorta cluster health before upgrade.
    Performs comprehensive pre-upgrade health checks including: service status (Analytics, Loader),
    memory usage, node topology, infrastructure services (Spark, Zookeeper, DB), connectors, tenants,
    email configuration, and database migration status.

    USE CASE: Run at the START of any upgrade conversation to baseline cluster health.
    Results identify BLOCKERS (critical issues that must be resolved before upgrade) and
    WARNINGS (issues to monitor). Store results for comparison with post-upgrade validation.

    OUTPUT: Markdown report with Healthy, Warnings, Blockers sections.
    Use blockers to determine upgrade risk level (HIGH/MEDIUM/LOW).

    Args:
        cluster_name: CMC cluster name (e.g., 'customCluster'). Defaults to CMC_CLUSTER_NAME env var.
    """
    if cluster_name is None:
        cluster_name = os.getenv("CMC_CLUSTER_NAME")
        if not cluster_name:
            return "Error: No cluster_name provided and CMC_CLUSTER_NAME env var not set"
    return run_validation(cluster_name)


@app.tool()
def extract_cluster_metadata_tool(
    cluster_name: str | None = None,
    format: Literal["json", "markdown", "both"] = "both"
) -> str:
    """
    [AUTO-DETECTION] Automatically extracts upgrade-relevant metadata from cluster data.
    Eliminates need to ask user questions - infers everything from CMC cluster JSON!

    CLUSTER NAMING: This tool uses the CMC API and requires the CMC cluster name.
    Example: If CMC shows 'customCluster', use that name (NOT the Cloud Portal name like 'habibascluster').

    AUTO-DETECTS (No User Input Needed):
    - Deployment Type: Cloud (GCP/AWS/Azure) vs On-Premises (from storage path)
    - Database Type: MySQL/Oracle/PostgreSQL + migration requirements
    - Topology: Typical (1 node) vs Clustered/Custom (2+ nodes), HA status
    - Features: Notebook, Spark, SQLi, Kyuubi enabled/disabled status
    - Infrastructure: Spark mode (K8s/External/Embedded), Zookeeper (External/Embedded)
    - Service Status: All service states (Analytics, Loader, Notebook, SQLi)
    - Connectors: List of enabled connectors
    - Risk Assessment: AUTO-CLASSIFY as HIGH/MEDIUM/LOW risk based on blockers

    Args:
        cluster_name: CMC cluster name. Defaults to CMC_CLUSTER_NAME env var.
        format: Output format - 'json', 'markdown', or 'both' (default).
    """
    if cluster_name is None:
        cluster_name = os.getenv("CMC_CLUSTER_NAME")
        if not cluster_name:
            return "Error: No cluster_name provided and CMC_CLUSTER_NAME env var not set"
    from clients.cmc_client import CMCClient
    client = CMCClient()
    cluster_data = client.get_cluster(cluster_name)

    metadata = extract_cluster_metadata(cluster_data)

    if format == "json":
        return json.dumps(metadata, indent=2)
    elif format == "markdown":
        return format_metadata_report(metadata)
    else:  # "both" is default
        markdown_report = format_metadata_report(metadata)
        json_data = json.dumps(metadata, indent=2)
        return f"{markdown_report}\n\n---\n\n## Raw Metadata (JSON)\n\n```json\n{json_data}\n```"


@app.tool()
def research_upgrade_path_tool(from_version: str, to_version: str) -> str:
    """
    [STEP 2 - VERSION RESEARCH] Research upgrade path between two Incorta versions.
    Uses semantic search to find release notes, known issues, and community experiences.

    CRITICAL: For customer-specific recommendations, use 'search_upgrade_knowledge' multiple times instead
    to build a SEQUENTIAL path ordered by RELEASE DATE (not version number).
    Example: 2024.1.x (Jan) comes BEFORE 2024.7.x (Oct) even though 7 > 1.

    USE THIS TOOL FOR: Quick research of a single upgrade path (non-customer-specific).

    Args:
        from_version: Current Incorta version (e.g., '2024.1.0')
        to_version: Target Incorta version (e.g., '2024.7.0')
    """
    return research_upgrade_path(from_version, to_version)


@app.tool()
def search_upgrade_knowledge(query: str, limit: int = 10) -> str:
    """
    [GRANULAR VERSION SEARCH] Search Incorta documentation, community, and support articles.
    Primary tool for building customer-specific upgrade recommendations.

    REQUIRED SEARCHES FOR CUSTOMER UPGRADES:
    1. START: 'Incorta Release Support Policy' - Get ALL versions with release dates
    2. BUILD PATH: Identify ALL versions between current -> target (ordered by release date)
    3. FOR EACH VERSION: Search '[VERSION] release notes', '[VERSION] upgrade considerations'
    4. DEPENDENCIES: '[VERSION] python version', '[VERSION] spark version'
    5. TRANSITIONS: 'upgrade from [V1] to [V2]' for critical version jumps

    Args:
        query: Search query (e.g., 'Incorta Release Support Policy', '2024.7.0 release notes')
        limit: Number of results to return (default: 10)
    """
    result = search_knowledge_base({"query": query, "limit": limit})
    return json.dumps(result, indent=2)


@app.tool()
def get_zendesk_schema_tool() -> str:
    """
    [STEP 3A - BEFORE CUSTOMER TICKETS] Get Zendesk schema to understand available fields.
    Call this BEFORE querying customer tickets to see table structure.

    KEY TABLES FOR UPGRADES:
    - ticket (44 cols): Main ticket data - id, subject, priority, status, organization_id
    - ticket_customfields_v (15 cols): Severity, Deployment_Type, Release, Fixed_in
    - Ticket_Current_Release (2 cols): ticket_id, custom_field_value (current version)
    - Ticket_Target_Release (2 cols): ticket_id, custom_field_value (target version)
    - organization (15 cols): Customer data - id, name, region
    - ticket_jira_links (5 cols): Zendesk <-> Jira linkage
    """
    result = get_zendesk_schema({"fetch_schema": True})
    return json.dumps(result, indent=2)


@app.tool()
def query_upgrade_tickets(spark_sql: str) -> str:
    """
    [STEP 3B - CUSTOMER ISSUES] Query Zendesk for customer-reported support tickets.
    Essential for customer-specific upgrade recommendations. Schema: ZendeskTickets

    Example query for customer's open issues:
    SELECT t.id, t.subject, t.priority, t.status, tcf.Severity, o.name AS customer_name
    FROM ZendeskTickets.ticket t
    JOIN ZendeskTickets.organization o ON t.organization_id = o.id
    LEFT JOIN ZendeskTickets.ticket_customfields_v tcf ON t.id = tcf.ticket_id
    WHERE o.name LIKE '%[CUSTOMER_NAME]%'
    AND t.status IN ('open', 'pending', 'hold')
    ORDER BY t.priority DESC LIMIT 50

    Args:
        spark_sql: Spark SQL query to execute on ZendeskTickets schema
    """
    result = query_zendesk({"spark_sql": spark_sql})
    return json.dumps(result, indent=2)


@app.tool()
def get_jira_schema_tool() -> str:
    """
    [STEP 4A - BEFORE JIRA QUERIES] Get Jira schema to understand available fields.
    Call this BEFORE querying bugs/features to see table structure.

    KEY TABLES FOR UPGRADES:
    - Issues (324 cols): Main issue data - Key, Summary, StatusName, PriorityName, Customer
    - IssueFixVersions (10 cols): Which versions fix each issue
    - IssueAffectedVersions (8 cols): Versions affected by bugs
    - IssueLinks (11 cols): Issue relationships
    - IssueComponents (7 cols): Product areas
    """
    result = get_jira_schema({"fetch_schema": True})
    return json.dumps(result, indent=2)


@app.tool()
def query_upgrade_issues(spark_sql: str) -> str:
    """
    [STEP 4B - BUG TRACKING & FIXES] Query Jira for engineering bugs, features, and fixes.
    Critical for determining if customer's issues are fixed in target version. Schema: Jira_F

    Example query for bugs fixed in target version:
    SELECT i.Key, i.Summary, i.StatusName, ifv.Name AS fix_version
    FROM Jira_F.Issues i
    JOIN Jira_F.IssueFixVersions ifv ON i.Key = ifv.IssueKey
    WHERE ifv.Name = '[TARGET_VERSION]' AND i.IssueTypeName = 'Bug'
    ORDER BY i.PriorityName DESC LIMIT 100

    Args:
        spark_sql: Spark SQL query to execute on Jira_F schema
    """
    result = query_jira({"spark_sql": spark_sql})
    return json.dumps(result, indent=2)


@app.tool()
def get_cloud_metadata(
    cluster_name: str | None = None,
    include_consumption: bool = True,
    include_users: bool = True
) -> str:
    """
    [CLOUD-SPECIFIC DATA] Get cloud metadata from Cloud Portal API.
    Provides data NOT available in CMC API. Call AFTER extract_cluster_metadata.

    CLUSTER NAMING: This tool uses the Cloud Portal API and requires the Cloud Portal cluster name.
    Example: If Cloud Portal UI shows 'habibascluster', use that name (NOT the CMC name).

    NEW DATA AVAILABLE:
    - Consumption & Cost: Daily/monthly power units, usage trends
    - User Management: Authorized users, roles (owner/developer), last login times
    - Build Details: Custom build IDs, exact versions (Spark, Python)
    - Feature Flags: MLflow, Chat/OpenAI, Delta Share, Data Agent status
    - Upgrade History: Last upgrade timestamp, upgrade patterns

    Args:
        cluster_name: Cloud Portal cluster name. Defaults to CLOUD_PORTAL_CLUSTER_NAME env var.
        include_consumption: Include consumption/cost data (default: True)
        include_users: Include authorized users (default: True)
    """
    if cluster_name is None:
        cluster_name = os.getenv("CLOUD_PORTAL_CLUSTER_NAME")
        if not cluster_name:
            return "Error: No cluster_name provided and CLOUD_PORTAL_CLUSTER_NAME env var not set"
    user_id = os.getenv("CLOUD_PORTAL_USER_ID")
    if not user_id:
        return "Error: CLOUD_PORTAL_USER_ID environment variable not set"

    cloud_client = CloudPortalClient()
    clusters_info = cloud_client.get_clusters_info(user_id)

    our_cluster = None
    for item in clusters_info.get("instances", []):
        instance = item.get("instance", {})
        if instance.get("name") == cluster_name:
            our_cluster = instance
            break

    if not our_cluster:
        return f"Error: Cluster '{cluster_name}' not found in Cloud Portal"

    instance_uuid = our_cluster.get("id")

    report_lines = [
        f"# Cloud Portal Metadata: {cluster_name}",
        "",
        "## Instance Details",
        f"- **UUID:** `{instance_uuid}`",
        f"- **Build:** {our_cluster.get('customBuild')} ({our_cluster.get('customBuildID')})",
        f"- **Platform:** {our_cluster.get('platform')} - {our_cluster.get('region')}/{our_cluster.get('zone')}",
        f"- **K8s Cluster:** {our_cluster.get('k8sClusterCode')}",
        f"- **Status:** {our_cluster.get('status')}",
        "",
        "## Software Versions",
        f"- **Spark:** {our_cluster.get('incortaSparkVersion')}",
        f"- **Python:** {our_cluster.get('pythonVersion')}",
        "",
    ]

    if include_consumption:
        try:
            consumption = cloud_client.get_consumption(user_id, instance_uuid)
            total_pu = consumption.get('consumptionAgg', {}).get('totalAgg', 0)
            daily_data = consumption.get('consumptionAgg', {}).get('total', {}).get('daily', [])

            if daily_data:
                avg_pu = sum(d.get('powerUnit', 0) for d in daily_data) / len(daily_data)
                recent_7 = daily_data[-7:] if len(daily_data) >= 7 else daily_data

                report_lines.extend([
                    "## Consumption & Cost",
                    f"- **Total (This Month):** {total_pu:.6f} Power Units",
                    f"- **Daily Average:** {avg_pu:.6f} PU/day",
                    f"- **Estimated Downtime Cost (4h):** {(avg_pu / 24 * 4):.6f} PU",
                    "",
                    "**Recent Trend (Last 7 Days):**",
                ])

                for day in recent_7:
                    date = day.get('startTime', 'Unknown')
                    pu = day.get('powerUnit', 0)
                    report_lines.append(f"  - {date}: {pu:.6f} PU")

                report_lines.append("")

        except Exception as e:
            report_lines.extend([
                "## Consumption & Cost",
                f"Could not fetch consumption data: {str(e)}",
                ""
            ])

    if include_users:
        try:
            users_data = cloud_client.get_authorized_users(user_id, cluster_name)
            users_list = users_data.get('authorizedUserRoles', [])

            report_lines.extend([
                f"## Authorized Users ({len(users_list)} total)",
                ""
            ])

            for user_item in users_list:
                user = user_item.get('user', {})
                role = user_item.get('authorizedRoles', [{}])[0].get('role', 'unknown')
                status = user_item.get('status', 'unknown')
                email = user.get('email')
                last_login = user.get('lastLoginAt', 'Never')

                report_lines.append(
                    f"- **{email}** ({role.capitalize()}) - Status: {status} - Last login: {last_login}"
                )

            report_lines.append("")

        except Exception as e:
            report_lines.extend([
                "## Authorized Users",
                f"Could not fetch user data: {str(e)}",
                ""
            ])

    report_lines.extend([
        "## Feature Flags",
        f"- **SQLi:** {'Enabled' if our_cluster.get('sqliEnabled') else 'Disabled'}",
        f"- **Incorta X:** {'Enabled' if our_cluster.get('incortaXEnabled') else 'Disabled'}",
        f"- **Data Agent:** {'Enabled' if our_cluster.get('enableDataAgent') else 'Disabled'}",
        f"- **Chat/OpenAI:** {'Enabled' if our_cluster.get('enableChat') else 'Disabled'}",
        f"- **MLflow:** {'Enabled' if our_cluster.get('mlflowEnabled') else 'Disabled'}",
        f"- **Delta Share:** {'Enabled' if our_cluster.get('enableDeltaShare') else 'Disabled'}",
        "",
        "## Spark Configuration",
        f"- **Min Executors:** {our_cluster.get('minExecutors', 'N/A')}",
        f"- **Max Executors:** {our_cluster.get('maxExecutors', 'N/A')}",
        "",
        "## Storage",
        f"- **Data Size:** {our_cluster.get('dsize')} GB",
        f"- **Loader Size:** {our_cluster.get('dsizeLoader')} GB",
        f"- **CMC Size:** {our_cluster.get('dsizeCmc')} GB",
        f"- **Available Disk:** {our_cluster.get('availableDisk')} GB",
        f"- **Consumed Data:** {our_cluster.get('consumedData')} GB",
        "",
        "## Upgrade History",
        f"- **Last Upgrade:** {our_cluster.get('initiatedUpgradeAt') or 'Never'}",
        f"- **Created:** {our_cluster.get('createdAt')}",
        f"- **Last Updated:** {our_cluster.get('updatedAt')}",
        f"- **Last Running:** {our_cluster.get('runningAt')}",
    ])

    return "\n".join(report_lines)


if __name__ == "__main__":
    app.run(transport="streamable-http")
