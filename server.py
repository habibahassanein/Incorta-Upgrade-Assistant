
import json
import os
import sys
from typing import Literal

import requests
from starlette.requests import Request
from starlette.responses import HTMLResponse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from mcp.server.fastmcp import FastMCP

from workflows.pre_upgrade_validation import run_validation
from workflows.upgrade_research import research_upgrade_path
from tools.qdrant_tool import search_knowledge_base
from tools.incorta_tools import query_zendesk, query_jira, get_zendesk_schema, get_jira_schema
from tools.extract_cluster_metadata import extract_cluster_metadata, format_metadata_report
from clients.cloud_portal_client import CloudPortalClient, infer_cloud_cluster_name
from workflows.checklist_workflow import run_collect_checklist_data, run_write_checklist_excel
from workflows.readiness_report import run_readiness_report


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
    try:
        cluster_data = client.get_cluster(cluster_name)
    except (RuntimeError, requests.exceptions.RequestException) as e:
        return f"Error: Failed to fetch cluster data from CMC: {e}"

    try:
        metadata = extract_cluster_metadata(cluster_data)
    except Exception as e:
        return f"Error: Failed to extract metadata: {e}"

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
def collect_checklist_data(
    cmc_cluster_name: str | None = None,
    cloud_cluster_name: str | None = None,
    from_version: str = "",
    to_version: str = "",
) -> str:
    """
    [CHECKLIST PHASE 1 - COLLECT] Collect data for the Pre-Upgrade Checklist.
    Returns a preview table of all detected values for user review BEFORE writing to Excel.

    HOW TO USE:
    1. Call this tool with cluster names and version info
    2. Present the returned preview table to the user
    3. Ask the user to review and confirm or modify values
    4. After approval, call write_checklist_excel with the JSON data

    The tool collects data from CMC API, Cloud Portal API, and knowledge base.
    Items that cannot be auto-detected show as "Not Implemented" or "N/A".

    Args:
        cmc_cluster_name: CMC cluster name (e.g., 'customCluster'). Defaults to CMC_CLUSTER_NAME env var.
        cloud_cluster_name: Cloud Portal cluster name (e.g., 'habibascluster'). Defaults to CLOUD_PORTAL_CLUSTER_NAME env var.
        from_version: Current Incorta version (e.g., '2024.1.0').
        to_version: Target Incorta version (e.g., '2024.7.0').
    """
    cmc_cluster_name = cmc_cluster_name or os.getenv("CMC_CLUSTER_NAME", "")
    cloud_cluster_name = cloud_cluster_name or os.getenv("CLOUD_PORTAL_CLUSTER_NAME", "")

    if not cmc_cluster_name:
        return "Error: No CMC cluster name provided. Set CMC_CLUSTER_NAME env var or pass cmc_cluster_name."
    if not from_version or not to_version:
        return "Error: Both from_version and to_version are required."

    return run_collect_checklist_data(
        cmc_cluster_name=cmc_cluster_name,
        cloud_cluster_name=cloud_cluster_name,
        from_version=from_version,
        to_version=to_version,
    )


_DEFAULT_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "pre_upgrade_checklist.xlsx")


@app.tool()
def write_checklist_excel(
    cell_values_json: str,
    template_path: str | None = None,
    output_path: str | None = None,
) -> str:
    """
    [CHECKLIST PHASE 2 - WRITE] Write approved checklist values into an Excel template.
    Call this AFTER collect_checklist_data and user approval.

    Takes the JSON data from collect_checklist_data (potentially modified by the user),
    copies the Excel template, fills the 'Pre-Upgrade Checklist' sheet, and saves.
    All other sheets in the workbook are left untouched.

    Args:
        cell_values_json: JSON string of cell values from collect_checklist_data (the <checklist_data> block).
        template_path: Path to the Pre-Upgrade Checklist Excel template file. Defaults to the bundled template.
        output_path: Path for the filled output file. Defaults to template path with '_filled' suffix.
    """
    if not template_path:
        template_path = _DEFAULT_TEMPLATE_PATH
    if not os.path.exists(template_path):
        return f"Error: Template file not found at '{template_path}'"

    if not output_path:
        base, ext = os.path.splitext(template_path)
        output_path = f"{base}_filled{ext}"

    try:
        return run_write_checklist_excel(
            cell_values_json=cell_values_json,
            template_path=template_path,
            output_path=output_path,
        )
    except Exception as e:
        return f"Error writing Excel: {str(e)}"


@app.tool()
def generate_upgrade_readiness_report(
    to_version: str,
    cmc_cluster_name: str | None = None,
    from_version: str = "",
    cloud_cluster_name: str | None = None,
) -> str:
    """
    [ONE-SHOT REPORT] Generate a comprehensive Upgrade Readiness Report.
    Orchestrates all data sources (CMC, Cloud Portal, knowledge base, upgrade research)
    to produce an opinionated readiness assessment with a rating and Excel checklist data.

    This is the recommended single-command way to assess upgrade readiness.
    It runs ALL other tools internally and produces a unified report.

    OUTPUT INCLUDES:
    - Overall Readiness Rating: READY / READY WITH CAVEATS / NOT READY
    - Environment summary (deployment type, DB, topology, versions)
    - Blockers that must be resolved before upgrade
    - Warnings to review
    - Validation checks summary (10 health checks)
    - Key upgrade considerations (DB migration, HA, version-specific notes)
    - Version research (release notes, known issues)
    - Data gaps (if any data sources failed)
    - Pre-Upgrade Checklist JSON (for write_checklist_excel)

    Args:
        to_version: Target Incorta version (e.g., '2024.7.0'). Required.
        cmc_cluster_name: CMC cluster name (e.g., 'customCluster'). Defaults to CMC_CLUSTER_NAME env var.
        from_version: Current Incorta version (e.g., '2024.1.0'). Auto-detected from cluster if empty.
        cloud_cluster_name: Cloud Portal cluster name (e.g., 'habibascluster'). Auto-inferred from CMC_URL if not provided.
    """
    # Resolve CMC cluster name
    if cmc_cluster_name is None:
        cmc_cluster_name = os.getenv("CMC_CLUSTER_NAME")
        if not cmc_cluster_name:
            return "Error: No cmc_cluster_name provided and CMC_CLUSTER_NAME env var not set."

    # Resolve Cloud Portal cluster name: explicit → env var → inferred from CMC_URL
    if cloud_cluster_name is None:
        cloud_cluster_name = os.getenv("CLOUD_PORTAL_CLUSTER_NAME") or infer_cloud_cluster_name()

    try:
        return run_readiness_report(
            cmc_cluster_name=cmc_cluster_name,
            to_version=to_version,
            cloud_cluster_name=cloud_cluster_name or "",
        )
    except Exception as e:
        return f"Error generating readiness report: {str(e)}"


# Module-level state for the non-blocking login flows.
# Tracks either a pending Authorization Code + PKCE login (local)
# or a Device Code flow (headless/deployed).
_login_state = {
    "active": False,
    "flow": None,             # "public", "browser", or "device"
    # Public + browser flow state
    "state": None,            # CSRF state token
    "event": None,           # threading.Event — set when callback received
    "auth_code_holder": None, # {"code": str|None, "error": str|None}
    "code_verifier": None,
    "redirect_uri": None,
    "authorize_url": None,
    "server": None,           # HTTPServer instance (browser flow only)
    "server_thread": None,    # Background daemon thread (browser flow only)
    # Device flow state
    "device_code": None,
    "verification_uri": None,
    "user_code": None,
    "device_expires_in": None,
    "device_interval": None,
    "device_started_at": None,
    # Shared
    "client": None,           # CloudPortalClient instance
}


def _cleanup_login_state():
    """Reset the module-level login state and stop any running server."""
    if _login_state.get("server"):
        try:
            _login_state["server"].server_close()
        except Exception:
            pass
    _login_state.update({
        "active": False,
        "flow": None,
        "state": None,
        "event": None,
        "auth_code_holder": None,
        "code_verifier": None,
        "redirect_uri": None,
        "authorize_url": None,
        "server": None,
        "server_thread": None,
        "device_code": None,
        "verification_uri": None,
        "user_code": None,
        "device_expires_in": None,
        "device_interval": None,
        "device_started_at": None,
        "client": None,
    })


@app.custom_route("/callback", methods=["GET"])
async def oauth_callback(request: Request) -> HTMLResponse:
    """OAuth Authorization Code callback handler.

    Receives the redirect from Auth0 after user login, validates state,
    exchanges the code for a token, and completes the login flow.
    Used when MCP_PUBLIC_URL is set (deployed/Cloudflare tunnel environment).
    """
    from clients.cloud_portal_client import CloudPortalClient as _CPC
    params = dict(request.query_params)
    returned_state = params.get("state")
    error = params.get("error")
    code = params.get("code")

    def _html(status: int, title: str, body: str) -> HTMLResponse:
        content = (
            f"<html><head><title>{title}</title>"
            "<style>body{{font-family:sans-serif;max-width:600px;margin:60px auto;text-align:center}}"
            "h2{{color:#333}}p{{color:#666}}</style></head>"
            f"<body><h2>{title}</h2><p>{body}</p></body></html>"
        )
        return HTMLResponse(content=content, status_code=status)

    if not _login_state["active"] or _login_state.get("flow") != "public":
        return _html(400, "Login Error", "No active login flow. Please start login from Claude again.")

    expected_state = _login_state.get("state")
    if returned_state != expected_state:
        _cleanup_login_state()
        return _html(400, "Login Failed", "State mismatch — possible CSRF attack. Please try again.")

    if error:
        error_desc = params.get("error_description", error)
        _login_state["auth_code_holder"] = {"code": None, "error": error_desc}
        if _login_state.get("event"):
            _login_state["event"].set()
        return _html(400, "Login Failed", f"{error_desc}")

    if not code:
        _login_state["auth_code_holder"] = {"code": None, "error": "No authorization code received"}
        if _login_state.get("event"):
            _login_state["event"].set()
        return _html(400, "Login Failed", "No authorization code received. Please try again.")

    # Exchange code for token immediately in the callback
    client = _login_state.get("client") or _CPC()
    code_verifier = _login_state["code_verifier"]
    redirect_uri = _login_state["redirect_uri"]
    try:
        client.exchange_code_for_token(code, redirect_uri, code_verifier)
        _login_state["auth_code_holder"] = {"code": code, "error": None}
        if _login_state.get("event"):
            _login_state["event"].set()
        return _html(200, "Login Successful",
                     "You are now logged in. You can close this tab and return to Claude.")
    except RuntimeError as e:
        _login_state["auth_code_holder"] = {"code": None, "error": str(e)}
        if _login_state.get("event"):
            _login_state["event"].set()
        return _html(500, "Login Failed", f"Token exchange failed: {e}")


@app.tool()
def cloud_portal_login() -> str:
    """
    [AUTHENTICATION] Log in to the Cloud Portal via browser-based OAuth.
    Returns a login URL for the user to visit. After authentication, the token
    is cached and subsequent Cloud Portal API calls will work automatically.

    WHEN TO USE: Call this when get_cloud_metadata returns an authentication error.
    The user must visit the returned URL in their browser to complete login.

    Supports three environments automatically:
    - Deployed with MCP_PUBLIC_URL set: redirect goes to {MCP_PUBLIC_URL}/callback
    - Local development: redirect goes to localhost:8910/callback
    - Headless fallback: Device Code Flow (no callback server needed)

    Call this tool again after completing login to confirm.
    """
    import time as _time
    cloud_client = CloudPortalClient()

    # 1. Check if already authenticated with a valid token
    if cloud_client.bearer_token and not cloud_client._is_token_expired(cloud_client.bearer_token):
        _cleanup_login_state()
        try:
            user_id = cloud_client.get_user_id()
        except RuntimeError:
            user_id = "unknown"
        return (
            "## Already Authenticated\n\n"
            f"- **User ID:** `{user_id}`\n"
            "- Token is still valid.\n"
            "\nYou can use `get_cloud_metadata` directly."
        )

    # 2. Try silent refresh (no browser needed)
    if cloud_client._refresh_access_token():
        _cleanup_login_state()
        try:
            user_id = cloud_client.get_user_id()
        except RuntimeError:
            user_id = "unknown"
        return (
            "## Token Refreshed\n\n"
            f"- **User ID:** `{user_id}`\n"
            "- Token refreshed successfully (no browser login needed).\n"
            "\nYou can use `get_cloud_metadata` directly."
        )

    # 3. If a login flow is active, check for completion
    if _login_state["active"]:
        flow = _login_state.get("flow")

        if flow == "device":
            # Poll device code flow once
            device_code = _login_state["device_code"]
            client = _login_state["client"] or cloud_client
            started_at = _login_state["device_started_at"] or 0
            expires_in = _login_state["device_expires_in"] or 900

            if _time.time() > started_at + expires_in:
                _cleanup_login_state()
                return (
                    "## Login Expired\n\n"
                    "The device code has expired. Call this tool again to start a new login."
                )

            try:
                from clients.cloud_portal_client import AUTH0_DOMAIN, AUTH0_CLIENT_ID
                response = requests.post(
                    f"https://{AUTH0_DOMAIN}/oauth/token",
                    json={
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                        "device_code": device_code,
                        "client_id": AUTH0_CLIENT_ID,
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=15,
                )

                if response.status_code == 200:
                    data = response.json()
                    access_token = data.get("access_token")
                    refresh_token = data.get("refresh_token")
                    if access_token:
                        client.bearer_token = access_token
                        client.refresh_token = refresh_token
                        client._save_token(access_token, refresh_token)
                        _cleanup_login_state()
                        try:
                            user_id = client.get_user_id()
                        except RuntimeError:
                            user_id = "unknown"
                        return (
                            f"## Cloud Portal Login Successful\n\n"
                            f"- **User ID:** `{user_id}`\n"
                            f"- Token cached and will auto-refresh.\n"
                            f"\nYou can now use `get_cloud_metadata` to query your clusters."
                        )

                error = response.json().get("error", "")
                if error == "authorization_pending":
                    return (
                        f"## Login In Progress\n\n"
                        f"Still waiting for you to complete login.\n\n"
                        f"1. Visit: **{_login_state['verification_uri']}**\n"
                        f"2. Enter code: **`{_login_state['user_code']}`**\n\n"
                        f"After approving, call this tool again to confirm."
                    )
                elif error == "slow_down":
                    return (
                        f"## Login In Progress\n\n"
                        f"Please wait a moment before calling again.\n\n"
                        f"1. Visit: **{_login_state['verification_uri']}**\n"
                        f"2. Enter code: **`{_login_state['user_code']}`**"
                    )
                elif error in ("expired_token", "access_denied"):
                    _cleanup_login_state()
                    return f"## Login Failed\n\n{error.replace('_', ' ').capitalize()}. Call this tool again to restart."
                else:
                    _cleanup_login_state()
                    return f"## Login Failed\n\nUnexpected error: {error}. Call this tool again to restart."

            except Exception as e:
                return f"## Login Check Failed\n\n{str(e)}\n\nCall this tool again to retry."

        elif flow in ("public", "browser"):
            # Check if the callback route (public) or local server (browser) completed
            event = _login_state.get("event")
            holder = _login_state.get("auth_code_holder") or {}

            if event and event.is_set():
                if holder.get("error"):
                    err = holder["error"]
                    _cleanup_login_state()
                    return f"## Login Failed\n\n{err}"

                # For "public" flow, token was already exchanged in the /callback route
                if flow == "public":
                    _cleanup_login_state()
                    try:
                        # Reload client to pick up the saved token
                        fresh_client = CloudPortalClient()
                        user_id = fresh_client.get_user_id()
                    except RuntimeError:
                        user_id = "unknown"
                    return (
                        f"## Cloud Portal Login Successful\n\n"
                        f"- **User ID:** `{user_id}`\n"
                        f"- Token cached and will auto-refresh.\n"
                        f"\nYou can now use `get_cloud_metadata` to query your clusters."
                    )

                # "browser" flow: exchange code now
                auth_code = holder.get("code")
                if not auth_code:
                    _cleanup_login_state()
                    return "## Login Failed\n\nNo authorization code received."

                client = _login_state.get("client") or cloud_client
                code_verifier = _login_state["code_verifier"]
                redirect_uri = _login_state["redirect_uri"]
                _cleanup_login_state()

                try:
                    client.exchange_code_for_token(auth_code, redirect_uri, code_verifier)
                    user_id = client.get_user_id()
                    return (
                        f"## Cloud Portal Login Successful\n\n"
                        f"- **User ID:** `{user_id}`\n"
                        f"- Token cached and will auto-refresh.\n"
                        f"\nYou can now use `get_cloud_metadata` to query your clusters."
                    )
                except RuntimeError as e:
                    return f"## Login Failed\n\nToken exchange error: {str(e)}"
            else:
                # Still waiting
                authorize_url = _login_state.get("authorize_url", "")
                return (
                    f"## Login In Progress\n\n"
                    f"Still waiting for you to complete login.\n\n"
                    f"**Open this URL if you haven't already:**\n"
                    f"{authorize_url}\n\n"
                    f"After completing login in the browser, call this tool again."
                )

    # 4. No active flow — determine which flow to use and start it
    public_url = os.getenv("MCP_PUBLIC_URL", "").rstrip("/")

    if public_url:
        # Deployed with Cloudflare tunnel: use public callback URL
        redirect_uri = f"{public_url}/callback"
        login_info = cloud_client.build_authorize_url(redirect_uri)
        event = __import__("threading").Event()

        _login_state.update({
            "active": True,
            "flow": "public",
            "state": login_info["state"],
            "code_verifier": login_info["code_verifier"],
            "redirect_uri": redirect_uri,
            "authorize_url": login_info["authorize_url"],
            "auth_code_holder": {"code": None, "error": None},
            "event": event,
            "client": cloud_client,
        })

        return (
            f"## Cloud Portal Login\n\n"
            f"**Open this URL in your browser to log in:**\n"
            f"{login_info['authorize_url']}\n\n"
            f"After completing login, **call this tool again** to confirm."
        )

    if not cloud_client._is_headless():
        # Local development: use localhost callback server
        try:
            login_info = cloud_client.login_for_mcp()
        except RuntimeError as e:
            return f"## Login Error\n\n{str(e)}"

        _login_state.update({
            "active": True,
            "flow": "browser",
            "event": login_info["event"],
            "auth_code_holder": login_info["auth_code_holder"],
            "code_verifier": login_info["code_verifier"],
            "redirect_uri": login_info["redirect_uri"],
            "authorize_url": login_info["authorize_url"],
            "server": login_info["server"],
            "server_thread": login_info["server_thread"],
            "client": cloud_client,
        })

        return (
            f"## Cloud Portal Login\n\n"
            f"**Open this URL in your browser:**\n"
            f"{login_info['authorize_url']}\n\n"
            f"Complete login (including MFA if required), then **call this tool again** to confirm.\n"
        )

    # Headless fallback: Device Code Flow
    try:
        device_info = cloud_client.device_login()
    except RuntimeError as e:
        return f"## Login Error\n\n{str(e)}"

    _login_state.update({
        "active": True,
        "flow": "device",
        "device_code": device_info["device_code"],
        "verification_uri": device_info["verification_uri"],
        "user_code": device_info["user_code"],
        "device_expires_in": device_info["expires_in"],
        "device_interval": device_info["interval"],
        "device_started_at": _time.time(),
        "client": cloud_client,
    })

    return (
        f"## Cloud Portal Login (Device Code)\n\n"
        f"**Step 1:** Open this URL in your browser:\n"
        f"{device_info['verification_uri']}\n\n"
        f"**Step 2:** The code is pre-filled in the URL above. Just approve the login.\n\n"
        f"After approving, **call this tool again** to confirm login."
    )


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

    NOTE: Cloud Portal and CMC use different cluster names for the same cluster.
    - CMC name (e.g., 'customCluster') is used by extract_cluster_metadata_tool
    - Cloud Portal name (e.g., 'habibascluster') is used by this tool
    The AI agent should use both tools together and correlate results by context.

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
        cluster_name = os.getenv("CLOUD_PORTAL_CLUSTER_NAME") or infer_cloud_cluster_name()
        if not cluster_name:
            return "Error: No cluster_name provided. Set CLOUD_PORTAL_CLUSTER_NAME env var or ensure CMC_URL is configured."
    cloud_client = CloudPortalClient()

    try:
        user_id = cloud_client.get_user_id()
    except RuntimeError as e:
        error_msg = str(e)
        if "AUTHENTICATION_REQUIRED" in error_msg:
            return (
                "Error: Not authenticated with Cloud Portal.\n"
                "Please call the **cloud_portal_login** tool first to log in.\n"
                "After logging in, retry this tool."
            )
        return f"Error: {error_msg}"

    try:
        our_cluster = cloud_client.find_cluster(user_id, cluster_name)
    except (requests.exceptions.HTTPError, RuntimeError) as e:
        return f"Error: Failed to fetch clusters from Cloud Portal: {str(e)}"

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
            consumption_agg = consumption.get('consumptionAgg', {})
            total_pu = consumption_agg.get('totalAgg', 0)
            daily_data = consumption_agg.get('total', {}).get('daily', [])

            report_lines.append("## Consumption & Cost")

            if daily_data:
                avg_pu = sum(d.get('powerUnit', 0) for d in daily_data) / len(daily_data)
                recent_7 = daily_data[-7:] if len(daily_data) >= 7 else daily_data

                report_lines.extend([
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
            elif total_pu:
                report_lines.extend([
                    f"- **Total (This Month):** {total_pu:.6f} Power Units",
                    "- *Daily breakdown not available*",
                ])
            else:
                report_lines.append("- No consumption data available for this period.")

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
    host = os.getenv("MCP_HOST", "127.0.0.1")
    port = int(os.getenv("MCP_PORT", "8000"))
    app.settings.host = host
    app.settings.port = port
    app.run(transport="streamable-http")
