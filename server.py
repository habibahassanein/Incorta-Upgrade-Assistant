
import json
import os
import sys
import time
from typing import Literal

import requests
from starlette.requests import Request
from starlette.responses import HTMLResponse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from mcp.server.fastmcp import FastMCP

from workflows.pre_upgrade_validation import run_validation
from tools.qdrant_tool import search_knowledge_base
from tools.incorta_tools import query_zendesk, query_jira, get_zendesk_schema, get_jira_schema
from tools.extract_cluster_metadata import extract_cluster_metadata, format_metadata_report
from clients.cloud_portal_client import CloudPortalClient
from workflows.checklist_workflow import run_write_checklist_excel
from workflows.readiness_report import run_readiness_report


app = FastMCP("incorta-upgrade-agent", stateless_http=True)


# ==========================================
# HELPERS
# ==========================================


def _get_cmc_cluster_name(explicit: str | None = None) -> str | None:
    """Resolve CMC cluster name from: explicit param → cached config → env var.

    Returns the cluster name string, or None if not found anywhere.
    """
    if explicit:
        return explicit

    # Check cached config from portal login
    from clients.cmc_client import CMCClient
    cached = CMCClient.load_cached_config()
    if cached.get("cluster_name"):
        return cached["cluster_name"]

    # Fall back to environment variable
    return os.getenv("CMC_CLUSTER_NAME") or None


# ==========================================
# CUSTOMER UPGRADE RECOMMENDATION WORKFLOW
# ==========================================


@app.tool()
def generate_upgrade_readiness_report(
    to_version: str,
    customer_name: str,
    cmc_cluster_name: str | None = None,
    from_version: str = "",
    cloud_cluster_name: str | None = None,
) -> str:
    """
    [CORE - RUN FIRST] Generate a comprehensive Upgrade Readiness Report.
    Orchestrates all data sources (CMC, Cloud Portal, knowledge base, upgrade research,
    Zendesk customer support tickets, and Jira bug tracking) to produce an opinionated
    readiness assessment with a rating and Excel checklist data.

    This is the recommended single-command way to assess upgrade readiness.
    It runs ALL other tools internally and produces a unified report.

    PREREQUISITES:
    - Ask the user for the Cloud Portal cluster name (e.g., 'habibascluster') and
      make sure the cluster is running before calling this tool.
    - For Cloud Portal data, call `cloud_portal_login` first. Without it, these
      fields will appear as N/A in the report.

    AUTOMATIC ZENDESK ANALYSIS: The report automatically queries Zendesk for:
    - Known issues for this specific upgrade path (tag-based filtering)
    - Risk assessment (critical issues, resolution times)
    - Environment-specific issues (cloud vs on-prem)
    - Customer satisfaction metrics
    No manual SQL or Zendesk tool calls needed — this runs automatically.

    AUTOMATIC JIRA BUG ANALYSIS: The report automatically queries Jira for:
    - Customer-reported bugs and their fix version status
    - Bugs linked from Zendesk tickets
    - Bugs from other customers affecting versions in the upgrade path
    - Classification: fixed in target / still open / requires later release
    Requires customer_name to match bugs in Jira.

    OUTPUT INCLUDES:
    - Overall Readiness Rating: READY / READY WITH CAVEATS / NOT READY
    - Environment summary (deployment type, DB, topology, versions)
    - Known upgrade issues from customer support data
    - Customer bug fix status from Jira
    - Blockers that must be resolved before upgrade
    - Warnings to review
    - Validation checks summary (10 health checks)
    - Key upgrade considerations (DB migration, HA, version-specific notes)
    - Version research (release notes, known issues)
    - Data gaps (if any data sources failed)
    - Pre-Upgrade Checklist JSON (for write_checklist_excel)

    NEXT STEP: After getting this report, pass the <checklist_data> JSON block
    at the bottom to `write_checklist_excel` to download the filled Excel checklist.

    Args:
        to_version: Target Incorta version (e.g., '2024.7.0'). Required.
        customer_name: Customer name exactly as it appears in Jira's Customer field (e.g., 'Acme Corp'). Required for Jira bug analysis.
        cmc_cluster_name: CMC cluster name (e.g., 'customCluster'). Defaults to CMC_CLUSTER_NAME env var.
        from_version: Current Incorta version (e.g., '2024.1.0'). Auto-detected from cluster if empty.
        cloud_cluster_name: Cloud Portal cluster name (e.g., 'habibascluster'). REQUIRED — ask the user.
    """
    # Resolve CMC cluster name (explicit → cached → env)
    cmc_cluster_name = _get_cmc_cluster_name(cmc_cluster_name)
    if not cmc_cluster_name:
        return (
            "Error: CMC cluster name not available.\n"
            "Please call the **cmc_login** tool first to authenticate via the login portal, "
            "or provide the cmc_cluster_name parameter."
        )

    # Cloud Portal cluster name is required — do not infer
    if not cloud_cluster_name:
        return (
            "Error: cloud_cluster_name is required.\n"
            "Please ask the user for the Cloud Portal cluster name "
            "(e.g., 'habibascluster') and make sure the cluster is running."
        )

    try:
        return run_readiness_report(
            cmc_cluster_name=cmc_cluster_name,
            to_version=to_version,
            customer_name=customer_name,
            cloud_cluster_name=cloud_cluster_name,
        )
    except Exception as e:
        return f"Error generating readiness report: {str(e)}"

@app.tool()
def run_pre_upgrade_validation(cluster_name: str | None = None) -> str:
    """
    [HEALTH CHECK] Validates Incorta cluster health before upgrade.
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
    cluster_name = _get_cmc_cluster_name(cluster_name)
    if not cluster_name:
        return (
            "Error: CMC cluster name not available.\n"
            "Please call the **cmc_login** tool first to authenticate via the login portal, "
            "or provide the cluster_name parameter."
        )
    try:
        return run_validation(cluster_name)
    except Exception as e:
        return f"Error running pre-upgrade validation: {str(e)}"



_DEFAULT_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "pre_upgrade_checklist.xlsx")


@app.tool()
def write_checklist_excel(
    cell_values_json: str,
    filename: str = "pre_upgrade_checklist_filled.xlsx",
) -> str:
    """
    [EXCEL OUTPUT] Write approved checklist values into an Excel template.
    Call this with the `<checklist_data>` JSON block from `generate_upgrade_readiness_report`.

    Takes the JSON data embedded in the readiness report output (potentially modified by the user),
    fills the bundled 'Pre-Upgrade Checklist' template, and returns the result as a
    base64-encoded Excel file. Claude Desktop will offer it as a download — no file
    paths or VM access needed.

    All other sheets in the workbook are left untouched.

    Args:
        cell_values_json: JSON string of cell values from generate_upgrade_readiness_report (the <checklist_data> block).
        filename: Suggested filename for the downloaded file. Defaults to 'pre_upgrade_checklist_filled.xlsx'.
    """
    template_path = _DEFAULT_TEMPLATE_PATH

    if not os.path.exists(template_path):
        return json.dumps({
            "error": f"Bundled template not found at '{template_path}'. "
                     "Ensure pre_upgrade_checklist.xlsx is in the templates/ directory."
        })

    try:
        result = run_write_checklist_excel(
            cell_values_json=cell_values_json,
            template_path=template_path,
            filename=filename,
        )
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"error": f"Error writing Excel: {str(e)}"})



# Module-level state for the non-blocking login flows.
# Tracks a pending Authorization Code + PKCE login (public Cloudflare or local browser).
_login_state = {
    "active": False,
    "flow": None,             # "public" or "browser"
    # Shared PKCE state
    "state": None,            # CSRF state token
    "event": None,           # threading.Event — set when callback received
    "auth_code_holder": None, # {"code": str|None, "error": str|None}
    "code_verifier": None,
    "redirect_uri": None,
    "authorize_url": None,
    "server": None,           # HTTPServer instance (browser flow only)
    "server_thread": None,    # Background daemon thread (browser flow only)
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
        "client": None,
    })


@app.custom_route("/callback", methods=["GET"])
async def oauth_callback(request: Request) -> HTMLResponse:
    """OAuth Authorization Code callback handler.

    Receives the redirect from Auth0 after user login, validates state,
    exchanges the code for a token, and completes the login flow.
    Used when MCP_PUBLIC_URL is set in headless mode with a persistent, pre-registered domain.
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


# ==========================================
# CMC LOGIN PORTAL (browser-based form)
# ==========================================

# Module-level flag: set to True after successful CMC portal login
_cmc_login_success = False

_INCORTA_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Mulish:wght@400;600;700&display=swap');
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Mulish', sans-serif; background: #F5F6FA; min-height: 100vh;
       display: flex; align-items: center; justify-content: center; }
.card { background: #fff; border-radius: 12px; box-shadow: 0 4px 24px rgba(0,0,0,0.10);
        padding: 40px 36px; max-width: 440px; width: 100%; }
.logo { text-align: center; margin-bottom: 24px; }
.logo svg { height: 36px; }
h2 { color: #1B2A4A; font-size: 22px; font-weight: 700; text-align: center; margin-bottom: 8px; }
.subtitle { color: #6B7280; font-size: 14px; text-align: center; margin-bottom: 28px; }
label { display: block; color: #1B2A4A; font-weight: 600; font-size: 14px; margin-bottom: 6px; }
input[type=text], input[type=password] {
    width: 100%; padding: 10px 14px; border: 1.5px solid #D1D5DB; border-radius: 8px;
    font-family: 'Mulish', sans-serif; font-size: 15px; margin-bottom: 18px;
    transition: border-color 0.2s; outline: none;
}
input[type=text]:focus, input[type=password]:focus { border-color: #F26522; box-shadow: 0 0 0 3px rgba(242,101,34,0.12); }
.btn { display: block; width: 100%; padding: 12px; background: #F26522; color: #fff; border: none;
       border-radius: 8px; font-family: 'Mulish', sans-serif; font-size: 16px; font-weight: 700;
       cursor: pointer; transition: background 0.2s; }
.btn:hover { background: #d9551a; }
.error-box { background: #FEF2F2; border: 1px solid #FECACA; border-radius: 8px;
             padding: 12px 16px; color: #991B1B; font-size: 14px; margin-bottom: 18px; }
.success-box { background: #F0FDF4; border: 1px solid #BBF7D0; border-radius: 8px;
               padding: 16px; color: #166534; font-size: 14px; text-align: center; margin-top: 20px; }
.hint { color: #9CA3AF; font-size: 12px; margin-top: -14px; margin-bottom: 18px; }
"""

_INCORTA_LOGO_SVG = """<svg width="120" height="36" viewBox="0 0 120 36" xmlns="http://www.w3.org/2000/svg">
  <text x="0" y="28" font-family="Mulish, sans-serif" font-size="28" font-weight="700" fill="#1B2A4A">inc</text>
  <text x="42" y="28" font-family="Mulish, sans-serif" font-size="28" font-weight="700" fill="#F26522">o</text>
  <text x="60" y="28" font-family="Mulish, sans-serif" font-size="28" font-weight="700" fill="#1B2A4A">rta</text>
</svg>"""


def _cmc_html_page(title: str, body_html: str, status: int = 200) -> HTMLResponse:
    """Return an Incorta-branded HTML page."""
    html = (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'>"
        f"<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{title}</title><style>{_INCORTA_CSS}</style></head>"
        f"<body><div class='card'>"
        f"<div class='logo'>{_INCORTA_LOGO_SVG}</div>"
        f"{body_html}"
        f"</div></body></html>"
    )
    return HTMLResponse(content=html, status_code=status)


@app.custom_route("/cmc-login", methods=["GET"])
async def cmc_login_form(request: Request) -> HTMLResponse:
    """Serve the CMC login form."""
    form_html = (
        "<h2>CMC Login</h2>"
        "<p class='subtitle'>Enter your CMC credentials to authenticate.</p>"
        "<form method='POST' action='/cmc-login'>"
        "<label for='cmc_url'>CMC URL</label>"
        "<input type='text' id='cmc_url' name='cmc_url' "
        "placeholder='https://yourcluster.cloudstaging.incortalabs.com/cmc' required />"
        "<p class='hint'>Format: https://&lt;cluster&gt;.cloudstaging.incortalabs.com/cmc</p>"
        "<label for='username'>Username</label>"
        "<input type='text' id='username' name='username' placeholder='admin' required />"
        "<label for='password'>Password</label>"
        "<input type='password' id='password' name='password' required />"
        "<label for='cluster_name'>CMC Cluster Name</label>"
        "<input type='text' id='cluster_name' name='cluster_name' placeholder='customCluster' required />"
        "<p class='hint'>The cluster name as shown in CMC (e.g., customCluster)</p>"
        "<button type='submit' class='btn'>Login</button>"
        "</form>"
    )
    return _cmc_html_page("CMC Login", form_html)


@app.custom_route("/cmc-login", methods=["POST"])
async def cmc_login_submit(request: Request) -> HTMLResponse:
    """Process the CMC login form submission."""
    global _cmc_login_success

    form = await request.form()
    cmc_url = (form.get("cmc_url") or "").strip().rstrip("/")
    username = (form.get("username") or "").strip()
    password = (form.get("password") or "").strip()
    cluster_name = (form.get("cluster_name") or "").strip()

    # Validate all fields are provided
    missing = []
    if not cmc_url:
        missing.append("CMC URL")
    if not username:
        missing.append("Username")
    if not password:
        missing.append("Password")
    if not cluster_name:
        missing.append("CMC Cluster Name")

    if missing:
        error_html = (
            "<h2>CMC Login</h2>"
            f"<div class='error-box'>Missing required fields: {', '.join(missing)}</div>"
            "<p style='text-align:center; margin-top:16px;'>"
            "<a href='/cmc-login' style='color:#F26522; font-weight:600;'>Try Again</a></p>"
        )
        return _cmc_html_page("CMC Login - Error", error_html, status=400)

    # Try authenticating against CMC
    from clients.cmc_client import CMCClient

    try:
        client = CMCClient(url=cmc_url, user=username, password=password, cluster_name=cluster_name)
        client.login()  # This caches the token + url + cluster_name to disk
    except RuntimeError as e:
        error_msg = str(e)
        # Truncate long error messages for display
        if len(error_msg) > 300:
            error_msg = error_msg[:300] + "..."
        error_html = (
            "<h2>CMC Login</h2>"
            f"<div class='error-box'>{error_msg}</div>"
            "<p style='text-align:center; margin-top:16px;'>"
            "<a href='/cmc-login' style='color:#F26522; font-weight:600;'>Try Again</a></p>"
        )
        return _cmc_html_page("CMC Login - Failed", error_html, status=401)

    # Success!
    _cmc_login_success = True

    # Decode JWT for display
    expiry_info = ""
    try:
        import jwt as pyjwt
        claims = pyjwt.decode(client.token, options={"verify_signature": False})
        exp = claims.get("exp")
        if exp:
            remaining = (exp - time.time()) / 3600
            expiry_info = f"<br>Token expires in <strong>{remaining:.1f} hours</strong>."
    except Exception:
        pass

    success_html = (
        "<h2>Login Successful</h2>"
        "<div class='success-box'>"
        f"<strong>Authenticated as {username}</strong><br>"
        f"CMC URL: <code>{cmc_url}</code><br>"
        f"Cluster: <code>{cluster_name}</code>"
        f"{expiry_info}"
        "</div>"
        "<p style='text-align:center; margin-top:20px; color:#6B7280; font-size:14px;'>"
        "You can close this tab and return to Claude.</p>"
    )
    return _cmc_html_page("CMC Login - Success", success_html)


@app.tool()
def cloud_portal_login() -> str:
    """
    [CLOUD AUTH] Log in to the Cloud Portal via browser-based OAuth.

    TWO-STEP PROCESS:
      Step 1: Call this tool — you receive a login URL. Open it in your browser and complete login.
      Step 2: Call this tool again — it confirms login and caches the token for future calls.

    After authentication, the token is cached and subsequent Cloud Portal API calls
    will work automatically (including inside generate_upgrade_readiness_report).

    WHEN TO USE: Call this BEFORE generate_upgrade_readiness_report for cloud deployments.
    Without it, Spark/Python versions, disk sizes, and Data Agent status will be N/A.
    Also call this when get_cloud_metadata returns an authentication error.

    Supports two environments:
    - Local/browser (recommended): redirect always goes to localhost:8910/callback,
      regardless of MCP_PUBLIC_URL. Works whenever a browser is available.
    - Headless with persistent URL: set MCP_PUBLIC_URL to a domain registered in Auth0.
      Ephemeral Cloudflare tunnel URLs (*.trycloudflare.com) are rejected automatically.

    NOTE: Headless environments (Docker, no browser) cannot authenticate directly.
    Authenticate locally first — the token is cached and auto-refreshes across restarts.
    """
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

        if flow in ("public", "browser"):
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

    if public_url and ".trycloudflare.com" in public_url:
        import logging
        logging.getLogger(__name__).warning(
            "MCP_PUBLIC_URL is a trycloudflare.com URL which changes every session "
            "and cannot be registered in Auth0. OAuth will use localhost instead."
        )

    if not cloud_client._is_headless():
        # PREFERRED: Browser available — always use localhost callback.
        # OAuth redirects happen in the user's browser, which can reach localhost:8910
        # regardless of whether the MCP server is exposed via a tunnel.
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

    if public_url and ".trycloudflare.com" not in public_url:
        # FALLBACK: Headless with a persistent, pre-registered public URL.
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

    # No browser available and no valid public URL — cannot authenticate.
    return (
        "## Login Not Available in Headless Mode\n\n"
        "Cloud Portal authentication requires a browser (OAuth/PKCE) and cannot run "
        "in a headless or Docker environment without a public URL.\n\n"
        "**To authenticate:**\n"
        "1. Run the MCP server locally (without `HEADLESS=true` and outside Docker)\n"
        "2. Call `cloud_portal_login` — a browser window opens automatically at `localhost:8910`\n"
        "3. Complete login once — the token is cached and auto-refreshes across restarts\n\n"
        "**Alternatively:** Set `MCP_PUBLIC_URL` to a *persistent* public URL that is "
        "registered as a callback in Auth0 (quick-tunnel URLs like Cloudflare change "
        "each session and are not registered)."
    )


@app.tool()
def cmc_login() -> str:
    """
    [CMC AUTH] Authenticate with CMC via a browser-based login portal.

    TWO-STEP PROCESS:
      Step 1: Call this tool — you receive a URL for the login portal.
              Ask the user to open it in their browser, fill in the form, and submit.
      Step 2: Call this tool again — it detects the cached token and confirms authentication.

    The login portal collects: CMC URL, username, password, and cluster name.
    After successful login, the JWT and config are cached at ~/.incorta_cmc_token.json.
    Subsequent CMC tool calls (extract_cluster_metadata, run_pre_upgrade_validation, etc.)
    will use the cached token automatically — no .env variables needed.

    The cached token auto-expires (checked on each use). When it expires,
    call this tool again to re-open the login portal.
    """
    global _cmc_login_success
    from clients.cmc_client import CMCClient

    # Check if already authenticated (cached token on disk)
    cached = CMCClient.load_cached_config()
    if cached.get("access_token"):
        user = cached.get("user", "unknown")
        url = cached.get("url", "unknown")
        cluster = cached.get("cluster_name", "unknown")

        # Decode JWT for expiry info
        expiry_info = ""
        try:
            import jwt as pyjwt
            claims = pyjwt.decode(cached["access_token"], options={"verify_signature": False})
            exp = claims.get("exp")
            if exp:
                remaining = (exp - time.time()) / 3600
                expiry_info = f"- **Expires in:** {remaining:.1f} hours\n"
        except Exception:
            pass

        _cmc_login_success = False  # Reset for next round
        return (
            f"## CMC Authenticated\n\n"
            f"- **User:** `{user}`\n"
            f"- **CMC URL:** `{url}`\n"
            f"- **Cluster Name:** `{cluster}`\n"
            f"- **Token cached at:** `~/.incorta_cmc_token.json`\n"
            f"{expiry_info}\n"
            f"CMC tools are ready to use."
        )

    # Check if the portal form was just submitted successfully
    if _cmc_login_success:
        _cmc_login_success = False
        # Re-check cache (should be populated now)
        cached = CMCClient.load_cached_config()
        if cached.get("access_token"):
            user = cached.get("user", "unknown")
            url = cached.get("url", "unknown")
            cluster = cached.get("cluster_name", "unknown")
            return (
                f"## CMC Login Successful\n\n"
                f"- **User:** `{user}`\n"
                f"- **CMC URL:** `{url}`\n"
                f"- **Cluster Name:** `{cluster}`\n"
                f"- **Token cached at:** `~/.incorta_cmc_token.json`\n\n"
                f"CMC tools are now ready to use."
            )

    # No cached token — direct user to the login portal
    port = int(os.getenv("MCP_PORT", "8000"))
    host = os.getenv("MCP_HOST", "127.0.0.1")
    portal_url = f"http://{host}:{port}/cmc-login"

    return (
        f"## CMC Login Required\n\n"
        f"**Open this URL in your browser to log in:**\n"
        f"{portal_url}\n\n"
        f"Fill in the form with your CMC URL, username, password, and cluster name, "
        f"then click **Login**.\n\n"
        f"After successful login, **call this tool again** to confirm authentication."
    )


@app.tool()
def get_cloud_metadata(
    cluster_name: str | None = None,
    include_consumption: bool = True,
    include_users: bool = True
) -> str:
    """
    [CLOUD DATA] Get cloud metadata from Cloud Portal API.
    Provides data NOT available in CMC API. Call AFTER extract_cluster_metadata.

    IMPORTANT: You MUST ask the user for the cluster name. Do NOT infer or guess it.
    The cluster name is the Cloud Portal instance name (e.g., 'habibascluster').

    NOTE: Cloud Portal and CMC use different cluster names for the same cluster.
    - CMC name (e.g., 'customCluster') is used by extract_cluster_metadata_tool
    - Cloud Portal name (e.g., 'habibascluster') is used by this tool

    DATA AVAILABLE:
    - Instance details, build info, platform, service statuses
    - Software versions (Spark, Python, MySQL)
    - Sizing (analytics, loader, CMC memory/CPU/IPU)
    - Feature flags, feature bits
    - Consumption & cost data
    - Authorized users
    - Upgrade history

    Args:
        cluster_name: Cloud Portal cluster name. REQUIRED — ask the user for this value.
        include_consumption: Include consumption/cost data (default: True)
        include_users: Include authorized users (default: True)
    """
    if not cluster_name:
        return (
            "Error: cluster_name is required.\n"
            "Please ask the user for the Cloud Portal cluster name "
            "(e.g., 'habibascluster') and make sure the cluster is running."
        )

    cloud_client = CloudPortalClient()

    # Use the cp- search endpoint (no user_id needed, richer response)
    try:
        our_cluster = cloud_client.search_instances(cluster_name)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code in (401, 403):
            return (
                "Error: Not authenticated with Cloud Portal.\n"
                "Please call the **cloud_portal_login** tool first to log in.\n"
                "After logging in, retry this tool."
            )
        return f"Error: Failed to search instances from Cloud Portal: {str(e)}"
    except RuntimeError as e:
        error_msg = str(e)
        if "AUTHENTICATION_REQUIRED" in error_msg:
            return (
                "Error: Not authenticated with Cloud Portal.\n"
                "Please call the **cloud_portal_login** tool first to log in.\n"
                "After logging in, retry this tool."
            )
        return f"Error: {error_msg}"

    if not our_cluster:
        return f"Error: Cluster '{cluster_name}' not found in Cloud Portal."

    instance_uuid = our_cluster.get("id")
    cluster_status = our_cluster.get("status", "unknown")

    # --- Build report ---
    report_lines = [
        f"# Cloud Portal Metadata: {cluster_name}",
        "",
    ]

    # Warning if cluster is not running
    if cluster_status != "running":
        report_lines.extend([
            f"> **WARNING:** Cluster status is **{cluster_status}** (not running).",
            "> Some data may be stale. Please ensure the cluster is running for accurate results.",
            "",
        ])

    report_lines.extend([
        "## Instance Details",
        f"- **UUID:** `{instance_uuid}`",
        f"- **Build:** {our_cluster.get('customBuild') or 'Vanilla'} ({our_cluster.get('customBuildName') or our_cluster.get('image')})",
        f"- **Image:** {our_cluster.get('image')}",
        f"- **Platform:** {our_cluster.get('platform')} - {our_cluster.get('region')}/{our_cluster.get('zone')}",
        f"- **K8s Cluster:** {our_cluster.get('k8sClusterCode')}",
        f"- **Status:** {cluster_status}",
        f"- **Premium:** {'Yes' if our_cluster.get('isPremium') else 'No'}",
        f"- **Organization:** {our_cluster.get('organization')}",
        "",
    ])

    # Service statuses from instanceServices
    services = our_cluster.get("instanceServices", [])
    if services:
        svc = services[0]
        report_lines.extend([
            "## Service Status",
            f"- **CMC:** {svc.get('cmc_status', 'N/A')}",
            f"- **Analytics:** {svc.get('analytics_status', 'N/A')}",
            f"- **Loader:** {svc.get('loader_status', 'N/A')}",
            f"- **Spark:** {svc.get('spark_status', 'N/A')}",
            f"- **Zookeeper:** {svc.get('zookeeper_status', 'N/A')}",
            "",
        ])

    report_lines.extend([
        "## Software Versions",
        f"- **Spark:** {our_cluster.get('incortaSparkVersion')}",
        f"- **Python:** {our_cluster.get('pythonVersion') or 'N/A'}",
        f"- **MySQL:** {our_cluster.get('mysqlVersion') or 'N/A'}",
        "",
    ])

    # Sizing details from nested objects
    def _format_size(size_obj, label):
        if not size_obj or not isinstance(size_obj, dict):
            return [f"- **{label}:** N/A"]
        return [
            f"- **{label}:** {size_obj.get('displayName', 'N/A')} "
            f"(Memory: {size_obj.get('memoryRequest', '?')}-{size_obj.get('memoryLimit', '?')} GB, "
            f"CPU: {size_obj.get('cpu', 'N/A')} vCPU, "
            f"IPU: {size_obj.get('ipu', 'N/A')})",
        ]

    report_lines.append("## Cluster Sizing")
    report_lines.extend(_format_size(our_cluster.get("analyticsSize"), "Analytics"))
    report_lines.extend(_format_size(our_cluster.get("loaderSize"), "Loader"))
    report_lines.extend(_format_size(our_cluster.get("cmcSize"), "CMC"))
    report_lines.extend([
        f"- **Analytics Nodes:** {our_cluster.get('analyticsNodes', 'N/A')}",
        f"- **Loader Nodes:** {our_cluster.get('loaderNodes', 'N/A')}",
        f"- **ZK Replicas:** {our_cluster.get('zkReplicas', 'N/A')}",
        "",
    ])

    # Consumption (still uses the separate endpoint — needs user_id)
    if include_consumption:
        try:
            user_id = cloud_client.get_user_id()
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

    # Authorized users (still uses the separate endpoint — needs user_id)
    if include_users:
        try:
            user_id = cloud_client.get_user_id()
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
        f"- **OpenAI:** {'Enabled' if our_cluster.get('enableOpenAI') else 'Disabled'}",
        f"- **MLflow:** {'Enabled' if our_cluster.get('mlflowEnabled') else 'Disabled'}",
        f"- **Data Studio:** {'Enabled' if our_cluster.get('enableDataStudio') else 'Disabled'}",
        f"- **On-Demand Loader:** {'Enabled' if (our_cluster.get('onDemandLoader') or {}).get('enabled') else 'Disabled'}",
        "",
    ])

    # Feature bits
    feature_bits = our_cluster.get("featureBits", [])
    if feature_bits:
        report_lines.append("## Feature Bits")
        for fb in feature_bits:
            fb_details = fb.get("featureBits", {})
            report_lines.append(
                f"- **{fb_details.get('name', 'Unknown')}** (`{fb_details.get('key', '')}`) — "
                f"{'Enabled' if fb_details.get('enabled') else 'Disabled'}"
            )
        report_lines.append("")

    report_lines.extend([
        "## Spark Configuration",
        f"- **Min Executors:** {our_cluster.get('minExecutors', 'N/A')}",
        f"- **Max Executors:** {our_cluster.get('maxExecutors', 'N/A')}",
        f"- **Spark Memory:** {our_cluster.get('sparkMem', 'N/A')} MB",
        f"- **Spark CPU:** {our_cluster.get('sparkCpu', 'N/A')} millicores",
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



@app.tool()
def extract_cluster_metadata_tool(
    cluster_name: str | None = None,
    format: Literal["json", "markdown", "both"] = "both"
) -> str:
    """
    [CLUSTER METADATA] Automatically extracts upgrade-relevant metadata from cluster data.
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
    cluster_name = _get_cmc_cluster_name(cluster_name)
    if not cluster_name:
        return (
            "Error: CMC cluster name not available.\n"
            "Please call the **cmc_login** tool first to authenticate via the login portal, "
            "or provide the cluster_name parameter."
        )
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
def search_upgrade_knowledge(query: str, limit: int = 10) -> str:
    """
    [MANUAL RESEARCH] Search Incorta documentation, community, and support articles.
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
    [CUSTOMER TICKETS - SCHEMA] Get Zendesk schema to understand available fields.
    Call this BEFORE querying customer tickets to see table structure.

    KEY TABLES FOR UPGRADES:
    - ticket (44 cols): Main ticket data - id, subject, priority, status, organization_id
    - ticket_customfields_v (15 cols): Severity, Deployment_Type, Release, Fixed_in
    - Ticket_Current_Release (2 cols): ticket_id, custom_field_value (current version)
    - Ticket_Target_Release (2 cols): ticket_id, custom_field_value (target version)
    - organization (15 cols): Customer data - id, name, region
    - ticket_jira_links (5 cols): Zendesk <-> Jira linkage

    UPGRADE ANALYSIS TABLES (used by automatic Zendesk collection):
    - ticket_tags: Tag-based filtering — most reliable way to find upgrade issues
    - Upgrade_tickets: Version tracking — from/to version for each upgrade ticket
    - Tickets_Env_Release: Environment context — cloud/onprem, release, account name
    - ticket_comments: Full communication history — problem descriptions, resolutions
    - ticket_audits/ticket_audit_events: Change tracking — escalations, reassignments
    - satisfaction_ratings: Customer satisfaction — scores, resolution quality

    Returns upgrade_analysis_ready=True if all required tables are present.
    Schema is cached within session to avoid redundant calls.
    """
    result = get_zendesk_schema({"fetch_schema": True})
    return json.dumps(result, indent=2)


@app.tool()
def query_upgrade_tickets(spark_sql: str) -> str:
    """
    [CUSTOMER TICKETS - QUERY] Query Zendesk for customer-reported support tickets.
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

    Returns bug_analysis_ready flag indicating whether all required tables
    (Issues, IssueFixVersions, IssueAffectedVersions, IssueLinks, IssueComponents)
    are present. Schema is cached after first successful fetch.

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
    [BUG TRACKING - QUERY] Query Jira for engineering bugs, features, and fixes.
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


if __name__ == "__main__":
    host = os.getenv("MCP_HOST", "127.0.0.1")
    port = int(os.getenv("MCP_PORT", "8000"))
    app.settings.host = host
    app.settings.port = port
    app.run(transport="streamable-http")
