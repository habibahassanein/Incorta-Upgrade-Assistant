from __future__ import annotations

"""
Incorta Upgrade Assistant MCP Server

Authentication design:
  - CMC & Analytics: stateless, credentials passed as HTTP headers per-request
  - Cloud Portal:    OAuth 2.0 PKCE flow via `cloud_portal_connect` tool;
                     JWTs cached per-user in data/tokens/{email}.json

Headers read from every MCP request:
  cmc-url, cmc-user, cmc-password, cmc-cluster-name
  incorta-tenant, incorta-username, incorta-password
  cloud-portal-email
"""

import asyncio
import contextlib
import json
import logging
import os
import secrets
import sys
import tempfile
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Dict, Literal

import jwt as pyjwt
import requests
import uvicorn
from dotenv import load_dotenv
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, HTMLResponse, Response
from starlette.routing import Mount, Route
from starlette.types import Receive, Scope, Send

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from context.user_context import user_context
from tools.qdrant_tool import get_embedding_model, search_knowledge_base
from tools.incorta_tools import (
    query_zendesk,
    query_jira,
    get_zendesk_schema,
    get_jira_schema,
)
from tools.extract_cluster_metadata import (
    extract_cluster_metadata,
    format_metadata_report,
)
from tools.test_connection import (
    derive_incorta_url_from_cmc,
    login_to_incorta_analytics,
    test_all_connections,
)
from clients.cloud_portal_client import (
    CloudPortalClient,
    build_authorize_url,
    exchange_code_for_token,
    get_valid_token,
    save_token,
    delete_token,
    load_token,
)

from workflows.checklist_workflow import run_write_checklist_excel
from workflows.readiness_report import run_readiness_report

logger = logging.getLogger("incorta-upgrade-agent")
logging.basicConfig(level=logging.INFO)

MCP_PORT = int(os.getenv("MCP_PORT", "8000"))
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PUBLIC_URL = os.getenv("MCP_PUBLIC_URL", "").rstrip("/")

# Temp directory for generated Excel files (served via /download/<token>)
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/tmp/upgrade-assistant-downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_TTL = int(os.getenv("DOWNLOAD_TTL", "3600"))  # 1 hour

_DEFAULT_TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "templates",
    "pre_upgrade_checklist.xlsx",
)

# ---------------------------------------------------------------------------
# OAuth state storage (in-memory)
# pending_logins:   state_token → {email, code_verifier, redirect_uri, expires_at}
# completed_logins: email       → {success: bool, error: str|None}
# ---------------------------------------------------------------------------
pending_logins: Dict[str, Dict] = {}
completed_logins: Dict[str, Dict] = {}


def _purge_expired_pending_logins():
    """Remove stale pending login states (older than 10 minutes)."""
    now = time.time()
    expired = [s for s, v in pending_logins.items() if v.get("expires_at", 0) < now]
    for s in expired:
        del pending_logins[s]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_cmc_client():
    """Build a CMCClient from the current request's ContextVar."""
    from clients.cmc_client import CMCClient

    ctx = user_context.get()
    cmc_url = ctx.get("cmc_url") or ""
    cmc_user = ctx.get("cmc_user") or ""
    cmc_password = ctx.get("cmc_password") or ""
    cmc_cluster_name = ctx.get("cmc_cluster_name") or ""

    if not cmc_url or not cmc_user or not cmc_password:
        raise RuntimeError(
            "CMC credentials not configured.\n"
            "Add cmc-url, cmc-user, and cmc-password to your MCP client headers."
        )

    return CMCClient(
        url=cmc_url,
        user=cmc_user,
        password=cmc_password,
        cluster_name=cmc_cluster_name,
    )


def _get_cmc_cluster_name(explicit: str | None = None) -> str | None:
    """Resolve CMC cluster name from explicit param or request context."""
    if explicit:
        return explicit
    ctx = user_context.get()
    return ctx.get("cmc_cluster_name") or os.getenv("CMC_CLUSTER_NAME") or None


def _get_cloud_portal_client() -> tuple[CloudPortalClient | None, str | None]:
    """
    Get a CloudPortalClient loaded with the current user's cached JWT.

    Returns (client, None) on success.
    Returns (None, error_message) if no valid token — includes login URL.
    """
    ctx = user_context.get()
    email = ctx.get("cloud_portal_email", "").strip()
    cmc_url = ctx.get("cmc_url", "")

    if not email:
        return None, (
            "Error: cloud-portal-email not configured.\n"
            "Add 'cloud-portal-email' to your MCP client config headers."
        )

    token = get_valid_token(email, cmc_url)

    if not token:
        # Build a fresh login URL so the user can immediately act
        public_url = MCP_PUBLIC_URL or f"http://localhost:{MCP_PORT}"
        redirect_uri = f"{public_url}/callback"
        login_info = build_authorize_url(redirect_uri, cmc_url)
        state = login_info["state"]
        pending_logins[state] = {
            "email": email,
            "code_verifier": login_info["code_verifier"],
            "redirect_uri": redirect_uri,
            "cmc_url": cmc_url,
            "expires_at": time.time() + 600,
        }

        return None, (
            f"Cloud Portal session expired or not set up for **{email}**.\n\n"
            f"Call the **`cloud_portal_connect`** tool — it will return a login URL.\n"
            f"Or open this URL directly in your browser:\n{login_info['authorize_url']}\n\n"
            f"After logging in, call this tool again."
        )

    return CloudPortalClient(bearer_token=token, cmc_url=cmc_url), None


# ===========================================================================
# MCP Server
# ===========================================================================

app = Server("incorta-upgrade-agent")


@app.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="generate_upgrade_readiness_report",
            description=(
                "[CORE - RUN FIRST] Generate a comprehensive Upgrade Readiness Report. "
                "Orchestrates all data sources (CMC, Cloud Portal, knowledge base, upgrade research, "
                "Zendesk customer support tickets, and Jira bug tracking) to produce an opinionated "
                "readiness assessment with a rating and Excel checklist data.\n\n"
                "CREDENTIALS NOTE: All required passwords and credentials (CMC, Analytics, Cloud Portal, Internal DB) "
                "are AUTOMATICALLY handled by the user's MCP connection headers or server environment.\n"
                "DO NOT ask the user for any credentials or passwords. Just call the tools directly.\n\n"
                "MANDATORY STEPS BEFORE CALLING THIS TOOL:\n"
                "  1. ALWAYS call cloud_portal_connect first — even if you think the token is cached.\n"
                "     This is required. Cloud Portal provides Spark version, Python version, disk sizes.\n"
                "  2. Ask the user for customer_name — must be a real COMPANY name from Jira.\n"
                "     Examples: 'Apple', 'Starbucks', 'Keysight', 'Toast', 'Broadcom'.\n"
                "     REJECT person names ('Anas', 'John') or generic words ('test', 'demo').\n\n"
                "GENERATING THE EXCEL CHECKLIST (MANDATORY AFTER REPORT):\n"
                "  The report includes a <checklist_data> JSON block. Use it to fill the official template:\n"
                "  1. Copy the template to Claude's sandbox:\n"
                "     Use Filesystem:copy_file_user_to_claude with path:\n"
                "     /Users/anasahmed/WorkProjects/Upgrade_agent_v2/Incorta-Upgrade-Assistant/templates/pre_upgrade_checklist.xlsx\n"
                "  2. Use bash_tool to fill it with openpyxl:\n"
                "     python3 << 'PYEOF'\n"
                "     import json; from openpyxl import load_workbook; from openpyxl.styles import PatternFill, Alignment\n"
                "     wb = load_workbook('/mnt/user-data/uploads/pre_upgrade_checklist.xlsx')\n"
                "     ws = wb['Pre-Upgrade Checklist']\n"
                "     fills = {\n"
                "       'Done': PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid'),\n"
                "       'PASS': PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid'),\n"
                "       'Failed': PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid'),\n"
                "       'FAIL': PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid'),\n"
                "       'Review': PatternFill(start_color='FFEB9C', end_color='FFEB9C', fill_type='solid'),\n"
                "       'WARNING': PatternFill(start_color='FFEB9C', end_color='FFEB9C', fill_type='solid'),\n"
                "       'Pending': PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid'),\n"
                "       'N/A': PatternFill(start_color='D9D9D9', end_color='D9D9D9', fill_type='solid'),\n"
                "     }\n"
                "     data = json.loads('<checklist_data_json_here>')\n"
                "     for row, cols in data.items():\n"
                "       b = ws.cell(row=int(row), column=2); b.value = cols.get('B', ''); b.alignment = Alignment(wrap_text=True, vertical='top')\n"
                "       c = ws.cell(row=int(row), column=3); c.value = cols.get('C', ''); c.alignment = Alignment(horizontal='center', vertical='center')\n"
                "       if cols.get('C') in fills: c.fill = fills[cols['C']]\n"
                "     wb.save('/mnt/user-data/outputs/<filename>.xlsx')\n"
                "     PYEOF\n"
                "  3. Call present_files with /mnt/user-data/outputs/<filename>.xlsx\n"
                "  NOTE: The template has 8 sheets — only Pre-Upgrade Checklist is modified. Column B = value, C = status.\n\n"
                "Args:\n"
                "  to_version: Target Incorta version (e.g. '2026.1.0'). Required.\n"
                "  customer_name: Company name from Jira (e.g. 'Apple', 'Starbucks'). Optional but recommended.\n"
                "     If provided, the report includes bugs specifically fixed for that customer.\n"
                "     If omitted or unknown, pass 'Unknown' and the report runs without customer bug data.\n"
                "     MUST be a company name, not a person name ('Anas', 'John' are invalid).\n"
                "  cmc_cluster_name: CMC cluster name. Defaults to cmc-cluster-name header.\n"
                "  from_version: Current version. Leave empty — auto-detected from cluster.\n"
                "  cloud_cluster_name: Cloud Portal cluster name. Leave empty — auto-detected from Analytics URL."
            ),
            inputSchema={
                "type": "object",
                "required": ["to_version"],
                "properties": {
                    "to_version": {"type": "string"},
                    "customer_name": {"type": "string"},
                    "cmc_cluster_name": {"type": "string"},
                    "from_version": {"type": "string"},
                    "cloud_cluster_name": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="write_checklist_excel",
            description=(
                "[EXCEL OUTPUT] Fills the official 8-sheet Incorta pre-upgrade checklist template\n"
                "with the collected data and returns a direct download link for the xlsx file.\n\n"
                "ALWAYS call this after generate_upgrade_readiness_report.\n"
                "Pass the entire <checklist_data> JSON block from the report output.\n\n"
                "OUTPUT: Returns JSON with:\n"
                "  - 'download_url': A direct HTTPS link to download the filled Excel file.\n"
                "  - 'filename': The suggested filename.\n"
                "  - 'summary': A human-readable summary of what was filled.\n"
                "  - 'expires_in': How long the link is valid (1 hour).\n\n"
                "HOW TO DELIVER:\n"
                "  1. Show the summary to the user.\n"
                "  2. Present the download_url as a clickable markdown link:\n"
                "     [Download Checklist](download_url)\n"
                "  3. Tell the user the link expires in 1 hour.\n"
                "  DO NOT attempt to decode base64 or write files yourself — the server handles everything.\n"
                "  DO NOT build the Excel yourself with openpyxl — this tool uses the official template.\n\n"
                "Args:\n"
                "  cell_values_json: The <checklist_data> JSON from generate_upgrade_readiness_report.\n"
                "  filename: Output filename. Default: 'pre_upgrade_checklist_filled.xlsx'."
            ),
            inputSchema={
                "type": "object",
                "required": ["cell_values_json"],
                "properties": {
                    "cell_values_json": {"type": "string"},
                    "filename": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="cloud_portal_connect",
            description=(
                "[CLOUD AUTH] Connect your Cloud Portal account.\n\n"
                "CREDENTIALS: The user's email is automatically injected from MCP headers.\n"
                "If the cached token is expired, call this tool to give the user a browser login link.\n\n"
                "TWO-STEP PROCESS:\n"
                "  Step 1: Call this tool — you receive a login URL. Open it in your browser "
                "and complete login (Google SSO or username/password).\n"
                "  Step 2: Call this tool again — it confirms login and caches the token.\n\n"
                "Args:\n"
                "  force: If True, clears the existing token and forces re-authentication."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "force": {"type": "boolean"},
                },
            },
        ),
        types.Tool(
            name="get_cloud_metadata",
            description=(
                "[CLOUD DATA] Get cloud metadata from Cloud Portal API.\n"
                "Provides data NOT available in CMC: Spark/Python/MySQL versions, sizing, "
                "feature flags, consumption, authorized users, upgrade history.\n\n"
                "CREDENTIALS: Uses cached token from cloud_portal_connect. Do not ask for passwords.\n"
                "IMPORTANT: Call cloud_portal_connect first. Ask the user for the Cloud Portal cluster name.\n\n"
                "Args:\n"
                "  cluster_name: Cloud Portal cluster name (e.g. 'habibascluster'). REQUIRED.\n"
                "  include_consumption: Include cost data. Default: True.\n"
                "  include_users: Include authorized users. Default: True."
            ),
            inputSchema={
                "type": "object",
                "required": ["cluster_name"],
                "properties": {
                    "cluster_name": {"type": "string"},
                    "include_consumption": {"type": "boolean"},
                    "include_users": {"type": "boolean"},
                },
            },
        ),
        types.Tool(
            name="extract_cluster_metadata_tool",
            description=(
                "[CLUSTER METADATA] Extract upgrade-relevant metadata from CMC cluster data.\n"
                "Auto-detects deployment type, DB type, topology, features, infrastructure.\n\n"
                "CREDENTIALS: CMC credentials are automatically injected from MCP headers. Do not ask the user.\n\n"
                "Args:\n"
                "  cluster_name: CMC cluster name. Defaults to cmc-cluster-name header.\n"
                "  format: 'json', 'markdown', or 'both' (default)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_name": {"type": "string"},
                    "format": {"type": "string", "enum": ["json", "markdown", "both"]},
                },
            },
        ),
        types.Tool(
            name="test_datasource_connections",
            description=(
                "[CONNECTIVITY CHECK] Test all datasource connections on the Incorta Analytics instance.\n"
                "Fetches all datasources and tests each connection, reporting success/failure.\n\n"
                "CREDENTIALS: ALL Analytics credentials (tenant, user, password, URL) are AUTOMATICALLY injected "
                "from MCP headers. The LLM MUST call this tool directly WITHOUT asking the user for passwords."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        types.Tool(
            name="search_upgrade_knowledge",
            description=(
                "[MANUAL RESEARCH] Search Incorta documentation, community, and support articles.\n\n"
                "CREDENTIALS: API keys are handled by the server. Call directly.\n\n"
                "Args:\n"
                "  query: Search query (e.g. '2024.7.0 release notes')\n"
                "  limit: Number of results. Default: 10."
            ),
            inputSchema={
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {"type": "string"},
                    "limit": {"type": "integer"},
                },
            },
        ),
        types.Tool(
            name="query_upgrade_tickets",
            description=(
                "[CUSTOMER TICKETS] Query Zendesk for customer-reported support tickets.\n"
                "Schema (ZendeskTickets) is always included in the response.\n\n"
                "CREDENTIALS: DB login is handled automatically. Call directly.\n\n"
                "Args:\n"
                "  spark_sql: (Optional) Spark SQL query against ZendeskTickets schema.\n"
                "             If omitted, returns schema only."
            ),
            inputSchema={
                "type": "object",
                "properties": {"spark_sql": {"type": "string"}},
            },
        ),
        types.Tool(
            name="query_upgrade_issues",
            description=(
                "[BUG TRACKING] Query Jira for engineering bugs, features, and fixes.\n"
                "Schema (Jira_F) is always included in the response.\n\n"
                "CREDENTIALS: DB login is handled automatically. Call directly.\n\n"
                "Args:\n"
                "  spark_sql: (Optional) Spark SQL query against Jira_F schema.\n"
                "             If omitted, returns schema only."
            ),
            inputSchema={
                "type": "object",
                "properties": {"spark_sql": {"type": "string"}},
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> list[types.TextContent]:
    """Dispatch tool calls."""

    def _text(content: str) -> list[types.TextContent]:
        return [types.TextContent(type="text", text=content)]

    def _json(obj: Any) -> list[types.TextContent]:
        return [types.TextContent(type="text", text=json.dumps(obj, indent=2))]

    # -----------------------------------------------------------------------
    # generate_upgrade_readiness_report
    # -----------------------------------------------------------------------
    if name == "generate_upgrade_readiness_report":
        to_version = arguments.get("to_version", "")
        customer_name = arguments.get("customer_name", "")
        if not customer_name:
            customer_name = "Unknown"

        cloud_cluster_name = arguments.get("cloud_cluster_name", "")
        if not cloud_cluster_name:
            cloud_cluster_name = user_context.get().get("auto_cloud_cluster_name", "")

        from_version = arguments.get("from_version", "")
        cmc_cluster_name = _get_cmc_cluster_name(arguments.get("cmc_cluster_name"))

        if not cmc_cluster_name:
            return _text(
                "Error: CMC cluster name not available.\n"
                "Ensure cmc-cluster-name is set in your MCP client headers"
            )
        if not cloud_cluster_name:
            return _text(
                "Error: cloud_cluster_name is required.\n"
                "Ask the user for the Cloud Portal cluster name (e.g. 'habibascluster')."
            )

        try:
            result = run_readiness_report(
                cmc_cluster_name=cmc_cluster_name,
                to_version=to_version,
                customer_name=customer_name,
                cloud_cluster_name=cloud_cluster_name,
            )
            return _text(result)
        except Exception as e:
            return _text(f"Error generating readiness report: {e}")

    # -----------------------------------------------------------------------
    # write_checklist_excel
    # -----------------------------------------------------------------------
    elif name == "write_checklist_excel":
        cell_values_json = arguments.get("cell_values_json", "")
        filename = arguments.get("filename", "pre_upgrade_checklist_filled.xlsx")
        if not filename.endswith(".xlsx"):
            filename += ".xlsx"

        if not os.path.exists(_DEFAULT_TEMPLATE_PATH):
            return _json({"error": f"Template not found at '{_DEFAULT_TEMPLATE_PATH}'."})

        try:
            result = run_write_checklist_excel(
                cell_values_json=cell_values_json,
                template_path=_DEFAULT_TEMPLATE_PATH,
                filename=filename,
            )
        except Exception as e:
            return _json({"error": f"Error writing Excel: {e}"})

        # Save to DOWNLOAD_DIR and return a URL — no base64 in tool result
        token = secrets.token_hex(16) + ".xlsx"
        dest = DOWNLOAD_DIR / token
        try:
            import base64 as _b64
            dest.write_bytes(_b64.b64decode(result["base64"]))
        except Exception as e:
            return _json({"error": f"Error saving file to disk: {e}"})

        public_base = MCP_PUBLIC_URL or f"http://localhost:{MCP_PORT}"
        download_url = f"{public_base}/download/{token}?filename={filename}"

        return _json({
            "download_url": download_url,
            "filename": filename,
            "summary": result.get("summary", ""),
            "expires_in": "1 hour",
        })

    # -----------------------------------------------------------------------
    # cloud_portal_connect
    # -----------------------------------------------------------------------
    elif name == "cloud_portal_connect":
        ctx = user_context.get()
        email = ctx.get("cloud_portal_email", "").strip()
        cmc_url = ctx.get("cmc_url", "")
        force = arguments.get("force", False)

        if not email:
            return _text(
                "Error: cloud-portal-email not configured.\n"
                "Add 'cloud-portal-email' to your MCP client config headers."
            )

        # Force re-auth — clear existing token
        if force:
            delete_token(email)
            completed_logins.pop(email, None)

        # Already have a valid token
        token = get_valid_token(email, cmc_url)
        if token:
            try:
                claims = pyjwt.decode(token, options={"verify_signature": False})
                exp = claims.get("exp")
                if exp:
                    hours = (exp - time.time()) / 3600
                    expiry = f"Token valid for {hours:.1f} more hours."
                else:
                    expiry = "Token valid."
            except Exception:
                expiry = "Token valid."
            return _text(
                f"## Cloud Portal — Already Connected\n\n"
                f"- **Email:** {email}\n"
                f"- {expiry}\n\n"
                f"Cloud Portal tools are ready. "
                f"Use `force=true` to re-authenticate."
            )

        # Check if user just completed login (callback was received)
        _purge_expired_pending_logins()
        if email in completed_logins:
            result = completed_logins.pop(email)
            if result.get("success"):
                token_data = load_token(email)
                exp = token_data.get("exp") if token_data else None
                expiry = ""
                if exp:
                    hours = (exp - time.time()) / 3600
                    expiry = f"\n- **Token valid for:** {hours:.1f} hours"
                return _text(
                    f"## Cloud Portal — Connected ✅\n\n"
                    f"- **Email:** {email}"
                    f"{expiry}\n\n"
                    f"Cloud Portal tools are now ready to use."
                )
            else:
                return _text(
                    f"## Cloud Portal — Login Failed\n\n"
                    f"{result.get('error', 'Unknown error')}\n\n"
                    f"Call this tool again to get a new login URL."
                )

        # Start a new OAuth flow
        public_url = MCP_PUBLIC_URL or f"http://localhost:{MCP_PORT}"
        redirect_uri = f"{public_url}/callback"
        login_info = build_authorize_url(redirect_uri, cmc_url)
        state = login_info["state"]

        pending_logins[state] = {
            "email": email,
            "code_verifier": login_info["code_verifier"],
            "redirect_uri": redirect_uri,
            "cmc_url": cmc_url,
            "expires_at": time.time() + 600,
        }

        return _text(
            f"## Cloud Portal Connect\n\n"
            f"**Open this URL in your browser to connect `{email}`:**\n\n"
            f"{login_info['authorize_url']}\n\n"
            f"Log in with your Incorta account (Google SSO or email/password).\n"
            f"After completing login, **call this tool again** to confirm."
        )

    # -----------------------------------------------------------------------
    # get_cloud_metadata
    # -----------------------------------------------------------------------
    elif name == "get_cloud_metadata":
        cluster_name = arguments.get("cluster_name", "")
        include_consumption = arguments.get("include_consumption", True)
        include_users = arguments.get("include_users", True)
        ctx = user_context.get()
        cmc_url = ctx.get("cmc_url", "")

        if not cluster_name:
            return _text(
                "Error: cluster_name is required.\n"
                "Ask the user for the Cloud Portal cluster name (e.g. 'habibascluster')."
            )

        client, error = _get_cloud_portal_client()
        if error:
            return _text(error)

        try:
            our_cluster = client.search_instances(cluster_name, cmc_url)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            body = e.response.text[:300] if e.response is not None else ""
            if status == 401:
                # 401 from the cp- API usually means wrong audience in the JWT,
                # NOT that the token is expired. Surface the raw error so we can debug.
                return _text(
                    f"Error: Cloud Admin API returned 401.\n\n"
                    f"This usually means the Auth0 audience in the token does not match "
                    f"what cp-cloudstaging.incortalabs.com expects.\n\n"
                    f"Current audience: {client._headers().get('Authorization', '')[:40]}...\n"
                    f"API response: {body}\n\n"
                    f"Ask the cloud team: what is the correct Auth0 audience for "
                    f"client_id=0jXCrcpFe6PDm6sIMxDi7hunFCWeRLpt?"
                )
            if status == 403:
                return _text(
                    f"Error: Cloud Admin API returned 403 — token accepted but insufficient permissions.\n"
                    f"API response: {body}"
                )
            return _text(
                f"Error fetching cluster from Cloud Portal (HTTP {status}): {e}"
            )
        except Exception as e:
            return _text(f"Error: {e}")

        if not our_cluster:
            return _text(f"Error: Cluster '{cluster_name}' not found in Cloud Portal.")

        instance_uuid = our_cluster.get("id")
        cluster_status = our_cluster.get("status", "unknown")

        report_lines = [f"# Cloud Portal Metadata: {cluster_name}", ""]

        if cluster_status != "running":
            report_lines.extend(
                [
                    f"> **WARNING:** Cluster status is **{cluster_status}** (not running).",
                    "> Some data may be stale.",
                    "",
                ]
            )

        report_lines.extend(
            [
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
            ]
        )

        services = our_cluster.get("instanceServices", [])
        if services:
            svc = services[0]
            report_lines.extend(
                [
                    "## Service Status",
                    f"- **CMC:** {svc.get('cmc_status', 'N/A')}",
                    f"- **Analytics:** {svc.get('analytics_status', 'N/A')}",
                    f"- **Loader:** {svc.get('loader_status', 'N/A')}",
                    f"- **Spark:** {svc.get('spark_status', 'N/A')}",
                    f"- **Zookeeper:** {svc.get('zookeeper_status', 'N/A')}",
                    "",
                ]
            )

        report_lines.extend(
            [
                "## Software Versions",
                f"- **Spark:** {our_cluster.get('incortaSparkVersion')}",
                f"- **Python:** {our_cluster.get('pythonVersion') or 'N/A'}",
                f"- **MySQL:** {our_cluster.get('mysqlVersion') or 'N/A'}",
                "",
            ]
        )

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
        report_lines.extend(
            [
                f"- **Analytics Nodes:** {our_cluster.get('analyticsNodes', 'N/A')}",
                f"- **Loader Nodes:** {our_cluster.get('loaderNodes', 'N/A')}",
                f"- **ZK Replicas:** {our_cluster.get('zkReplicas', 'N/A')}",
                "",
            ]
        )

        if include_consumption:
            try:
                user_id = client.get_user_id()
                consumption = client.get_consumption(user_id, instance_uuid)
                consumption_agg = consumption.get("consumptionAgg", {})
                total_pu = consumption_agg.get("totalAgg", 0)
                daily_data = consumption_agg.get("total", {}).get("daily", [])

                report_lines.append("## Consumption & Cost")
                if daily_data:
                    avg_pu = sum(d.get("powerUnit", 0) for d in daily_data) / len(
                        daily_data
                    )
                    recent_7 = daily_data[-7:] if len(daily_data) >= 7 else daily_data
                    report_lines.extend(
                        [
                            f"- **Total (This Month):** {total_pu:.6f} Power Units",
                            f"- **Daily Average:** {avg_pu:.6f} PU/day",
                            f"- **Estimated Downtime Cost (4h):** {(avg_pu / 24 * 4):.6f} PU",
                            "",
                            "**Recent Trend (Last 7 Days):**",
                        ]
                    )
                    for day in recent_7:
                        report_lines.append(
                            f"  - {day.get('startTime', 'Unknown')}: {day.get('powerUnit', 0):.6f} PU"
                        )
                elif total_pu:
                    report_lines.append(
                        f"- **Total (This Month):** {total_pu:.6f} Power Units"
                    )
                else:
                    report_lines.append("- No consumption data available.")
                report_lines.append("")
            except Exception as e:
                report_lines.extend(
                    ["## Consumption & Cost", f"Could not fetch: {e}", ""]
                )

        if include_users:
            try:
                user_id = client.get_user_id()
                users_data = client.get_authorized_users(user_id, cluster_name)
                users_list = users_data.get("authorizedUserRoles", [])
                report_lines.extend(
                    [f"## Authorized Users ({len(users_list)} total)", ""]
                )
                for user_item in users_list:
                    u = user_item.get("user", {})
                    role = user_item.get("authorizedRoles", [{}])[0].get(
                        "role", "unknown"
                    )
                    status = user_item.get("status", "unknown")
                    report_lines.append(
                        f"- **{u.get('email')}** ({role.capitalize()}) - {status} - Last: {u.get('lastLoginAt', 'Never')}"
                    )
                report_lines.append("")
            except Exception as e:
                report_lines.extend(
                    ["## Authorized Users", f"Could not fetch: {e}", ""]
                )

        report_lines.extend(
            [
                "## Feature Flags",
                f"- **SQLi:** {'Enabled' if our_cluster.get('sqliEnabled') else 'Disabled'}",
                f"- **Incorta X:** {'Enabled' if our_cluster.get('incortaXEnabled') else 'Disabled'}",
                f"- **Data Agent:** {'Enabled' if our_cluster.get('enableDataAgent') else 'Disabled'}",
                f"- **Chat/OpenAI:** {'Enabled' if our_cluster.get('enableChat') else 'Disabled'}",
                f"- **MLflow:** {'Enabled' if our_cluster.get('mlflowEnabled') else 'Disabled'}",
                f"- **Data Studio:** {'Enabled' if our_cluster.get('enableDataStudio') else 'Disabled'}",
                "",
                "## Spark Configuration",
                f"- **Min Executors:** {our_cluster.get('minExecutors', 'N/A')}",
                f"- **Max Executors:** {our_cluster.get('maxExecutors', 'N/A')}",
                f"- **Spark Memory:** {our_cluster.get('sparkMem', 'N/A')} MB",
                f"- **Spark CPU:** {our_cluster.get('sparkCpu', 'N/A')} millicores",
                "",
                "## Storage",
                f"- **Analytics Pod (dsize):** {our_cluster.get('dsize')} GB (allocated)",
                f"- **Loader Pod (dsizeLoader):** {our_cluster.get('dsizeLoader')} GB (allocated)",
                f"- **CMC Pod (dsizeCmc):** {our_cluster.get('dsizeCmc')} GB (allocated)",
                "- _Per-pod utilization not available (API limited)_",
                f"- **Available Disk:** {our_cluster.get('availableDisk')} GB",
                f"- **Tenant Folder Size (consumedData):** {our_cluster.get('consumedData')} GB",
                "",
                "## Cluster Configuration",
                f"- **Timezone:** {our_cluster.get('timezone', 'N/A')}",
                f"- **Auto-Suspend:** {'Enabled' if our_cluster.get('sleeppable') else 'Disabled'}",
                f"- **Idle Time:** {our_cluster.get('idleTime', 'N/A')} hours",
                "",
                "## Upgrade History",
                f"- **Last Upgrade:** {our_cluster.get('initiatedUpgradeAt') or 'Never'}",
                f"- **Created:** {our_cluster.get('createdAt')}",
                f"- **Last Updated:** {our_cluster.get('updatedAt')}",
            ]
        )

        return _text("\n".join(report_lines))

    # -----------------------------------------------------------------------
    # extract_cluster_metadata_tool
    # -----------------------------------------------------------------------
    elif name == "extract_cluster_metadata_tool":
        cluster_name = _get_cmc_cluster_name(arguments.get("cluster_name"))
        fmt = arguments.get("format", "both")

        if not cluster_name:
            return _text(
                "Error: CMC cluster name not available.\n"
                "Ensure cmc-cluster-name is set in your MCP client headers."
            )

        try:
            client = _get_cmc_client()
            cluster_data = client.get_cluster(cluster_name)
        except (RuntimeError, requests.exceptions.RequestException) as e:
            return _text(f"Error: Failed to fetch cluster data from CMC: {e}")

        try:
            metadata = extract_cluster_metadata(cluster_data)
        except Exception as e:
            return _text(f"Error: Failed to extract metadata: {e}")

        if fmt == "json":
            return _json(metadata)
        elif fmt == "markdown":
            return _text(format_metadata_report(metadata))
        else:
            md = format_metadata_report(metadata)
            js = json.dumps(metadata, indent=2)
            return _text(f"{md}\n\n---\n\n## Raw Metadata (JSON)\n\n```json\n{js}\n```")

    # -----------------------------------------------------------------------
    # test_datasource_connections
    # -----------------------------------------------------------------------
    elif name == "test_datasource_connections":
        ctx = user_context.get()
        cmc_url = ctx.get("cmc_url", "")
        tenant = ctx.get("incorta_tenant", "")
        username = ctx.get("incorta_username", "")
        password = ctx.get("incorta_password", "")

        if not cmc_url:
            return _text(
                "Error: cmc-url not configured.\n"
                "Ensure cmc-url is set in your MCP client headers."
            )
        missing = []
        if not tenant:
            missing.append("incorta-tenant")
        if not username:
            missing.append("incorta-username")
        if not password:
            missing.append("incorta-password")
        if missing:
            return _text(
                f"Error: Analytics credentials not configured: {', '.join(missing)}.\n"
                f"Add these headers to your MCP client config."
            )

        incorta_url = derive_incorta_url_from_cmc(cmc_url)

        try:
            session = login_to_incorta_analytics(
                incorta_url, tenant, username, password
            )
        except RuntimeError as e:
            return _text(f"Error: Analytics login failed: {e}")

        try:
            result = test_all_connections(session)
            return _json(result)
        except Exception as e:
            return _json({"error": f"Failed to test connections: {e}"})

    # -----------------------------------------------------------------------
    # search_upgrade_knowledge
    # -----------------------------------------------------------------------
    elif name == "search_upgrade_knowledge":
        query = arguments.get("query", "")
        limit = arguments.get("limit", 10)
        result = await search_knowledge_base({"query": query, "limit": limit})
        return _json(result)

    # -----------------------------------------------------------------------
    # query_upgrade_tickets (auto-includes Zendesk schema)
    # -----------------------------------------------------------------------
    elif name == "query_upgrade_tickets":
        schema = get_zendesk_schema({"fetch_schema": True})
        spark_sql = arguments.get("spark_sql", "")
        if spark_sql:
            query_result = query_zendesk({"spark_sql": spark_sql})
            return _json({"schema": schema, "query_result": query_result})
        return _json({"schema": schema})

    # -----------------------------------------------------------------------
    # query_upgrade_issues (auto-includes Jira schema)
    # -----------------------------------------------------------------------
    elif name == "query_upgrade_issues":
        schema = get_jira_schema({"fetch_schema": True})
        spark_sql = arguments.get("spark_sql", "")
        if spark_sql:
            query_result = query_jira({"spark_sql": spark_sql})
            return _json({"schema": schema, "query_result": query_result})
        return _json({"schema": schema})

    else:
        return _text(f"Unknown tool: {name}")


# ===========================================================================
# HTTP Routes
# ===========================================================================


async def serve_download(request: Request) -> Response:
    """Serve a generated Excel file by token. Files auto-expire after DOWNLOAD_TTL seconds."""
    token = request.path_params.get("token", "")
    if not token or "/" in token or ".." in token:
        return Response("Not found", status_code=404)

    # Opportunistic cleanup of expired files
    now = time.time()
    for f in DOWNLOAD_DIR.glob("*.xlsx"):
        try:
            if now - f.stat().st_mtime > DOWNLOAD_TTL:
                f.unlink(missing_ok=True)
        except OSError:
            pass

    file_path = DOWNLOAD_DIR / token
    if not file_path.exists() or not file_path.is_file():
        return Response("File not found or expired (files expire after 1 hour)", status_code=404)

    filename = request.query_params.get("filename", token)
    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


async def debug_token(request: Request) -> HTMLResponse:
    """
    Debug endpoint: shows the current cached token claims for an email.
    Usage: GET /debug-token?email=anas.ahmed@incorta.com
    """
    email = request.query_params.get("email", "")
    if not email:
        return HTMLResponse("<pre>Usage: /debug-token?email=your@email.com</pre>")

    from clients.cloud_portal_client import load_token
    import pyjwt_compat

    data = load_token(email)
    if not data:
        return HTMLResponse(f"<pre>No token found for {email}</pre>")

    access_token = data.get("access_token", "")
    try:
        claims = pyjwt.decode(access_token, options={"verify_signature": False})
    except Exception as e:
        claims = {"decode_error": str(e)}

    import json as _json

    output = {
        "email": email,
        "has_refresh_token": bool(data.get("refresh_token")),
        "cached_at": data.get("cached_at"),
        "exp": claims.get("exp"),
        "expires_in_hours": round((claims.get("exp", 0) - time.time()) / 3600, 2)
        if claims.get("exp")
        else None,
        "audience": claims.get("aud"),
        "issuer": claims.get("iss"),
        "scope": claims.get("scope"),
        "azp": claims.get("azp"),
        "sub": claims.get("sub"),
    }
    return HTMLResponse(f"<pre>{_json.dumps(output, indent=2)}</pre>")


async def oauth_callback(request: Request) -> HTMLResponse:
    """
    OAuth 2.0 callback handler.

    Receives Auth0 redirect after user login, validates state (links to the
    user's email), exchanges the authorization code for tokens, verifies the
    email claim, and saves the JWT to data/tokens/{email}.json.
    """
    params = dict(request.query_params)
    state = params.get("state")
    code = params.get("code")
    error = params.get("error")

    def _html(status: int, title: str, body: str) -> HTMLResponse:
        content = (
            "<!DOCTYPE html><html><head>"
            f"<title>{title}</title>"
            "<style>"
            "body{font-family:sans-serif;max-width:560px;margin:80px auto;text-align:center;color:#15152b}"
            "h2{margin-bottom:12px}"
            "p{color:#555;font-size:15px}"
            ".code{font-size:24px;font-weight:700;color:#4854fe;margin:20px 0}"
            ".ok{color:#027a48}"
            ".err{color:#b42318}"
            "</style></head><body>"
            f"<h2>{title}</h2><p>{body}</p>"
            "</body></html>"
        )
        return HTMLResponse(content=content, status_code=status)

    # Validate state
    if not state or state not in pending_logins:
        return _html(
            400,
            "Login Error",
            "Login session not found or expired.<br>Please start over from Claude.",
        )

    login_info = pending_logins.pop(state)
    email = login_info["email"]
    code_verifier = login_info["code_verifier"]
    redirect_uri = login_info["redirect_uri"]
    cmc_url = login_info.get("cmc_url", "")

    if error:
        error_desc = params.get("error_description", error)
        completed_logins[email] = {"success": False, "error": error_desc}
        return _html(
            400, "Login Failed", f"{error_desc}<br><br>Return to Claude and try again."
        )

    if not code:
        completed_logins[email] = {
            "success": False,
            "error": "No authorization code received.",
        }
        return _html(
            400,
            "Login Failed",
            "No authorization code received.<br>Return to Claude and try again.",
        )

    # Exchange code for tokens
    try:
        token_data = exchange_code_for_token(code, redirect_uri, code_verifier, cmc_url)
    except RuntimeError as e:
        completed_logins[email] = {"success": False, "error": str(e)}
        return _html(
            500,
            "Login Failed",
            f"Token exchange error: {e}<br>Return to Claude and try again.",
        )

    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")

    if not access_token:
        completed_logins[email] = {
            "success": False,
            "error": "No access_token in Auth0 response.",
        }
        return _html(
            500,
            "Login Failed",
            "Auth0 did not return an access token.<br>Return to Claude and try again.",
        )

    # Verify email claim matches
    try:
        claims = pyjwt.decode(access_token, options={"verify_signature": False})
    except pyjwt.DecodeError:
        claims = {}

    jwt_email = (
        claims.get("email") or claims.get("https://namespace/email") or ""
    ).lower()

    if jwt_email and jwt_email != email.lower():
        msg = f"You logged in as {jwt_email} but your config says {email}. Please update cloud-portal-email in your config."
        completed_logins[email] = {"success": False, "error": msg}
        return _html(400, "Email Mismatch", msg)

    # Save token per-user
    save_token(email, access_token, refresh_token, claims)
    completed_logins[email] = {"success": True}

    exp = claims.get("exp")
    expiry_msg = ""
    if exp:
        hours = (exp - time.time()) / 3600
        expiry_msg = f" Token valid for {hours:.1f} hours."

    return _html(
        200,
        "✅ Connected!",
        f"Authenticated as <strong>{email}</strong>.{expiry_msg}<br><br>"
        "You can close this tab and return to Claude.",
    )


# ===========================================================================
# Starlette Application
# ===========================================================================

session_manager = StreamableHTTPSessionManager(
    app=app,
    event_store=None,
    json_response=True,
    stateless=True,
)


async def handle_streamable_http(scope: Scope, receive: Receive, send: Send) -> None:
    """
    StreamableHTTP MCP endpoint.

    Extracts all credential headers and stores them in the user_context
    ContextVar before delegating to the session manager.
    Each request is fully isolated — ContextVar is per-coroutine.
    """
    raw_headers = {
        k.decode("utf-8", errors="replace").lower(): v.decode("utf-8", errors="replace")
        for k, v in scope.get("headers", [])
    }

    cmc_url = raw_headers.get("cmc-url", "").rstrip("/")
    cmc_cluster_name = raw_headers.get("cmc-cluster-name", "")

    # auto-detect Analytics url from cmc URL if missing
    incorta_env_url = raw_headers.get("incorta-analytics-url", "").rstrip("/")
    if not incorta_env_url and cmc_url.endswith("/cmc"):
        incorta_env_url = cmc_url[:-4] + "/incorta"

    # auto-detect Cloud Portal cluster name from the Analytics url first subdomain
    auto_cloud_cluster_name = ""
    if incorta_env_url:
        import urllib.parse

        parsed_env = urllib.parse.urlparse(incorta_env_url)
        if parsed_env.hostname:
            auto_cloud_cluster_name = parsed_env.hostname.split(".")[0]

    user_context.set(
        {
            "cmc_url": cmc_url,
            "cmc_user": raw_headers.get("cmc-user", ""),
            "cmc_password": raw_headers.get("cmc-password", ""),
            "cmc_cluster_name": cmc_cluster_name,
            "incorta_tenant": raw_headers.get(
                "incorta-tenant", "default"
            ),  # default fallback
            "incorta_username": raw_headers.get("incorta-username", ""),
            "incorta_password": raw_headers.get("incorta-password", ""),
            "incorta_env_url": incorta_env_url,
            "cloud_portal_email": raw_headers.get("cloud-portal-email", ""),
            "auto_cloud_cluster_name": auto_cloud_cluster_name,
        }
    )

    try:
        await session_manager.handle_request(scope, receive, send)
    except Exception as e:
        logger.exception(f"Error handling StreamableHTTP request: {e}")


@contextlib.asynccontextmanager
async def lifespan(_app: Starlette) -> AsyncIterator[None]:
    # Preload the embedding model before accepting traffic so the first
    # search_upgrade_knowledge call cannot stall the event loop downloading
    # ~440MB from HuggingFace inside a request handler.
    logger.info("Preloading embedding model (BAAI/bge-base-en-v1.5)...")
    await asyncio.to_thread(get_embedding_model)
    logger.info("Embedding model ready.")
    async with session_manager.run():
        logger.info(f"Incorta Upgrade Assistant MCP server started")
        logger.info(f"  StreamableHTTP: http://{MCP_HOST}:{MCP_PORT}/mcp")
        logger.info(
            f"  OAuth callback: {MCP_PUBLIC_URL or f'http://localhost:{MCP_PORT}'}/callback"
        )
        try:
            yield
        finally:
            logger.info("Server shutting down.")


starlette_app = Starlette(
    debug=False,
    routes=[
        Route("/callback", endpoint=oauth_callback, methods=["GET"]),
        Route("/debug-token", endpoint=debug_token, methods=["GET"]),
        Route("/download/{token}", endpoint=serve_download, methods=["GET"]),
        Mount("/mcp", app=handle_streamable_http),
    ],
    lifespan=lifespan,
)


if __name__ == "__main__":
    uvicorn.run(
        starlette_app,
        host=MCP_HOST,
        port=MCP_PORT,
        forwarded_allow_ips="*",  # trust nginx reverse proxy host headers
        proxy_headers=True,
    )
