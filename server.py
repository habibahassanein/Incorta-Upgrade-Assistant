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

import contextlib
import json
import logging
import os
import secrets
import sys
import time
from collections.abc import AsyncIterator
from typing import Any, Dict, Literal

import jwt as pyjwt
import requests
import uvicorn
from dotenv import load_dotenv
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response
from starlette.routing import Mount, Route
from starlette.types import Receive, Scope, Send

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from context.user_context import user_context
from workflows.pre_upgrade_validation import run_validation
from tools.qdrant_tool import search_knowledge_base
from tools.incorta_tools import query_zendesk, query_jira, get_zendesk_schema, get_jira_schema
from tools.extract_cluster_metadata import extract_cluster_metadata, format_metadata_report
from tools.test_connection import derive_incorta_url_from_cmc, login_to_incorta_analytics, test_all_connections
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

_DEFAULT_TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "templates", "pre_upgrade_checklist.xlsx"
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
                "PREREQUISITES:\n"
                "- CMC credentials in headers (cmc-url, cmc-user, cmc-password, cmc-cluster-name)\n"
                "- Cloud Portal: call cloud_portal_connect first; without it Cloud Portal fields show N/A\n"
                "- Ask the user for cloud_cluster_name (Cloud Portal instance name, e.g. 'habibascluster')\n\n"
                "Args:\n"
                "  to_version: Target Incorta version (e.g. '2024.7.0'). Required.\n"
                "  customer_name: Customer name as it appears in Jira. Required.\n"
                "  cmc_cluster_name: CMC cluster name. Defaults to cmc-cluster-name header.\n"
                "  from_version: Current version. Auto-detected if empty.\n"
                "  cloud_cluster_name: Cloud Portal cluster name. REQUIRED — ask the user."
            ),
            inputSchema={
                "type": "object",
                "required": ["to_version", "customer_name", "cloud_cluster_name"],
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
            name="run_pre_upgrade_validation",
            description=(
                "[HEALTH CHECK] Validates Incorta cluster health before upgrade.\n"
                "Performs comprehensive pre-upgrade health checks: service status, memory, "
                "topology, Spark/Zookeeper/DB, connectors, tenants, email config, DB migration.\n\n"
                "Args:\n"
                "  cluster_name: CMC cluster name. Defaults to cmc-cluster-name header."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_name": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="write_checklist_excel",
            description=(
                "[EXCEL OUTPUT] Write approved checklist values into an Excel template.\n"
                "Call this with the <checklist_data> JSON block from generate_upgrade_readiness_report.\n\n"
                "Args:\n"
                "  cell_values_json: JSON string from generate_upgrade_readiness_report.\n"
                "  filename: Suggested filename. Default: 'pre_upgrade_checklist_filled.xlsx'."
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
                "TWO-STEP PROCESS:\n"
                "  Step 1: Call this tool — you receive a login URL. Open it in your browser "
                "and complete login (Google SSO or username/password).\n"
                "  Step 2: Call this tool again — it confirms login and caches the token.\n\n"
                "Prerequisites:\n"
                "  - cloud-portal-email must be set in your MCP client headers.\n\n"
                "The token is cached server-side for your email and auto-refreshes. "
                "You only need to do this once per session (or after a long inactivity).\n\n"
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
                "IMPORTANT: Call cloud_portal_connect first. "
                "Ask the user for the Cloud Portal cluster name — do NOT guess it.\n\n"
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
                "Requires Analytics headers: incorta-tenant, incorta-username, incorta-password.\n"
                "Analytics URL is auto-derived from the cmc-url header."
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
            name="get_zendesk_schema_tool",
            description=(
                "[CUSTOMER TICKETS - SCHEMA] Get Zendesk schema to understand available fields.\n"
                "Call BEFORE querying customer tickets."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="query_upgrade_tickets",
            description=(
                "[CUSTOMER TICKETS - QUERY] Query Zendesk for customer-reported support tickets.\n"
                "Schema name: ZendeskTickets\n\n"
                "Args:\n"
                "  spark_sql: Spark SQL query against ZendeskTickets schema."
            ),
            inputSchema={
                "type": "object",
                "required": ["spark_sql"],
                "properties": {"spark_sql": {"type": "string"}},
            },
        ),
        types.Tool(
            name="get_jira_schema_tool",
            description=(
                "[BUG TRACKING - SCHEMA] Get Jira schema to understand available fields.\n"
                "Call BEFORE querying bugs/features."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="query_upgrade_issues",
            description=(
                "[BUG TRACKING - QUERY] Query Jira for engineering bugs, features, and fixes.\n"
                "Schema name: Jira_F\n\n"
                "Args:\n"
                "  spark_sql: Spark SQL query against Jira_F schema."
            ),
            inputSchema={
                "type": "object",
                "required": ["spark_sql"],
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
        cloud_cluster_name = arguments.get("cloud_cluster_name", "")
        from_version = arguments.get("from_version", "")
        cmc_cluster_name = _get_cmc_cluster_name(arguments.get("cmc_cluster_name"))

        if not cmc_cluster_name:
            return _text(
                "Error: CMC cluster name not available.\n"
                "Ensure cmc-cluster-name is set in your MCP client headers."
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
    # run_pre_upgrade_validation
    # -----------------------------------------------------------------------
    elif name == "run_pre_upgrade_validation":
        cluster_name = _get_cmc_cluster_name(arguments.get("cluster_name"))
        if not cluster_name:
            return _text(
                "Error: CMC cluster name not available.\n"
                "Ensure cmc-cluster-name is set in your MCP client headers."
            )
        try:
            return _text(run_validation(cluster_name))
        except Exception as e:
            return _text(f"Error running pre-upgrade validation: {e}")

    # -----------------------------------------------------------------------
    # write_checklist_excel
    # -----------------------------------------------------------------------
    elif name == "write_checklist_excel":
        cell_values_json = arguments.get("cell_values_json", "")
        filename = arguments.get("filename", "pre_upgrade_checklist_filled.xlsx")

        if not os.path.exists(_DEFAULT_TEMPLATE_PATH):
            return _json({"error": f"Template not found at '{_DEFAULT_TEMPLATE_PATH}'."})

        try:
            result = run_write_checklist_excel(
                cell_values_json=cell_values_json,
                template_path=_DEFAULT_TEMPLATE_PATH,
                filename=filename,
            )
            return _json(result)
        except Exception as e:
            return _json({"error": f"Error writing Excel: {e}"})

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
            if e.response is not None and e.response.status_code in (401, 403):
                return _text(
                    "Error: Cloud Portal token rejected.\n"
                    "Call `cloud_portal_connect` with force=true to re-authenticate."
                )
            return _text(f"Error fetching cluster from Cloud Portal: {e}")
        except Exception as e:
            return _text(f"Error: {e}")

        if not our_cluster:
            return _text(f"Error: Cluster '{cluster_name}' not found in Cloud Portal.")

        instance_uuid = our_cluster.get("id")
        cluster_status = our_cluster.get("status", "unknown")

        report_lines = [f"# Cloud Portal Metadata: {cluster_name}", ""]

        if cluster_status != "running":
            report_lines.extend([
                f"> **WARNING:** Cluster status is **{cluster_status}** (not running).",
                "> Some data may be stale.",
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

        if include_consumption:
            try:
                user_id = client.get_user_id()
                consumption = client.get_consumption(user_id, instance_uuid)
                consumption_agg = consumption.get("consumptionAgg", {})
                total_pu = consumption_agg.get("totalAgg", 0)
                daily_data = consumption_agg.get("total", {}).get("daily", [])

                report_lines.append("## Consumption & Cost")
                if daily_data:
                    avg_pu = sum(d.get("powerUnit", 0) for d in daily_data) / len(daily_data)
                    recent_7 = daily_data[-7:] if len(daily_data) >= 7 else daily_data
                    report_lines.extend([
                        f"- **Total (This Month):** {total_pu:.6f} Power Units",
                        f"- **Daily Average:** {avg_pu:.6f} PU/day",
                        f"- **Estimated Downtime Cost (4h):** {(avg_pu / 24 * 4):.6f} PU",
                        "",
                        "**Recent Trend (Last 7 Days):**",
                    ])
                    for day in recent_7:
                        report_lines.append(f"  - {day.get('startTime', 'Unknown')}: {day.get('powerUnit', 0):.6f} PU")
                elif total_pu:
                    report_lines.append(f"- **Total (This Month):** {total_pu:.6f} Power Units")
                else:
                    report_lines.append("- No consumption data available.")
                report_lines.append("")
            except Exception as e:
                report_lines.extend(["## Consumption & Cost", f"Could not fetch: {e}", ""])

        if include_users:
            try:
                user_id = client.get_user_id()
                users_data = client.get_authorized_users(user_id, cluster_name)
                users_list = users_data.get("authorizedUserRoles", [])
                report_lines.extend([f"## Authorized Users ({len(users_list)} total)", ""])
                for user_item in users_list:
                    u = user_item.get("user", {})
                    role = user_item.get("authorizedRoles", [{}])[0].get("role", "unknown")
                    status = user_item.get("status", "unknown")
                    report_lines.append(
                        f"- **{u.get('email')}** ({role.capitalize()}) - {status} - Last: {u.get('lastLoginAt', 'Never')}"
                    )
                report_lines.append("")
            except Exception as e:
                report_lines.extend(["## Authorized Users", f"Could not fetch: {e}", ""])

        report_lines.extend([
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
            f"- **Data Size:** {our_cluster.get('dsize')} GB",
            f"- **Loader Size:** {our_cluster.get('dsizeLoader')} GB",
            f"- **CMC Size:** {our_cluster.get('dsizeCmc')} GB",
            f"- **Available Disk:** {our_cluster.get('availableDisk')} GB",
            "",
            "## Upgrade History",
            f"- **Last Upgrade:** {our_cluster.get('initiatedUpgradeAt') or 'Never'}",
            f"- **Created:** {our_cluster.get('createdAt')}",
            f"- **Last Updated:** {our_cluster.get('updatedAt')}",
        ])

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
            session = login_to_incorta_analytics(incorta_url, tenant, username, password)
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
        result = search_knowledge_base({"query": query, "limit": limit})
        return _json(result)

    # -----------------------------------------------------------------------
    # get_zendesk_schema_tool
    # -----------------------------------------------------------------------
    elif name == "get_zendesk_schema_tool":
        result = get_zendesk_schema({"fetch_schema": True})
        return _json(result)

    # -----------------------------------------------------------------------
    # query_upgrade_tickets
    # -----------------------------------------------------------------------
    elif name == "query_upgrade_tickets":
        spark_sql = arguments.get("spark_sql", "")
        result = query_zendesk({"spark_sql": spark_sql})
        return _json(result)

    # -----------------------------------------------------------------------
    # get_jira_schema_tool
    # -----------------------------------------------------------------------
    elif name == "get_jira_schema_tool":
        result = get_jira_schema({"fetch_schema": True})
        return _json(result)

    # -----------------------------------------------------------------------
    # query_upgrade_issues
    # -----------------------------------------------------------------------
    elif name == "query_upgrade_issues":
        spark_sql = arguments.get("spark_sql", "")
        result = query_jira({"spark_sql": spark_sql})
        return _json(result)

    else:
        return _text(f"Unknown tool: {name}")


# ===========================================================================
# HTTP Routes
# ===========================================================================

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
        return _html(400, "Login Error",
                     "Login session not found or expired.<br>Please start over from Claude.")

    login_info = pending_logins.pop(state)
    email = login_info["email"]
    code_verifier = login_info["code_verifier"]
    redirect_uri = login_info["redirect_uri"]
    cmc_url = login_info.get("cmc_url", "")

    if error:
        error_desc = params.get("error_description", error)
        completed_logins[email] = {"success": False, "error": error_desc}
        return _html(400, "Login Failed", f"{error_desc}<br><br>Return to Claude and try again.")

    if not code:
        completed_logins[email] = {"success": False, "error": "No authorization code received."}
        return _html(400, "Login Failed",
                     "No authorization code received.<br>Return to Claude and try again.")

    # Exchange code for tokens
    try:
        token_data = exchange_code_for_token(code, redirect_uri, code_verifier, cmc_url)
    except RuntimeError as e:
        completed_logins[email] = {"success": False, "error": str(e)}
        return _html(500, "Login Failed",
                     f"Token exchange error: {e}<br>Return to Claude and try again.")

    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")

    if not access_token:
        completed_logins[email] = {"success": False, "error": "No access_token in Auth0 response."}
        return _html(500, "Login Failed",
                     "Auth0 did not return an access token.<br>Return to Claude and try again.")

    # Verify email claim matches
    try:
        claims = pyjwt.decode(access_token, options={"verify_signature": False})
    except pyjwt.DecodeError:
        claims = {}

    jwt_email = (
        claims.get("email")
        or claims.get("https://namespace/email")
        or ""
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

    return _html(200, "✅ Connected!",
                 f"Authenticated as <strong>{email}</strong>.{expiry_msg}<br><br>"
                 "You can close this tab and return to Claude.")


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

    user_context.set({
        "cmc_url":            raw_headers.get("cmc-url", "").rstrip("/"),
        "cmc_user":           raw_headers.get("cmc-user", ""),
        "cmc_password":       raw_headers.get("cmc-password", ""),
        "cmc_cluster_name":   raw_headers.get("cmc-cluster-name", ""),
        "incorta_tenant":     raw_headers.get("incorta-tenant", ""),
        "incorta_username":   raw_headers.get("incorta-username", ""),
        "incorta_password":   raw_headers.get("incorta-password", ""),
        "incorta_env_url":    raw_headers.get("incorta-analytics-url", "").rstrip("/"),
        "cloud_portal_email": raw_headers.get("cloud-portal-email", ""),
    })

    try:
        await session_manager.handle_request(scope, receive, send)
    except Exception as e:
        logger.exception(f"Error handling StreamableHTTP request: {e}")


@contextlib.asynccontextmanager
async def lifespan(_app: Starlette) -> AsyncIterator[None]:
    async with session_manager.run():
        logger.info(f"Incorta Upgrade Assistant MCP server started")
        logger.info(f"  StreamableHTTP: http://{MCP_HOST}:{MCP_PORT}/mcp")
        logger.info(f"  OAuth callback: {MCP_PUBLIC_URL or f'http://localhost:{MCP_PORT}'}/callback")
        try:
            yield
        finally:
            logger.info("Server shutting down.")


starlette_app = Starlette(
    debug=False,
    routes=[
        Route("/callback", endpoint=oauth_callback, methods=["GET"]),
        Mount("/mcp", app=handle_streamable_http),
    ],
    lifespan=lifespan,
)


if __name__ == "__main__":
    uvicorn.run(
        starlette_app,
        host=MCP_HOST,
        port=MCP_PORT,
        forwarded_allow_ips="*",   # trust nginx reverse proxy host headers
        proxy_headers=True,
    )
