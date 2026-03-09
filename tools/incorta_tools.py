

import logging
import os
from typing import Dict, Any, List
import requests

from context.user_context import user_context

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Zendesk schema cache & upgrade-analysis constants
# ---------------------------------------------------------------------------

_zendesk_schema_cache: dict | None = None

# Tables required for the full upgrade-analysis workflow (guide Section 9)
UPGRADE_ANALYSIS_TABLES: List[str] = [
    "ticket",
    "ticket_tags",
    "Upgrade_tickets",
    "Tickets_Env_Release",
    "ticket_comments",
    "ticket_audits",
    "ticket_audit_events",
    "satisfaction_ratings",
    "organization",
    "ticket_customfields_v",
]

# All 7 verified upgrade tags used for tag-based filtering
UPGRADE_TAGS: List[str] = [
    "upgrade",
    "upgrade-issue",
    "post_upgrade_issue",
    "cloud_upgrade",
    "customer_upgrade",
    "customer_upgrade_cloud",
    "customer_upgrade_onprem",
]

# Metadata documenting how each upgrade-analysis table is used
UPGRADE_ANALYSIS_TABLE_METADATA: dict = {
    "ticket_tags": {
        "purpose": "Tag-based filtering (most reliable for upgrade issues)",
        "join_pattern": "INNER JOIN ZendeskTickets.ticket_tags tt ON t.id = tt.ticket_id",
        "key_values": ", ".join(UPGRADE_TAGS),
    },
    "Upgrade_tickets": {
        "purpose": "Version tracking (from/to)",
        "join_pattern": "LEFT JOIN ZendeskTickets.Upgrade_tickets ut ON t.id = ut.Ticket_Id",
        "key_columns": "Ticket_Id, from, to",
    },
    "Tickets_Env_Release": {
        "purpose": "Environment & release context",
        "join_pattern": "LEFT JOIN ZendeskTickets.Tickets_Env_Release ter ON t.id = ter.ticket_id",
        "key_columns": "ticket_id, release, env, Account_Name",
    },
    "ticket_comments": {
        "purpose": "Full communication history",
        "join_pattern": "LEFT JOIN ZendeskTickets.ticket_comments tc ON t.id = tc.ticket_id",
        "key_columns": "ticket_id, body, type, public, author_id",
    },
    "ticket_audits": {
        "purpose": "Change tracking (status, priority, assignment changes)",
        "join_pattern": "via ticket_audit_events -> ticket_audits",
        "key_columns": "ticket_id, field_name, previous_value, value",
    },
    "satisfaction_ratings": {
        "purpose": "Customer satisfaction scoring",
        "join_pattern": "LEFT JOIN ZendeskTickets.satisfaction_ratings sr ON t.id = sr.ticket_id",
        "key_columns": "ticket_id, score, created_at",
    },
}


def clear_zendesk_schema_cache():
    """Clear the cached Zendesk schema. Call between sessions or when schema may have changed."""
    global _zendesk_schema_cache
    _zendesk_schema_cache = None


# ---------------------------------------------------------------------------
# Jira schema cache & bug-analysis constants
# ---------------------------------------------------------------------------

_jira_schema_cache: dict | None = None

# Tables required for the full bug-analysis workflow
BUG_ANALYSIS_TABLES: List[str] = [
    "Issues",
    "IssueFixVersions",
    "IssueAffectedVersions",
    "IssueLinks",
    "IssueComponents",
]

# Metadata documenting how each bug-analysis table is used
BUG_ANALYSIS_TABLE_METADATA: dict = {
    "Issues": {
        "purpose": "Main issue data (bugs, features, tasks)",
        "key_columns": "Key, Summary, StatusName, PriorityName, Customer, IssueTypeName",
    },
    "IssueFixVersions": {
        "purpose": "Which versions fix each issue",
        "join_pattern": "JOIN Jira_F.IssueFixVersions ifv ON i.Key = ifv.IssueKey",
        "key_columns": "IssueKey, Name (version name)",
    },
    "IssueAffectedVersions": {
        "purpose": "Versions affected by bugs",
        "join_pattern": "JOIN Jira_F.IssueAffectedVersions iav ON i.Key = iav.IssueKey",
        "key_columns": "IssueKey, Name (version name)",
    },
    "IssueLinks": {
        "purpose": "Issue relationships (blocks, duplicates, relates to)",
        "join_pattern": "JOIN Jira_F.IssueLinks il ON i.Key = il.SourceIssueKey",
        "key_columns": "SourceIssueKey, DestinationIssueKey, LinkTypeName",
    },
    "IssueComponents": {
        "purpose": "Product areas affected by each issue",
        "join_pattern": "JOIN Jira_F.IssueComponents ic ON i.Key = ic.IssueKey",
        "key_columns": "IssueKey, Name (component name)",
    },
}


def clear_jira_schema_cache():
    """Clear the cached Jira schema. Call between sessions or when schema may have changed."""
    global _jira_schema_cache
    _jira_schema_cache = None


def login_to_incorta():
    """
    Get Incorta session credentials from context.
    Returns session details for API calls.
    """
    ctx = user_context.get()
    env_url = ctx.get("incorta_env_url") or os.getenv("INCORTA_ENV_URL")
    tenant = ctx.get("incorta_tenant") or os.getenv("INCORTA_TENANT")
    username = ctx.get("incorta_username") or os.getenv("INCORTA_USERNAME")
    password = ctx.get("incorta_password") or os.getenv("INCORTA_PASSWORD")

    # Login
    response = requests.post(
        f"{env_url}/authservice/login",
        data={"tenant": tenant, "user": username, "pass": password},
        verify=True,
        timeout=60
    )

    if response.status_code != 200:
        raise Exception(f"Incorta login failed: {response.status_code}")

    # Extract session cookies
    id_cookie, login_id = None, None
    for item in response.cookies.items():
        if item[0].startswith("JSESSIONID"):
            id_cookie, login_id = item
            break

    if not id_cookie or not login_id:
        raise Exception("Failed to retrieve session cookies")

    # Get CSRF token
    response = requests.get(
        f"{env_url}/service/user/isLoggedIn",
        cookies={id_cookie: login_id},
        verify=True,
        timeout=60
    )

    if response.status_code != 200 or "XSRF-TOKEN" not in response.cookies:
        raise Exception(f"Failed to get CSRF token")

    csrf_token = response.cookies["XSRF-TOKEN"]
    authorization = response.json().get("accessToken")

    return {
        "env_url": env_url,
        "id_cookie": id_cookie,
        "id": login_id,
        "csrf": csrf_token,
        "authorization": authorization,
        "session_cookie": {id_cookie: login_id, "XSRF-TOKEN": csrf_token}
    }


def get_zendesk_schema(arguments: Dict[str, Any]) -> dict:
    """
    Get Zendesk schema details from Incorta.

    Returns a cached result after the first successful fetch to avoid redundant
    API calls when multiple helper functions need the schema in parallel.

    Args:
        arguments: Dict with optional 'fetch_schema' flag (kept for backward compatibility).

    Returns:
        dict: Schema details with tables, columns, upgrade_analysis_ready flag,
              missing_upgrade_tables list, and upgrade_analysis_tables metadata.
    """
    global _zendesk_schema_cache

    # Return cached schema if available
    if _zendesk_schema_cache is not None:
        return _zendesk_schema_cache

    try:
        login_creds = login_to_incorta()
    except Exception as e:
        return {
            "error": f"Zendesk schema unavailable (Incorta login failed: {e}). Customer ticket analysis will be skipped.",
            "source": "zendesk",
            "upgrade_analysis_ready": False,
            "missing_upgrade_tables": list(UPGRADE_ANALYSIS_TABLES),
            "data_gaps": ["All Zendesk tables — Incorta login failed"],
        }

    url = f"{login_creds['env_url']}/bff/v1/schemas/name/ZendeskTickets"

    cookie = ""
    for key, value in login_creds['session_cookie'].items():
        cookie += f"{key}={value};"

    headers = {
        "Authorization": f"Bearer {login_creds['authorization']}",
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": login_creds["csrf"],
        "Cookie": cookie
    }

    try:
        response = requests.get(url, headers=headers, verify=False, timeout=30)
    except requests.exceptions.RequestException as e:
        return {
            "error": f"Zendesk schema unavailable (request failed: {e}). Customer ticket analysis will be skipped.",
            "source": "zendesk",
            "upgrade_analysis_ready": False,
            "missing_upgrade_tables": list(UPGRADE_ANALYSIS_TABLES),
            "data_gaps": ["All Zendesk tables — data source unreachable"],
        }

    if response.status_code == 200:
        full_schema = response.json()

        # Compress schema: extract only table names and column names/types
        tables = []
        for table in full_schema.get("tables", []):
            table_name = table.get("name", "Unknown")
            columns = [
                f"{col.get('name', '')} ({col.get('dataType', 'UNKNOWN')})"
                for col in table.get("columns", [])
            ]
            tables.append({
                "table": table_name,
                "columns": columns
            })

        # Validate required upgrade-analysis tables
        found_tables = {t["table"] for t in tables}
        missing = [t for t in UPGRADE_ANALYSIS_TABLES if t not in found_tables]

        result = {
            "source": "zendesk",
            "schema_name": "ZendeskTickets",
            "tables": tables,
            "table_count": len(tables),
            "upgrade_analysis_ready": len(missing) == 0,
            "missing_upgrade_tables": missing,
            "upgrade_analysis_tables": UPGRADE_ANALYSIS_TABLE_METADATA,
            "note": "Schema compressed for context efficiency. Use SQL queries to access data.",
        }

        # Cache only successful responses
        _zendesk_schema_cache = result
        return result
    else:
        return {
            "error": f"Zendesk schema unavailable (HTTP {response.status_code}). Customer ticket analysis will be skipped.",
            "source": "zendesk",
            "upgrade_analysis_ready": False,
            "missing_upgrade_tables": list(UPGRADE_ANALYSIS_TABLES),
            "data_gaps": [f"All Zendesk tables — HTTP {response.status_code}"],
        }


def get_jira_schema(arguments: Dict[str, Any]) -> dict:
    """
    Get Jira schema details from Incorta.

    Returns a cached result after the first successful fetch to avoid redundant
    API calls when multiple helper functions need the schema in parallel.

    Args:
        arguments: Dict with optional 'fetch_schema' flag (kept for backward compatibility).

    Returns:
        dict: Schema details with tables, columns, bug_analysis_ready flag,
              missing_bug_tables list, and bug_analysis_tables metadata.
    """
    global _jira_schema_cache

    # Return cached schema if available
    if _jira_schema_cache is not None:
        return _jira_schema_cache

    try:
        login_creds = login_to_incorta()
    except Exception as e:
        return {
            "error": f"Jira schema unavailable (Incorta login failed: {e}). Bug analysis will be skipped.",
            "source": "jira",
            "bug_analysis_ready": False,
            "missing_bug_tables": list(BUG_ANALYSIS_TABLES),
            "data_gaps": ["All Jira tables — Incorta login failed"],
        }

    url = f"{login_creds['env_url']}/bff/v1/schemas/name/Jira_F"

    cookie = ""
    for key, value in login_creds['session_cookie'].items():
        cookie += f"{key}={value};"

    headers = {
        "Authorization": f"Bearer {login_creds['authorization']}",
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": login_creds["csrf"],
        "Cookie": cookie
    }

    try:
        response = requests.get(url, headers=headers, verify=False, timeout=30)
    except requests.exceptions.RequestException as e:
        return {
            "error": f"Jira schema unavailable (request failed: {e}). Bug analysis will be skipped.",
            "source": "jira",
            "bug_analysis_ready": False,
            "missing_bug_tables": list(BUG_ANALYSIS_TABLES),
            "data_gaps": ["All Jira tables — data source unreachable"],
        }

    if response.status_code == 200:
        full_schema = response.json()

        # Compress schema: extract only table names and column names/types
        tables = []
        for table in full_schema.get("tables", []):
            table_name = table.get("name", "Unknown")
            columns = [
                f"{col.get('name', '')} ({col.get('dataType', 'UNKNOWN')})"
                for col in table.get("columns", [])
            ]
            tables.append({
                "table": table_name,
                "columns": columns
            })

        # Validate required bug-analysis tables
        found_tables = {t["table"] for t in tables}
        missing = [t for t in BUG_ANALYSIS_TABLES if t not in found_tables]

        result = {
            "source": "jira",
            "schema_name": "Jira_F",
            "tables": tables,
            "table_count": len(tables),
            "bug_analysis_ready": len(missing) == 0,
            "missing_bug_tables": missing,
            "bug_analysis_tables": BUG_ANALYSIS_TABLE_METADATA,
            "note": "Schema compressed for context efficiency. Use SQL queries to access data.",
        }

        # Cache only successful responses
        _jira_schema_cache = result
        return result
    else:
        return {
            "error": f"Jira schema unavailable (HTTP {response.status_code}). Bug analysis will be skipped.",
            "source": "jira",
            "bug_analysis_ready": False,
            "missing_bug_tables": list(BUG_ANALYSIS_TABLES),
            "data_gaps": [f"All Jira tables — HTTP {response.status_code}"],
        }


def query_zendesk(arguments: Dict[str, Any]) -> dict:
    """
    Execute SQL query on Zendesk data in Incorta.

    Args:
        spark_sql (str): Spark SQL query to execute

    Returns:
        dict: Query results with columns and rows
    """
    login_creds = login_to_incorta()
    url = f"{login_creds['env_url']}/bff/v1/sqlxquery"

    cookie = ""
    for key, value in login_creds['session_cookie'].items():
        cookie += f"{key}={value};"

    headers = {
        "Authorization": f"Bearer {login_creds['authorization']}",
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": login_creds["csrf"],
        "Cookie": cookie
    }

    params = {"sql": arguments['spark_sql']}

    for attempt in range(2):
        try:
            response = requests.post(url, headers=headers, json=params,
                                     verify=False, timeout=120)
            break
        except requests.exceptions.Timeout:
            if attempt == 0:
                logger.warning("Zendesk query timed out, retrying...")
                continue
            return {"error": "Zendesk query timed out after 2 attempts (120s each). Try a more selective query."}

    if response.status_code == 200:
        return {
            "source": "zendesk",
            "data": response.json()
        }
    else:
        return {
            "error": f"Failed to query Zendesk: {response.status_code} - {response.text}"
        }


def query_jira(arguments: Dict[str, Any]) -> dict:
    """
    Execute SQL query on Jira data in Incorta.

    Args:
        spark_sql (str): Spark SQL query to execute

    Returns:
        dict: Query results with columns and rows
    """
    login_creds = login_to_incorta()
    url = f"{login_creds['env_url']}/bff/v1/sqlxquery"

    cookie = ""
    for key, value in login_creds['session_cookie'].items():
        cookie += f"{key}={value};"

    headers = {
        "Authorization": f"Bearer {login_creds['authorization']}",
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": login_creds["csrf"],
        "Cookie": cookie
    }

    params = {"sql": arguments['spark_sql']}

    for attempt in range(2):
        try:
            response = requests.post(url, headers=headers, json=params,
                                     verify=False, timeout=120)
            break
        except requests.exceptions.Timeout:
            if attempt == 0:
                logger.warning("Jira query timed out, retrying...")
                continue
            return {"error": "Jira query timed out after 2 attempts (120s each). Try a more selective query."}

    if response.status_code == 200:
        return {
            "source": "jira",
            "data": response.json()
        }
    else:
        return {
            "error": f"Failed to query Jira: {response.status_code} - {response.text}"
        }
