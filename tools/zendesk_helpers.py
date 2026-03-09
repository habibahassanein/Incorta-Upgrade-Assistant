"""
Zendesk Helper Functions for Upgrade Analysis

Encapsulates the 6 specialized queries from the upgrade_agent_data_retrieval_guide.
Each function is schema-aware: it checks `upgrade_analysis_ready` from the cached
schema before running any SQL, and returns structured errors with `data_gaps`
when required tables are missing.

No user writes SQL — all queries are generated internally.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from tools.incorta_tools import (
    get_zendesk_schema,
    query_zendesk,
    UPGRADE_TAGS,
)

logger = logging.getLogger(__name__)

# SQL-ready IN clause for upgrade tags
_TAGS_IN_CLAUSE = ", ".join(f"'{t}'" for t in UPGRADE_TAGS)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _check_schema_ready() -> dict | None:
    """Return an error dict if schema is not ready, else None."""
    schema = get_zendesk_schema({"fetch_schema": True})
    if schema.get("error") or not schema.get("upgrade_analysis_ready"):
        missing = schema.get("missing_upgrade_tables", [])
        return {
            "error": f"Zendesk upgrade analysis unavailable — missing tables: {missing}",
            "data_gaps": missing,
        }
    return None


def _run_query(sql: str) -> dict:
    """Execute a Spark SQL query and return the raw result dict."""
    return query_zendesk({"spark_sql": sql})


def _extract_rows(result: dict) -> List[Dict[str, Any]]:
    """Extract rows from a query_zendesk response as list-of-dicts.

    The Incorta sqlxquery endpoint returns:
      { "data": { "columns": [...], "rows": [[...], ...] } }
    or sometimes:
      { "data": [ { "col": val, ... }, ... ] }
    This helper normalises both formats.
    """
    if result.get("error"):
        return []

    data = result.get("data", {})

    # Format 1: { "columns": [...], "rows": [[...], ...] }
    if isinstance(data, dict):
        columns = data.get("columns", [])
        rows_raw = data.get("rows", [])
        if columns and rows_raw:
            return [dict(zip(columns, row)) for row in rows_raw]
        # If no columns/rows, it might already be a list or empty
        if isinstance(data.get("data"), list):
            return data["data"]
        return []

    # Format 2: already a list of dicts
    if isinstance(data, list):
        return data

    return []


# ---------------------------------------------------------------------------
# Helper 1: Issues by Version Pair (Guide Section 7, Query A)
# ---------------------------------------------------------------------------


def get_upgrade_issues_by_version_pair(from_v: str, to_v: str) -> dict:
    """Identify known upgrade issues for a specific version pair.

    Returns:
        {
            "version_pairs": [{from, to, issue_count, affected_accounts, resolved_count}],
            "found": bool,
            "error": str | None,
            "data_gaps": []
        }
    """
    err = _check_schema_ready()
    if err:
        return {"version_pairs": [], "found": False, **err}

    sql = f"""
    SELECT
        ut.`from`                          AS from_version,
        ut.`to`                            AS to_version,
        COUNT(DISTINCT t.id)               AS issue_count,
        COUNT(DISTINCT ter.Account_Name)   AS affected_accounts,
        COUNT(CASE WHEN t.status = 'closed' THEN 1 END) AS resolved_count
    FROM ZendeskTickets.ticket t
    INNER JOIN ZendeskTickets.ticket_tags tt ON t.id = tt.ticket_id
    LEFT JOIN ZendeskTickets.Upgrade_tickets ut ON t.id = ut.Ticket_Id
    LEFT JOIN ZendeskTickets.Tickets_Env_Release ter ON t.id = ter.ticket_id
    WHERE LOWER(tt.tag) IN ({_TAGS_IN_CLAUSE})
      AND ut.`from` = '{from_v}' AND ut.`to` = '{to_v}'
    GROUP BY ut.`from`, ut.`to`
    """

    result = _run_query(sql)
    if result.get("error"):
        return {
            "version_pairs": [],
            "found": False,
            "error": result["error"],
            "data_gaps": [],
        }

    rows = _extract_rows(result)
    pairs = [
        {
            "from": r.get("from_version", from_v),
            "to": r.get("to_version", to_v),
            "issue_count": int(r.get("issue_count", 0)),
            "affected_accounts": int(r.get("affected_accounts", 0)),
            "resolved_count": int(r.get("resolved_count", 0)),
        }
        for r in rows
    ]
    return {
        "version_pairs": pairs,
        "found": len(pairs) > 0,
        "error": None,
        "data_gaps": [],
    }


# ---------------------------------------------------------------------------
# Helper 2: High-Risk Upgrade Patterns (Guide Section 7, Query B)
# ---------------------------------------------------------------------------


def get_high_risk_upgrade_patterns(from_v: str, to_v: str) -> dict:
    """Assess risk level for a specific upgrade path.

    Returns:
        {
            "upgrade_path": str,
            "risk_level": "HIGH" | "MEDIUM" | "LOW" | "UNKNOWN",
            "critical_issues": int,
            "total_issues": int,
            "max_resolution_days": int,
            "avg_resolution_days": float,
            "warnings": [],
            "error": str | None,
            "data_gaps": []
        }
    """
    err = _check_schema_ready()
    if err:
        return {
            "upgrade_path": f"{from_v} -> {to_v}",
            "risk_level": "UNKNOWN",
            "critical_issues": 0,
            "total_issues": 0,
            "max_resolution_days": 0,
            "avg_resolution_days": 0.0,
            "warnings": [],
            **err,
        }

    sql = f"""
    SELECT
        COUNT(DISTINCT t.id) AS total_issues,
        SUM(CASE WHEN t.priority IN ('urgent', 'high') THEN 1 ELSE 0 END) AS critical_issues,
        MAX(DATEDIFF(t.updated_at, t.created_at)) AS max_resolution_days,
        AVG(DATEDIFF(t.updated_at, t.created_at)) AS avg_resolution_days
    FROM ZendeskTickets.ticket t
    INNER JOIN ZendeskTickets.ticket_tags tt ON t.id = tt.ticket_id
    INNER JOIN ZendeskTickets.Upgrade_tickets ut ON t.id = ut.Ticket_Id
    WHERE LOWER(tt.tag) IN ({_TAGS_IN_CLAUSE})
      AND ut.`from` = '{from_v}' AND ut.`to` = '{to_v}'
    """

    result = _run_query(sql)
    if result.get("error"):
        return {
            "upgrade_path": f"{from_v} -> {to_v}",
            "risk_level": "UNKNOWN",
            "critical_issues": 0,
            "total_issues": 0,
            "max_resolution_days": 0,
            "avg_resolution_days": 0.0,
            "warnings": [],
            "error": result["error"],
            "data_gaps": [],
        }

    rows = _extract_rows(result)
    row = rows[0] if rows else {}

    total = int(row.get("total_issues", 0))
    critical = int(row.get("critical_issues", 0))
    max_days = int(row.get("max_resolution_days", 0) or 0)
    avg_days = float(row.get("avg_resolution_days", 0) or 0)

    # Determine risk level
    warnings: list[str] = []
    if critical >= 3 or total >= 10:
        risk_level = "HIGH"
        warnings.append(f"{critical} critical issues reported for this upgrade path")
    elif critical >= 1 or total >= 5:
        risk_level = "MEDIUM"
        warnings.append(f"{total} issues reported, {critical} critical")
    elif total > 0:
        risk_level = "LOW"
        warnings.append(f"{total} minor issue(s) reported")
    else:
        risk_level = "LOW"

    if max_days > 14:
        warnings.append(f"Longest resolution took {max_days} days")

    return {
        "upgrade_path": f"{from_v} -> {to_v}",
        "risk_level": risk_level,
        "critical_issues": critical,
        "total_issues": total,
        "max_resolution_days": max_days,
        "avg_resolution_days": round(avg_days, 1),
        "warnings": warnings,
        "error": None,
        "data_gaps": [],
    }


# ---------------------------------------------------------------------------
# Helper 3: Environment-Specific Issues (Guide Section 7, Query C)
# ---------------------------------------------------------------------------


def get_environment_specific_issues(from_v: str, to_v: str) -> dict:
    """Break down upgrade issues by environment (cloud vs on-prem).

    Returns:
        {
            "by_environment": { "env_name": { issue_count, affected_accounts, issue_tags } },
            "found": bool,
            "error": str | None,
            "data_gaps": []
        }
    """
    err = _check_schema_ready()
    if err:
        return {"by_environment": {}, "found": False, **err}

    sql = f"""
    SELECT
        ter.env                            AS environment,
        COUNT(DISTINCT t.id)               AS issue_count,
        COUNT(DISTINCT ter.Account_Name)   AS affected_accounts
    FROM ZendeskTickets.ticket t
    INNER JOIN ZendeskTickets.ticket_tags tt ON t.id = tt.ticket_id
    INNER JOIN ZendeskTickets.Upgrade_tickets ut ON t.id = ut.Ticket_Id
    INNER JOIN ZendeskTickets.Tickets_Env_Release ter ON t.id = ter.ticket_id
    WHERE LOWER(tt.tag) IN ({_TAGS_IN_CLAUSE})
      AND ut.`from` = '{from_v}' AND ut.`to` = '{to_v}'
    GROUP BY ter.env
    ORDER BY issue_count DESC
    """

    result = _run_query(sql)
    if result.get("error"):
        return {
            "by_environment": {},
            "found": False,
            "error": result["error"],
            "data_gaps": [],
        }

    rows = _extract_rows(result)
    by_env: dict[str, dict] = {}
    for r in rows:
        env_name = r.get("environment", "Unknown") or "Unknown"
        by_env[env_name] = {
            "issue_count": int(r.get("issue_count", 0)),
            "affected_accounts": int(r.get("affected_accounts", 0)),
        }

    return {
        "by_environment": by_env,
        "found": len(by_env) > 0,
        "error": None,
        "data_gaps": [],
    }


# ---------------------------------------------------------------------------
# Helper 4: Common Issue Types (Guide Section 7, Query D)
# ---------------------------------------------------------------------------


def get_common_issue_types() -> dict:
    """Identify the most common upgrade issue tag patterns across all versions.

    Returns:
        {
            "issue_types": [{ tag, count, avg_resolution_days }],
            "found": bool,
            "error": str | None,
            "data_gaps": []
        }
    """
    err = _check_schema_ready()
    if err:
        return {"issue_types": [], "found": False, **err}

    sql = f"""
    SELECT
        tt.tag,
        COUNT(DISTINCT t.id)                            AS issue_count,
        ROUND(AVG(DATEDIFF(t.updated_at, t.created_at)), 1) AS avg_resolution_days
    FROM ZendeskTickets.ticket t
    INNER JOIN ZendeskTickets.ticket_tags tt ON t.id = tt.ticket_id
    WHERE LOWER(tt.tag) IN ({_TAGS_IN_CLAUSE})
    GROUP BY tt.tag
    ORDER BY issue_count DESC
    """

    result = _run_query(sql)
    if result.get("error"):
        return {
            "issue_types": [],
            "found": False,
            "error": result["error"],
            "data_gaps": [],
        }

    rows = _extract_rows(result)
    types = [
        {
            "tag": r.get("tag", ""),
            "count": int(r.get("issue_count", 0)),
            "avg_resolution_days": float(r.get("avg_resolution_days", 0) or 0),
        }
        for r in rows
    ]
    return {
        "issue_types": types,
        "found": len(types) > 0,
        "error": None,
        "data_gaps": [],
    }


# ---------------------------------------------------------------------------
# Helper 5: Complete Upgrade Issues (Guide Section 6 — master query)
# ---------------------------------------------------------------------------


def get_complete_upgrade_issues(from_v: str, to_v: str) -> dict:
    """Retrieve fully-enriched upgrade issue details for a version pair.

    Returns:
        {
            "issues": [{ ticket_id, subject, status, priority, upgrade_from,
                         upgrade_to, environment, customer_account, tags,
                         days_to_resolution }],
            "total_issues": int,
            "error": str | None,
            "data_gaps": []
        }
    """
    err = _check_schema_ready()
    if err:
        return {"issues": [], "total_issues": 0, **err}

    sql = f"""
    SELECT
        t.id                              AS ticket_id,
        t.subject,
        t.status,
        t.priority,
        t.created_at,
        t.updated_at,
        ut.`from`                         AS upgrade_from,
        ut.`to`                           AS upgrade_to,
        ter.env                           AS environment,
        ter.Account_Name                  AS customer_account,
        DATEDIFF(t.updated_at, t.created_at) AS days_to_resolution
    FROM ZendeskTickets.ticket t
    INNER JOIN ZendeskTickets.ticket_tags tt ON t.id = tt.ticket_id
    LEFT JOIN ZendeskTickets.Upgrade_tickets ut ON t.id = ut.Ticket_Id
    LEFT JOIN ZendeskTickets.Tickets_Env_Release ter ON t.id = ter.ticket_id
    WHERE LOWER(tt.tag) IN ({_TAGS_IN_CLAUSE})
      AND ut.`from` = '{from_v}' AND ut.`to` = '{to_v}'
    ORDER BY t.created_at DESC
    LIMIT 50
    """

    result = _run_query(sql)
    if result.get("error"):
        return {
            "issues": [],
            "total_issues": 0,
            "error": result["error"],
            "data_gaps": [],
        }

    rows = _extract_rows(result)
    issues = [
        {
            "ticket_id": r.get("ticket_id"),
            "subject": r.get("subject", ""),
            "status": r.get("status", ""),
            "priority": r.get("priority", ""),
            "upgrade_from": r.get("upgrade_from", from_v),
            "upgrade_to": r.get("upgrade_to", to_v),
            "environment": r.get("environment", ""),
            "customer_account": r.get("customer_account", ""),
            "days_to_resolution": int(r.get("days_to_resolution", 0) or 0),
        }
        for r in rows
    ]
    return {
        "issues": issues,
        "total_issues": len(issues),
        "error": None,
        "data_gaps": [],
    }


# ---------------------------------------------------------------------------
# Helper 6: Satisfaction Assessment (Guide Section 5)
# ---------------------------------------------------------------------------


def assess_upgrade_satisfaction(from_v: str, to_v: str) -> dict:
    """Assess customer satisfaction for a specific upgrade path.

    Returns:
        {
            "avg_satisfaction": float,
            "rated_count": int,
            "total_tickets": int,
            "resolved_count": int,
            "avg_resolution_days": float,
            "error": str | None,
            "data_gaps": []
        }
    """
    err = _check_schema_ready()
    if err:
        return {
            "avg_satisfaction": 0.0,
            "rated_count": 0,
            "total_tickets": 0,
            "resolved_count": 0,
            "avg_resolution_days": 0.0,
            **err,
        }

    sql = f"""
    SELECT
        COUNT(DISTINCT t.id)                                 AS total_tickets,
        COUNT(DISTINCT CASE WHEN sr.score IS NOT NULL THEN t.id END) AS rated_count,
        AVG(sr.score)                                        AS avg_satisfaction,
        COUNT(CASE WHEN t.status = 'closed' THEN 1 END)     AS resolved_count,
        ROUND(AVG(DATEDIFF(t.updated_at, t.created_at)), 1) AS avg_resolution_days
    FROM ZendeskTickets.ticket t
    INNER JOIN ZendeskTickets.ticket_tags tt ON t.id = tt.ticket_id
    INNER JOIN ZendeskTickets.Upgrade_tickets ut ON t.id = ut.Ticket_Id
    LEFT JOIN ZendeskTickets.satisfaction_ratings sr ON t.id = sr.ticket_id
    WHERE LOWER(tt.tag) IN ({_TAGS_IN_CLAUSE})
      AND ut.`from` = '{from_v}' AND ut.`to` = '{to_v}'
    """

    result = _run_query(sql)
    if result.get("error"):
        return {
            "avg_satisfaction": 0.0,
            "rated_count": 0,
            "total_tickets": 0,
            "resolved_count": 0,
            "avg_resolution_days": 0.0,
            "error": result["error"],
            "data_gaps": [],
        }

    rows = _extract_rows(result)
    row = rows[0] if rows else {}

    return {
        "avg_satisfaction": round(float(row.get("avg_satisfaction", 0) or 0), 1),
        "rated_count": int(row.get("rated_count", 0)),
        "total_tickets": int(row.get("total_tickets", 0)),
        "resolved_count": int(row.get("resolved_count", 0)),
        "avg_resolution_days": float(row.get("avg_resolution_days", 0) or 0),
        "error": None,
        "data_gaps": [],
    }


# ---------------------------------------------------------------------------
# Helper 7: Linked Jira Keys (for Zendesk→Jira bridge)
# ---------------------------------------------------------------------------


def get_linked_jira_keys(from_v: str, to_v: str) -> dict:
    """Extract Jira issue keys linked to upgrade-related Zendesk tickets.

    Queries the ticket_jira_links table to find Jira keys associated with
    upgrade tickets for a given version pair, enabling targeted Jira analysis.

    Returns:
        {
            "jira_keys": [str],
            "found": bool,
            "error": str | None,
            "data_gaps": []
        }
    """
    err = _check_schema_ready()
    if err:
        return {"jira_keys": [], "found": False, **err}

    # ticket_jira_links may not exist — query gracefully
    sql = f"""
    SELECT DISTINCT tjl.jira_issue_key
    FROM ZendeskTickets.ticket_jira_links tjl
    INNER JOIN ZendeskTickets.ticket t ON tjl.ticket_id = t.id
    INNER JOIN ZendeskTickets.ticket_tags tt ON t.id = tt.ticket_id
    LEFT JOIN ZendeskTickets.Upgrade_tickets ut ON t.id = ut.Ticket_Id
    WHERE LOWER(tt.tag) IN ({_TAGS_IN_CLAUSE})
      AND (ut.`from` = '{from_v}' AND ut.`to` = '{to_v}')
    """

    result = _run_query(sql)
    if result.get("error"):
        # ticket_jira_links may not exist — not a blocker
        return {
            "jira_keys": [],
            "found": False,
            "error": None,
            "data_gaps": ["ticket_jira_links table may not be available"],
        }

    rows = _extract_rows(result)
    keys = [r.get("jira_issue_key", "") for r in rows if r.get("jira_issue_key")]

    return {
        "jira_keys": keys,
        "found": len(keys) > 0,
        "error": None,
        "data_gaps": [],
    }


# ---------------------------------------------------------------------------
# Helper 8: Customer-Specific Jira Links (all tickets, not just upgrade-tagged)
# ---------------------------------------------------------------------------


def get_customer_jira_links(customer_name: str) -> dict:
    """Extract Jira issue keys linked to ANY of a customer's Zendesk tickets.

    Unlike get_linked_jira_keys() which only finds upgrade-tagged tickets,
    this helper finds ALL Jira links for a customer's org — providing a
    complete view of customer bugs tracked in both systems.

    Uses case-insensitive LIKE matching on organization name for semantic
    fuzzy matching (e.g., "Acme" matches "Acme Corp", "ACME Corporation").

    Args:
        customer_name: Customer name (fuzzy matched against org name).

    Returns:
        {
            "jira_keys": [str],
            "found": bool,
            "error": str | None,
            "data_gaps": []
        }
    """
    if not customer_name:
        return {"jira_keys": [], "found": False, "error": None, "data_gaps": []}

    err = _check_schema_ready()
    if err:
        return {"jira_keys": [], "found": False, **err}

    sql = f"""
    SELECT DISTINCT tjl.jira_issue_key
    FROM ZendeskTickets.ticket_jira_links tjl
    INNER JOIN ZendeskTickets.ticket t ON tjl.ticket_id = t.id
    INNER JOIN ZendeskTickets.organization o ON t.organization_id = o.id
    WHERE LOWER(o.name) LIKE LOWER('%{customer_name}%')
    LIMIT 200
    """

    result = _run_query(sql)
    if result.get("error"):
        # ticket_jira_links or organization table may not exist
        return {
            "jira_keys": [],
            "found": False,
            "error": None,
            "data_gaps": ["ticket_jira_links or organization table may not be available"],
        }

    rows = _extract_rows(result)
    keys = [r.get("jira_issue_key", "") for r in rows if r.get("jira_issue_key")]

    return {
        "jira_keys": keys,
        "found": len(keys) > 0,
        "error": None,
        "data_gaps": [],
    }
