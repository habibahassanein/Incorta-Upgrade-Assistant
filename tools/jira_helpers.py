"""
Jira Helper Functions for Bug Analysis

Encapsulates specialized queries for customer bug discovery and fix version analysis.
Each function is schema-aware: it checks `bug_analysis_ready` from the cached
schema before running any SQL, and returns structured errors with `data_gaps`
when required tables are missing.

No user writes SQL — all queries are generated internally.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from tools.incorta_tools import get_jira_schema, query_jira

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _check_schema_ready() -> dict | None:
    """Return an error dict if schema is not ready, else None."""
    schema = get_jira_schema({"fetch_schema": True})
    if schema.get("error") or not schema.get("bug_analysis_ready"):
        missing = schema.get("missing_bug_tables", [])
        return {
            "error": f"Jira bug analysis unavailable — missing tables: {missing}",
            "data_gaps": missing,
        }
    return None


def _run_query(sql: str) -> dict:
    """Execute a Spark SQL query and return the raw result dict."""
    return query_jira({"spark_sql": sql})


def _extract_rows(result: dict) -> List[Dict[str, Any]]:
    """Extract rows from a query_jira response as list-of-dicts.

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
        if isinstance(data.get("data"), list):
            return data["data"]
        return []

    # Format 2: already a list of dicts
    if isinstance(data, list):
        return data

    return []


# ---------------------------------------------------------------------------
# Helper 1: Customer Bugs with Fix Versions
# ---------------------------------------------------------------------------


def get_customer_bugs(customer_name: str, from_version: str = "") -> dict:
    """Retrieve all bugs reported by a customer with their fix version info.

    Args:
        customer_name: Customer name to filter by (uses LIKE matching).
        from_version: Current version to filter by affected version (optional).
                      When provided, only returns bugs affecting this version.

    Returns:
        {
            "bugs": [{ key, summary, status, priority, issue_type, customer,
                       fix_version, created, updated, resolution, labels,
                       description, affected_version }],
            "total_bugs": int,
            "found": bool,
            "error": str | None,
            "data_gaps": []
        }
    """
    err = _check_schema_ready()
    if err:
        return {"bugs": [], "total_bugs": 0, "found": False, **err}

    # Build optional affected version join/filter
    affected_join = ""
    affected_select = ""
    affected_where = ""
    if from_version:
        affected_select = ",\n        iav.Name AS affected_version"
        affected_join = "\n    JOIN Jira_F.IssueAffectedVersions iav ON i.Key = iav.IssueKey"
        affected_where = f"\n      AND iav.Name LIKE '{from_version}%'"

    sql = f"""
    SELECT
        i.Key,
        i.Summary,
        i.StatusName,
        i.PriorityName,
        i.IssueTypeName,
        i.Customer,
        i.Created,
        i.Updated,
        i.ResolutionName,
        i.Labels,
        SUBSTRING(i.Description, 1, 500) AS description,
        ifv.Name AS fix_version{affected_select}
    FROM Jira_F.Issues i
    LEFT JOIN Jira_F.IssueFixVersions ifv ON i.Key = ifv.IssueKey{affected_join}
    WHERE i.Customer LIKE '%{customer_name}%'
      AND i.IssueTypeName = 'Bug'{affected_where}
    ORDER BY i.PriorityName ASC
    LIMIT 200
    """

    result = _run_query(sql)
    if result.get("error"):
        return {
            "bugs": [],
            "total_bugs": 0,
            "found": False,
            "error": result["error"],
            "data_gaps": [],
        }

    rows = _extract_rows(result)
    bugs = []
    for r in rows:
        bug = {
            "key": r.get("Key", ""),
            "summary": r.get("Summary", ""),
            "status": r.get("StatusName", ""),
            "priority": r.get("PriorityName", ""),
            "issue_type": r.get("IssueTypeName", ""),
            "customer": r.get("Customer", ""),
            "fix_version": r.get("fix_version"),
            "created": r.get("Created", ""),
            "updated": r.get("Updated", ""),
            "resolution": r.get("ResolutionName", ""),
            "labels": r.get("Labels", ""),
            "description": r.get("description", ""),
        }
        if from_version:
            bug["affected_version"] = r.get("affected_version", "")
        bugs.append(bug)

    return {
        "bugs": bugs,
        "total_bugs": len(bugs),
        "found": len(bugs) > 0,
        "error": None,
        "data_gaps": [],
    }


# ---------------------------------------------------------------------------
# Helper 2: Linked Jira Issues (from Zendesk tickets)
# ---------------------------------------------------------------------------


def get_linked_jira_issues(jira_keys: List[str]) -> dict:
    """Retrieve specific Jira issues by key with their fix version status.

    Called with Jira keys extracted from Zendesk ticket_jira_links.

    Args:
        jira_keys: List of Jira issue keys (e.g., ['PROD-123', 'PROD-456']).

    Returns:
        {
            "issues": [{ key, summary, status, priority, issue_type,
                         fix_version }],
            "total_issues": int,
            "found": bool,
            "error": str | None,
            "data_gaps": []
        }
    """
    if not jira_keys:
        return {
            "issues": [],
            "total_issues": 0,
            "found": False,
            "error": None,
            "data_gaps": [],
        }

    err = _check_schema_ready()
    if err:
        return {"issues": [], "total_issues": 0, "found": False, **err}

    keys_in_clause = ", ".join(f"'{k}'" for k in jira_keys)

    sql = f"""
    SELECT
        i.Key,
        i.Summary,
        i.StatusName,
        i.PriorityName,
        i.IssueTypeName,
        i.Customer,
        i.Created,
        i.Updated,
        i.ResolutionName,
        i.Labels,
        SUBSTRING(i.Description, 1, 500) AS description,
        ifv.Name AS fix_version
    FROM Jira_F.Issues i
    LEFT JOIN Jira_F.IssueFixVersions ifv ON i.Key = ifv.IssueKey
    WHERE i.Key IN ({keys_in_clause})
    ORDER BY i.PriorityName ASC
    """

    result = _run_query(sql)
    if result.get("error"):
        return {
            "issues": [],
            "total_issues": 0,
            "found": False,
            "error": result["error"],
            "data_gaps": [],
        }

    rows = _extract_rows(result)
    issues = [
        {
            "key": r.get("Key", ""),
            "summary": r.get("Summary", ""),
            "status": r.get("StatusName", ""),
            "priority": r.get("PriorityName", ""),
            "issue_type": r.get("IssueTypeName", ""),
            "customer": r.get("Customer", ""),
            "fix_version": r.get("fix_version"),
            "created": r.get("Created", ""),
            "updated": r.get("Updated", ""),
            "resolution": r.get("ResolutionName", ""),
            "labels": r.get("Labels", ""),
            "description": r.get("description", ""),
        }
        for r in rows
    ]
    return {
        "issues": issues,
        "total_issues": len(issues),
        "found": len(issues) > 0,
        "error": None,
        "data_gaps": [],
    }


# ---------------------------------------------------------------------------
# Helper 3: Upgrade Path Bugs (from other customers)
# ---------------------------------------------------------------------------


def get_upgrade_path_bugs(from_version: str, to_version: str) -> dict:
    """Find bugs from other customers that affect versions in the upgrade path.

    Queries IssueAffectedVersions to find bugs affecting versions between
    from_version and to_version, providing a broader risk view.

    Args:
        from_version: Current Incorta version.
        to_version: Target Incorta version.

    Returns:
        {
            "bugs": [{ key, summary, status, priority, affected_version,
                       fix_version }],
            "total_bugs": int,
            "found": bool,
            "error": str | None,
            "data_gaps": []
        }
    """
    err = _check_schema_ready()
    if err:
        return {"bugs": [], "total_bugs": 0, "found": False, **err}

    # Build version filter — need at least one version to filter on
    version_conditions = []
    if from_version:
        version_conditions.append(f"iav.Name LIKE '{from_version}%'")
    if to_version:
        version_conditions.append(f"iav.Name LIKE '{to_version}%'")
    if not version_conditions:
        return {"bugs": [], "total_bugs": 0, "found": False,
                "error": None, "data_gaps": []}
    version_filter = " OR ".join(version_conditions)

    sql = f"""
    SELECT
        i.Key,
        i.Summary,
        i.StatusName,
        i.PriorityName,
        i.Created,
        i.Updated,
        i.ResolutionName,
        i.Labels,
        SUBSTRING(i.Description, 1, 500) AS description,
        iav.Name AS affected_version,
        ifv.Name AS fix_version
    FROM Jira_F.Issues i
    JOIN Jira_F.IssueAffectedVersions iav ON i.Key = iav.IssueKey
    LEFT JOIN Jira_F.IssueFixVersions ifv ON i.Key = ifv.IssueKey
    WHERE i.IssueTypeName = 'Bug'
      AND ({version_filter})
    ORDER BY i.PriorityName ASC
    LIMIT 200
    """

    result = _run_query(sql)
    if result.get("error"):
        return {
            "bugs": [],
            "total_bugs": 0,
            "found": False,
            "error": result["error"],
            "data_gaps": [],
        }

    rows = _extract_rows(result)
    bugs = [
        {
            "key": r.get("Key", ""),
            "summary": r.get("Summary", ""),
            "status": r.get("StatusName", ""),
            "priority": r.get("PriorityName", ""),
            "affected_version": r.get("affected_version", ""),
            "fix_version": r.get("fix_version"),
            "created": r.get("Created", ""),
            "updated": r.get("Updated", ""),
            "resolution": r.get("ResolutionName", ""),
            "labels": r.get("Labels", ""),
            "description": r.get("description", ""),
        }
        for r in rows
    ]
    return {
        "bugs": bugs,
        "total_bugs": len(bugs),
        "found": len(bugs) > 0,
        "error": None,
        "data_gaps": [],
    }


# ---------------------------------------------------------------------------
# Helper 4: Bug Fix Classification
# ---------------------------------------------------------------------------


_WONT_FIX_RESOLUTIONS = frozenset({
    "won't fix", "wontfix", "duplicate", "cannot reproduce", "incomplete",
})


def classify_bug_fix_status(
    bugs: List[Dict[str, Any]], to_version: str
) -> dict:
    """Classify each bug as fixed in target, still open, won't fix, or requires dot release.

    Uses both StatusName and ResolutionName for accurate classification.
    A bug "Closed as Won't Fix" is separated from genuinely fixed bugs.

    Args:
        bugs: List of bug dicts with at least 'key', 'status', 'fix_version',
              and optionally 'resolution'.
        to_version: Target Incorta version.

    Returns:
        {
            "fixed_in_target": [{ key, summary, fix_version }],
            "still_open": [{ key, summary, status }],
            "wont_fix": [{ key, summary, resolution }],
            "requires_later_release": [{ key, summary, fix_version }],
            "summary": { fixed_count, open_count, wont_fix_count,
                         later_release_count, total },
            "error": None,
            "data_gaps": []
        }
    """
    fixed_in_target: List[Dict[str, Any]] = []
    still_open: List[Dict[str, Any]] = []
    wont_fix: List[Dict[str, Any]] = []
    requires_later: List[Dict[str, Any]] = []

    # De-duplicate by key (a bug may appear multiple times with different fix versions)
    seen_keys: dict[str, Dict[str, Any]] = {}
    for bug in bugs:
        key = bug.get("key", "")
        if not key:
            continue
        existing = seen_keys.get(key)
        if existing:
            # Merge fix versions — keep the one matching to_version if any
            if bug.get("fix_version") == to_version:
                seen_keys[key] = bug
            elif existing.get("fix_version") is None and bug.get("fix_version"):
                seen_keys[key] = bug
        else:
            seen_keys[key] = bug

    for bug in seen_keys.values():
        key = bug.get("key", "")
        summary = bug.get("summary", "")
        status = bug.get("status", "")
        fix_version = bug.get("fix_version")
        resolution = (bug.get("resolution") or "").strip()

        # Check for "Won't Fix" / "Duplicate" / "Cannot Reproduce" first
        if resolution.lower() in _WONT_FIX_RESOLUTIONS:
            wont_fix.append({
                "key": key,
                "summary": summary,
                "resolution": resolution,
            })
        elif fix_version and fix_version == to_version:
            fixed_in_target.append({
                "key": key,
                "summary": summary,
                "fix_version": fix_version,
            })
        elif fix_version and _version_is_later(fix_version, to_version):
            requires_later.append({
                "key": key,
                "summary": summary,
                "fix_version": fix_version,
            })
        elif not fix_version or status.lower() not in ("closed", "done", "resolved"):
            still_open.append({
                "key": key,
                "summary": summary,
                "status": status,
            })
        else:
            # Has a fix version that's earlier or equal — resolved
            fixed_in_target.append({
                "key": key,
                "summary": summary,
                "fix_version": fix_version,
            })

    return {
        "fixed_in_target": fixed_in_target,
        "still_open": still_open,
        "wont_fix": wont_fix,
        "requires_later_release": requires_later,
        "summary": {
            "fixed_count": len(fixed_in_target),
            "open_count": len(still_open),
            "wont_fix_count": len(wont_fix),
            "later_release_count": len(requires_later),
            "total": len(seen_keys),
        },
        "error": None,
        "data_gaps": [],
    }


def _version_is_later(version_a: str, version_b: str) -> bool:
    """Return True if version_a is later than version_b.

    Handles Incorta version formats like '2024.1.0', '2024.7.2'.
    Falls back to string comparison if parsing fails.
    """
    try:
        parts_a = [int(x) for x in version_a.split(".")]
        parts_b = [int(x) for x in version_b.split(".")]
        return parts_a > parts_b
    except (ValueError, AttributeError):
        return version_a > version_b
