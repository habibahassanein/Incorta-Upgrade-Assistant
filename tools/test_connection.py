import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Analytics session cache
#
# Each MCP tool call previously did a fresh login (~0.5–1.5 s of overhead).
# The cache is keyed by (cmc_url, tenant, username); the stored value is the
# session dict returned by login_to_incorta_analytics plus an absolute expiry.
# A short TTL keeps the cache close to the Incorta server's own JSESSIONID
# lifetime. Credentials are never stored — only the resulting tokens.
# ---------------------------------------------------------------------------

_SESSION_TTL_SECONDS = 300  # 5 minutes
_session_cache: dict[tuple[str, str, str], tuple[dict, float]] = {}
_session_cache_lock = threading.Lock()


def _build_headers(session: dict) -> dict:
    """Build standard Incorta API headers from a session dict."""
    cookie = ""
    for key, value in session["session_cookie"].items():
        cookie += f"{key}={value};"

    return {
        "Authorization": f"Bearer {session['authorization']}",
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": session["csrf"],
        "Cookie": cookie,
    }


def derive_incorta_url_from_cmc(cmc_url: str) -> str:
    """Derive the Incorta Analytics base URL from a CMC URL.

    Example: https://cluster.cloud2.incorta.com/cmc → https://cluster.cloud2.incorta.com/incorta
    """
    url = cmc_url.rstrip("/")
    if url.endswith("/cmc"):
        base = url[:-4]
    else:
        base = url
    return f"{base}/incorta"


def login_to_incorta_analytics(incorta_url: str, tenant: str, username: str, password: str) -> dict:
    """Authenticate to Incorta Analytics and return session credentials.

    Same pattern as login_to_incorta() in incorta_tools.py but accepts URL explicitly.
    """
    # Login
    response = requests.post(
        f"{incorta_url}/authservice/login",
        data={"tenant": tenant, "user": username, "pass": password},
        verify=True,
        timeout=60,
    )

    if response.status_code != 200:
        raise RuntimeError(f"Incorta Analytics login failed: {response.status_code} - {response.text[:200]}")

    # Extract session cookies
    id_cookie, login_id = None, None
    for item in response.cookies.items():
        if item[0].startswith("JSESSIONID"):
            id_cookie, login_id = item
            break

    if not id_cookie or not login_id:
        raise RuntimeError("Failed to retrieve session cookies from Incorta Analytics")

    # Get CSRF token
    response = requests.get(
        f"{incorta_url}/service/user/isLoggedIn",
        cookies={id_cookie: login_id},
        verify=True,
        timeout=60,
    )

    if response.status_code != 200 or "XSRF-TOKEN" not in response.cookies:
        raise RuntimeError("Failed to get CSRF token from Incorta Analytics")

    csrf_token = response.cookies["XSRF-TOKEN"]
    authorization = response.json().get("accessToken")

    return {
        "env_url": incorta_url,
        "id_cookie": id_cookie,
        "id": login_id,
        "csrf": csrf_token,
        "authorization": authorization,
        "session_cookie": {id_cookie: login_id, "XSRF-TOKEN": csrf_token},
    }


def get_or_create_session(
    cmc_url: str, tenant: str, username: str, password: str
) -> dict:
    """Return a cached Analytics session, logging in only when needed.

    Keyed by (cmc_url, tenant, username). TTL is _SESSION_TTL_SECONDS.
    """
    key = (cmc_url, tenant, username)
    now = time.time()

    with _session_cache_lock:
        entry = _session_cache.get(key)
        if entry is not None and entry[1] > now:
            return entry[0]

    # Login outside the lock so concurrent requests for different users
    # don't serialize on network I/O. Two simultaneous misses for the
    # same user do a redundant login; the later write overwrites the earlier.
    incorta_url = derive_incorta_url_from_cmc(cmc_url)
    session = login_to_incorta_analytics(incorta_url, tenant, username, password)

    with _session_cache_lock:
        _session_cache[key] = (session, now + _SESSION_TTL_SECONDS)
    return session


def invalidate_session(cmc_url: str, tenant: str, username: str) -> None:
    """Drop the cached session for this user. Call after a 401/403."""
    key = (cmc_url, tenant, username)
    with _session_cache_lock:
        _session_cache.pop(key, None)


def get_all_datasources(session: dict) -> tuple[list[dict], list[dict]]:
    """Fetch all datasources from the Incorta Analytics instance.

    Returns (testable, skipped) where testable have supportsTestQuery=true.
    Handles pagination by incrementing pageNumber until no more results.
    """
    headers = _build_headers(session)
    url = session["env_url"]
    all_datasources = []
    page = 0

    while True:
        response = requests.get(
            f"{url}/service/datasource/getDataSources",
            params={"pageNumber": page, "pageSize": -1},
            headers=headers,
            verify=False,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        ds_list = data.get("dataSources", [])
        if not ds_list:
            break
        all_datasources.extend(ds_list)
        # pageSize=-1 should return all; break after first page
        # If it paginates, the loop handles it
        if data.get("pageSize", -1) == -1:
            break
        page += 1

    testable = [ds for ds in all_datasources if ds.get("supportsTestQuery", False)]
    skipped = [ds for ds in all_datasources if not ds.get("supportsTestQuery", False)]

    return testable, skipped


def test_single_connection(session: dict, datasource_id: int, datasource_name: str) -> dict:
    """Test a single datasource connection.

    Returns dict with name, id, success (bool), message, http_status (int | None).
    http_status is only set when we received an HTTP response; it's None for
    transport errors (timeout, connection error, etc.).
    """
    headers = _build_headers(session)
    url = session["env_url"]

    # Remove Content-Type: application/json — the API expects form-encoded data
    form_headers = {k: v for k, v in headers.items() if k != "Content-Type"}

    http_status: int | None = None
    try:
        response = requests.post(
            f"{url}/service/datasource/testConnection",
            headers=form_headers,
            data={"id": datasource_id, "name": datasource_name},
            verify=False,
            timeout=15,
        )
        http_status = response.status_code

        if response.status_code == 200:
            result = response.json() if response.text else {}
            # Interpret response — the API may return various formats
            if isinstance(result, dict):
                success = result.get("success", True)
                message = result.get("message", "Connection successful")
            elif isinstance(result, bool):
                success = result
                message = "Connection successful" if success else "Connection failed"
            else:
                success = True
                message = "Connection successful"
        else:
            success = False
            message = f"HTTP {response.status_code}: {response.text[:200]}"

    except requests.exceptions.Timeout:
        success = False
        message = "Connection timed out (15s)"
    except requests.exceptions.ConnectionError as e:
        success = False
        message = f"Connection error: {str(e)[:200]}"
    except Exception as e:
        success = False
        message = f"Error: {str(e)[:200]}"

    return {
        "name": datasource_name,
        "id": datasource_id,
        "success": success,
        "message": message,
        "http_status": http_status,
    }


# ---------------------------------------------------------------------------
# Datasource-type label
# ---------------------------------------------------------------------------

def _datasource_type(ds: dict) -> str:
    """Best-effort type label for display."""
    return ds.get("subType") or ds.get("type") or "unknown"


def _paginate(items: list, page: int | None, page_size: int | None) -> tuple[list, dict]:
    """Slice items for (page, page_size); return (slice, metadata).

    If page_size is None/<=0, returns the full list with total_pages=1 and
    has_more=False. Otherwise pages are 1-based; an out-of-range page
    returns an empty slice but accurate metadata.

    Caller is responsible for sorting `items` deterministically before
    calling so page boundaries are stable across calls.
    """
    total_items = len(items)
    if not page_size or page_size <= 0:
        return items, {
            "page": 1,
            "page_size": total_items,
            "total_items": total_items,
            "total_pages": 1 if total_items else 0,
            "has_more": False,
            "next_page": None,
        }

    page = max(1, int(page or 1))
    total_pages = (total_items + page_size - 1) // page_size if total_items else 0
    start = (page - 1) * page_size
    end = start + page_size
    sliced = items[start:end]
    has_more = end < total_items
    return sliced, {
        "page": page,
        "page_size": page_size,
        "total_items": total_items,
        "total_pages": total_pages,
        "has_more": has_more,
        "next_page": page + 1 if has_more else None,
    }


def _fetch_with_auth_retry(cmc_url, tenant, username, password):
    """Fetch datasource list; retry once with a fresh session on 401/403."""
    session = get_or_create_session(cmc_url, tenant, username, password)
    try:
        testable, skipped = get_all_datasources(session)
    except requests.exceptions.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (401, 403):
            logger.info("Datasource list returned %s — invalidating session and retrying", status)
            invalidate_session(cmc_url, tenant, username)
            session = get_or_create_session(cmc_url, tenant, username, password)
            testable, skipped = get_all_datasources(session)
        else:
            raise
    return session, testable, skipped


def list_datasources(
    cmc_url: str,
    tenant: str,
    username: str,
    password: str,
    page: int | None = None,
    page_size: int | None = None,
) -> dict:
    """List datasources on the cluster without testing them.

    With no page/page_size, returns every datasource. With page/page_size,
    returns a deterministic slice (sorted by id) with pagination metadata
    so the caller can request the next page.
    """
    try:
        session, testable, skipped = _fetch_with_auth_retry(
            cmc_url, tenant, username, password
        )
    except Exception as e:
        return {"error": f"Failed to fetch datasources: {str(e)}"}

    def _entry(ds, supports_test):
        return {
            "id": ds["id"],
            "name": ds["name"],
            "type": _datasource_type(ds),
            "category": ds.get("category", "unknown"),
            "supports_test": supports_test,
        }

    testable_entries = [_entry(ds, True) for ds in testable]
    skipped_entries = [_entry(ds, False) for ds in skipped]
    # Sort deterministically by id so pages are stable across calls.
    datasources = sorted(testable_entries + skipped_entries, key=lambda e: e["id"])

    page_slice, pagination = _paginate(datasources, page, page_size)

    return {
        "incorta_url": session["env_url"],
        "total": len(datasources),
        "testable": len(testable_entries),
        "skipped": len(skipped_entries),
        "pagination": pagination,
        "datasources": page_slice,
        "hint": (
            "Enumerate every datasource in this list for the user — do NOT summarize "
            "by count or type. Show name, id, type, and category for each entry, "
            "then ask which ones to test. Call test_datasource_connections again "
            "with datasource_ids set to their choice. "
            "If pagination.has_more is true, additional pages exist; request them "
            "with page=pagination.next_page if the user wants to browse further."
        ),
    }


def test_connections(
    cmc_url: str,
    tenant: str,
    username: str,
    password: str,
    datasource_ids: list[int] | None = None,
    page: int | None = None,
    page_size: int | None = None,
) -> dict:
    """Test datasource connections with optional id filtering and paging.

    If datasource_ids is set, only those IDs are tested. Otherwise all
    testable datasources are tested (backward-compatible default).

    When page_size is set, only that page of the selection is tested —
    results include pagination metadata so the caller can request the
    next page. Pages are sorted by id for stability.

    Automatically retries a fresh login once if any test returns 401/403.
    """
    try:
        session, testable, skipped = _fetch_with_auth_retry(
            cmc_url, tenant, username, password
        )
    except Exception as e:
        return {"error": f"Failed to fetch datasources: {str(e)}"}

    incorta_url = session["env_url"]
    total_on_cluster = len(testable) + len(skipped)

    # Apply id filter, if given.
    selected = testable
    id_filter_applied = False
    if datasource_ids:
        id_set = set(datasource_ids)
        selected = [ds for ds in selected if ds["id"] in id_set]
        id_filter_applied = True

    # Detect IDs the caller asked for that aren't testable (either not on
    # the cluster or don't support testConnection).
    requested_but_not_tested: list[int] = []
    if id_filter_applied:
        testable_ids = {ds["id"] for ds in testable}
        selected_ids = {ds["id"] for ds in selected}
        requested_but_not_tested = sorted(
            (did for did in datasource_ids if did not in selected_ids),
            key=lambda x: (x not in testable_ids, x),
        )

    # Deterministic sort, then paginate. The full filtered set's size is
    # surfaced via pagination.total_items so the caller knows what's left.
    selected = sorted(selected, key=lambda ds: ds["id"])
    selected, pagination = _paginate(selected, page, page_size)

    def _run_batch(session_for_batch, datasources_to_run):
        out = []

        def _test_ds(ds):
            ds_id = ds["id"]
            ds_name = ds["name"]
            logger.info("Testing connection: %s (id=%s)", ds_name, ds_id)
            r = test_single_connection(session_for_batch, ds_id, ds_name)
            r["type"] = _datasource_type(ds)
            r["category"] = ds.get("category", "unknown")
            return r

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(_test_ds, ds): ds for ds in datasources_to_run}
            for future in as_completed(futures):
                out.append(future.result())
        return out

    results = _run_batch(session, selected)

    # If any test came back with an auth failure, the cached session likely
    # expired mid-run. Drop the cache, log in fresh, and retry just those.
    auth_failed_ids = {
        r["id"] for r in results if r.get("http_status") in (401, 403)
    }
    if auth_failed_ids:
        logger.info(
            "Retrying %d datasource(s) after auth failure with fresh session",
            len(auth_failed_ids),
        )
        invalidate_session(cmc_url, tenant, username)
        fresh_session = get_or_create_session(cmc_url, tenant, username, password)
        retry_ds = [ds for ds in selected if ds["id"] in auth_failed_ids]
        retry_results = _run_batch(fresh_session, retry_ds)
        # Replace auth-failed entries with retry outcomes.
        results = [r for r in results if r["id"] not in auth_failed_ids] + retry_results

    passed = sum(1 for r in results if r["success"])
    failed = sum(1 for r in results if not r["success"])
    failed_names = [r["name"] for r in results if not r["success"]]
    skipped_names = [ds["name"] for ds in skipped]

    summary_parts = [f"{passed}/{len(selected)} datasource connections passed."]
    if failed:
        summary_parts.append(f"{failed} failed: {', '.join(failed_names)}.")
    if not datasource_ids and skipped:
        # Only surface the global skipped list when the user didn't pick
        # specific ids; otherwise the list is noise.
        summary_parts.append(
            f"{len(skipped)} skipped (no test support): {', '.join(skipped_names)}."
        )
    if requested_but_not_tested:
        summary_parts.append(
            f"{len(requested_but_not_tested)} requested id(s) not tested "
            f"(unknown or no test support): {requested_but_not_tested}."
        )

    if pagination["has_more"]:
        summary_parts.append(
            f"Page {pagination['page']} of {pagination['total_pages']}; "
            f"call again with page={pagination['next_page']} for the next batch."
        )

    return {
        "incorta_url": incorta_url,
        "total_on_cluster": total_on_cluster,
        "tested": len(selected),
        "skipped": len(skipped) if not datasource_ids else 0,
        "passed": passed,
        "failed": failed,
        "datasource_ids_requested": list(datasource_ids) if datasource_ids else None,
        "requested_but_not_tested": requested_but_not_tested or None,
        "pagination": pagination,
        "results": results,
        "skipped_datasources": skipped_names if not datasource_ids else [],
        "summary": " ".join(summary_parts),
    }
