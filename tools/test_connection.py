import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logger = logging.getLogger(__name__)


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

    Returns dict with name, id, success (bool), message.
    """
    headers = _build_headers(session)
    url = session["env_url"]

    # Remove Content-Type: application/json — the API expects form-encoded data
    form_headers = {k: v for k, v in headers.items() if k != "Content-Type"}

    try:
        response = requests.post(
            f"{url}/service/datasource/testConnection",
            headers=form_headers,
            data={"id": datasource_id, "name": datasource_name},
            verify=False,
            timeout=15,
        )

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
    }


def test_all_connections(session: dict) -> dict:
    """Test all datasource connections using a cached Incorta Analytics session.

    Returns a report dict with per-datasource results.
    """
    incorta_url = session["env_url"]

    # Fetch all datasources
    try:
        testable, skipped = get_all_datasources(session)
    except Exception as e:
        return {"error": f"Failed to fetch datasources: {str(e)}"}

    total = len(testable) + len(skipped)

    # Test connections concurrently (max 10 threads) to stay within MCP timeout
    results = []

    def _test_ds(ds):
        ds_id = ds["id"]
        ds_name = ds["name"]
        logger.info(f"Testing connection: {ds_name} (id={ds_id})")
        r = test_single_connection(session, ds_id, ds_name)
        r["type"] = ds.get("subType", ds.get("type", "unknown"))
        r["category"] = ds.get("category", "unknown")
        return r

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_test_ds, ds): ds for ds in testable}
        for future in as_completed(futures):
            results.append(future.result())

    passed = sum(1 for r in results if r["success"])
    failed = sum(1 for r in results if not r["success"])

    # Build summary
    failed_names = [r["name"] for r in results if not r["success"]]
    skipped_names = [ds["name"] for ds in skipped]

    summary_parts = [f"{passed}/{len(testable)} datasource connections passed."]
    if failed:
        summary_parts.append(f"{failed} failed: {', '.join(failed_names)}.")
    if skipped:
        summary_parts.append(f"{len(skipped)} skipped (no test support): {', '.join(skipped_names)}.")

    return {
        "incorta_url": incorta_url,
        "total": total,
        "tested": len(testable),
        "skipped": len(skipped),
        "passed": passed,
        "failed": failed,
        "results": results,
        "skipped_datasources": skipped_names,
        "summary": " ".join(summary_parts),
    }
