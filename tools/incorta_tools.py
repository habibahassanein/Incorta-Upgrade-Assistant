

import os
from typing import Dict, Any
import requests

from context.user_context import user_context


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

    Args:
        fetch_schema (bool): Flag to fetch schema details

    Returns:
        dict: Schema details with tables and columns
    """
    login_creds = login_to_incorta()
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

    response = requests.get(url, headers=headers, verify=False)

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

        return {
            "source": "zendesk",
            "schema_name": "ZendeskTickets",
            "tables": tables,
            "table_count": len(tables),
            "note": "Schema compressed for context efficiency. Use SQL queries to access data."
        }
    else:
        return {
            "error": f"Failed to fetch Zendesk schema: {response.status_code} - {response.text}"
        }


def get_jira_schema(arguments: Dict[str, Any]) -> dict:
    """
    Get Jira schema details from Incorta.

    Args:
        fetch_schema (bool): Flag to fetch schema details

    Returns:
        dict: Schema details with tables and columns
    """
    login_creds = login_to_incorta()
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

    response = requests.get(url, headers=headers, verify=False)

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

        return {
            "source": "jira",
            "schema_name": "Jira_F",
            "tables": tables,
            "table_count": len(tables),
            "note": "Schema compressed for context efficiency. Use SQL queries to access data."
        }
    else:
        return {
            "error": f"Failed to fetch Jira schema: {response.status_code} - {response.text}"
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

    response = requests.post(url, headers=headers, json=params, verify=False)

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

    response = requests.post(url, headers=headers, json=params, verify=False)

    if response.status_code == 200:
        return {
            "source": "jira",
            "data": response.json()
        }
    else:
        return {
            "error": f"Failed to query Jira: {response.status_code} - {response.text}"
        }
