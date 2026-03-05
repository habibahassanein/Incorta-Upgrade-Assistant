#!/usr/bin/env python3
"""
Standalone test: Can we use the cp- prefixed Cloud Portal endpoint
to search for instances directly, without needing user_id?

Endpoint: GET https://cp-cloudstaging.incortalabs.com/api/v2/instances?search=CLUSTER-NAME
Auth:     Bearer token (reused from existing cached token)

This script does NOT modify any existing code. It just tests the endpoint.
"""

import json
import sys
from pathlib import Path
from urllib.parse import urlparse

import requests

# Reuse the existing token cache
TOKEN_CACHE_PATH = Path.home() / ".incorta_cloud_token.json"
CLOUD_PORTAL_URL = "https://cloudstaging.incortalabs.com"


def load_cached_token():
    """Load the bearer token from the existing cache file."""
    if not TOKEN_CACHE_PATH.exists():
        print(f"ERROR: No cached token found at {TOKEN_CACHE_PATH}")
        print("Run `python scripts/cloud_login.py` first to authenticate.")
        sys.exit(1)

    data = json.loads(TOKEN_CACHE_PATH.read_text())
    token = data.get("access_token")
    if not token:
        print("ERROR: No access_token in cache file.")
        sys.exit(1)

    print(f"Loaded token from {TOKEN_CACHE_PATH}")
    return token


def build_cp_url():
    """Build the cp- prefixed base URL."""
    parsed = urlparse(CLOUD_PORTAL_URL)
    cp_hostname = f"cp-{parsed.hostname}"
    return f"{parsed.scheme}://{cp_hostname}/api/v2"


def test_search(cluster_name, token):
    """Call the cp- instances search endpoint and print results."""
    cp_base = build_cp_url()
    url = f"{cp_base}/instances"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    params = {"search": cluster_name}

    print(f"\n--- Request ---")
    print(f"GET {url}?search={cluster_name}")
    print(f"Authorization: Bearer <token>")

    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
    except requests.exceptions.ConnectionError as e:
        print(f"\nCONNECTION ERROR: Cannot reach {cp_base}")
        print(f"  {e}")
        return

    print(f"\n--- Response ---")
    print(f"Status: {r.status_code}")
    print(f"Content-Type: {r.headers.get('Content-Type')}")

    if r.status_code == 401:
        print("\nAUTH FAILED: The cp- endpoint rejected the bearer token.")
        print("This means it may require a different audience or auth method.")
        return

    if r.status_code == 403:
        print("\nFORBIDDEN: Token accepted but insufficient permissions.")
        return

    if r.status_code != 200:
        print(f"\nUnexpected status. Body:\n{r.text[:1000]}")
        return

    data = r.json()
    print(f"\nSUCCESS! Response structure:")
    print(json.dumps(data, indent=2, default=str)[:3000])

    # Check if the response has instance data we can use
    if isinstance(data, list):
        print(f"\nReturned {len(data)} instance(s)")
        if data:
            first = data[0]
            print(f"\nFirst instance keys: {list(first.keys())}")
    elif isinstance(data, dict):
        print(f"\nResponse keys: {list(data.keys())}")
        instances = data.get("instances") or data.get("data") or data.get("results")
        if instances:
            print(f"Found {len(instances)} instance(s)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_cp_search.py <CLUSTER_NAME>")
        print("Example: python scripts/test_cp_search.py habibascluster")
        sys.exit(1)

    cluster_name = sys.argv[1]
    token = load_cached_token()
    test_search(cluster_name, token)
