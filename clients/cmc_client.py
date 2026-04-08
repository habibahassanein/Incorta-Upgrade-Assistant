"""
CMC API client.

Credentials are passed via constructor arguments (sourced from request headers).
No disk caching — authentication is per-request and stateless.
"""

import base64
import os

import requests
from dotenv import load_dotenv

load_dotenv()

VERIFY_SSL = os.getenv("VERIFY_SSL", "true").lower() in ("1", "true", "yes")


class CMCClient:
    def __init__(self, url=None, user=None, password=None, cluster_name=None):
        self.url = (url or os.getenv("CMC_URL", "")).rstrip("/")
        self.user = user or os.getenv("CMC_USER", "")
        self.password = password or os.getenv("CMC_PASSWORD", "")
        self.cluster_name = cluster_name or os.getenv("CMC_CLUSTER_NAME", "")
        self.token = None

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def login(self):
        """Login to CMC with Basic Auth and return JWT token."""
        if not self.url:
            raise RuntimeError(
                "CMC URL not set. "
                "Ensure 'cmc-url' is included in your MCP client headers."
            )
        if not self.user or not self.password:
            raise RuntimeError(
                "CMC credentials not set. "
                "Ensure 'cmc-user' and 'cmc-password' are included in your MCP client headers."
            )

        credentials_b64 = base64.b64encode(
            f"{self.user}:{self.password}".encode()
        ).decode()

        r = requests.get(
            f"{self.url}/api/v1/auth/login",
            headers={
                "Accept": "application/json",
                "Authorization": f"Basic {credentials_b64}",
            },
            verify=VERIFY_SSL,
            timeout=30,
        )

        if r.status_code not in (200, 201):
            raise RuntimeError(
                f"CMC login failed (HTTP {r.status_code}). "
                f"Check cmc-url, cmc-user, and cmc-password. "
                f"Response: {r.text[:200]!r}"
            )

        data = self._parse_json_response(r, "CMC login")
        self.token = data.get("token")
        if not self.token:
            raise RuntimeError("CMC login response did not contain a token.")

        return self.token

    def _headers(self):
        """Get auth headers, logging in if no token yet."""
        if not self.token:
            self.login()
        return {
            "Accept": "application/json",
            "Authorization": f"bearer {self.token}",
        }

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_json_response(self, response, context="CMC API"):
        """Parse JSON response with clear error messages."""
        body = response.text.strip()
        if not body:
            raise RuntimeError(
                f"{context} returned an empty response (HTTP {response.status_code}). "
                f"Check that CMC URL '{self.url}' is correct and the CMC service is running."
            )
        if body.startswith("<!") or body.startswith("<html") or body.startswith("<HTML"):
            raise RuntimeError(
                f"{context} returned HTML instead of JSON (HTTP {response.status_code}). "
                f"This usually means the CMC session expired or the URL is redirecting to a login page."
            )
        try:
            return response.json()
        except ValueError as e:
            raise RuntimeError(
                f"{context} returned invalid JSON (HTTP {response.status_code}): {e}. "
                f"Response preview: {body[:200]!r}"
            )

    # ------------------------------------------------------------------
    # API methods
    # ------------------------------------------------------------------

    def get_cluster(self, cluster_name):
        """Get full cluster details."""
        r = requests.get(
            f"{self.url}/api/v1/clusters/{cluster_name}",
            headers=self._headers(),
            verify=VERIFY_SSL,
            timeout=60,
        )
        r.raise_for_status()
        return self._parse_json_response(r, f"CMC get_cluster '{cluster_name}'")

    def get_clusters_brief(self):
        """List all clusters."""
        r = requests.get(
            f"{self.url}/api/v1/clusters/brief",
            headers=self._headers(),
            verify=VERIFY_SSL,
            timeout=60,
        )
        r.raise_for_status()
        return self._parse_json_response(r, "CMC get_clusters_brief")
