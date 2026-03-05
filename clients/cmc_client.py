"""
CMC API client with JWT caching.

Authentication priority:
1. Constructor params (user=, password=)
2. Cached token from disk (~/.incorta_cmc_token.json)
3. Environment variables (CMC_USER, CMC_PASSWORD)
4. Error: "Not authenticated — call cmc_login tool"
"""

import os
import json
import time
import base64
from pathlib import Path

import jwt
import requests
from dotenv import load_dotenv

load_dotenv()

CMC_URL = os.getenv("CMC_URL", "").rstrip("/")
CMC_USER = os.getenv("CMC_USER", "")
CMC_PASSWORD = os.getenv("CMC_PASSWORD", "")
VERIFY_SSL = os.getenv("VERIFY_SSL", "true").lower() in ("1", "true", "yes")
CMC_TOKEN_CACHE_PATH = Path(
    os.getenv("CMC_TOKEN_CACHE_PATH", str(Path.home() / ".incorta_cmc_token.json"))
)


class CMCClient:
    def __init__(self, url=None, user=None, password=None, cluster_name=None):
        self.url = url or CMC_URL
        self.user = user or CMC_USER
        self.password = password or CMC_PASSWORD
        self.cluster_name = cluster_name or os.getenv("CMC_CLUSTER_NAME", "")
        self.token = None

        # Try loading cached token from disk first
        if not self.token:
            self._load_cached_token()

    # ------------------------------------------------------------------
    # Token caching
    # ------------------------------------------------------------------

    def _load_cached_token(self):
        """Load token from local cache file if it exists and hasn't expired.

        Also restores url and cluster_name from cache when not set via
        constructor params or environment variables.
        """
        if not CMC_TOKEN_CACHE_PATH.exists():
            return
        try:
            data = json.loads(CMC_TOKEN_CACHE_PATH.read_text())
            cached_url = data.get("url", "")
            # If we have a URL set (from env or constructor), check it matches
            if self.url and cached_url and cached_url != self.url:
                return  # Different CMC instance — don't use this token
            token = data.get("access_token")
            if token and not self._is_token_expired(token):
                self.token = token
                # Restore fields from cache when not provided externally
                if not self.user:
                    self.user = data.get("user", "")
                if not self.url and cached_url:
                    self.url = cached_url
                if not self.cluster_name:
                    self.cluster_name = data.get("cluster_name", "")
        except (json.JSONDecodeError, KeyError):
            pass

    def _save_token(self, token, user=None, cluster_name=None):
        """Cache token to local file with expiry metadata, URL, and cluster name."""
        try:
            # Signature verification skipped: token obtained from CMC over HTTPS.
            claims = jwt.decode(token, options={"verify_signature": False})
            exp = claims.get("exp")
        except jwt.DecodeError:
            exp = None

        cache_data = {
            "access_token": token,
            "user": user or self.user,
            "url": self.url,
            "cluster_name": cluster_name or self.cluster_name,
            "exp": exp,
            "cached_at": int(time.time()),
        }
        CMC_TOKEN_CACHE_PATH.write_text(json.dumps(cache_data, indent=2))

    @staticmethod
    def load_cached_config():
        """Load the cached CMC config (url, cluster_name, user) from disk.

        Returns a dict with keys: url, cluster_name, user, access_token.
        Returns empty dict if no cache exists or token is expired.
        """
        if not CMC_TOKEN_CACHE_PATH.exists():
            return {}
        try:
            data = json.loads(CMC_TOKEN_CACHE_PATH.read_text())
            token = data.get("access_token")
            if token and not CMCClient._is_token_expired(token):
                return data
            return {}
        except (json.JSONDecodeError, KeyError):
            return {}

    @staticmethod
    def _is_token_expired(token):
        """Check if a JWT token is expired (with 5-minute buffer)."""
        try:
            # Signature verification skipped: only reading exp claim.
            claims = jwt.decode(token, options={"verify_signature": False})
            exp = claims.get("exp")
            if exp is None:
                return False  # No expiry claim — assume valid
            return time.time() > (exp - 300)  # 5-min buffer
        except jwt.DecodeError:
            return True  # Can't decode — treat as expired

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def login(self):
        """Login to CMC with Basic Auth, get JWT token, and cache it."""
        if not self.url:
            raise RuntimeError(
                "CMC URL not available. "
                "Please call the **cmc_login** tool to open the login portal, "
                "or set CMC_URL in your .env file."
            )
        if not self.user or not self.password:
            raise RuntimeError(
                "CMC credentials not available. "
                "Please call the **cmc_login** tool to open the login portal, "
                "or set CMC_USER and CMC_PASSWORD environment variables."
            )

        credentials = f"{self.user}:{self.password}"
        credentials_b64 = base64.b64encode(credentials.encode()).decode()

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
                f"Check CMC_URL, CMC_USER, and CMC_PASSWORD. "
                f"Response: {r.text[:200]!r}"
            )

        data = self._parse_json_response(r, "CMC login")
        self.token = data.get("token")
        if not self.token:
            raise RuntimeError("No token in response")

        # Cache the token to disk
        self._save_token(self.token)

        return self.token

    def _headers(self):
        """Get auth headers, auto-logging in if needed."""
        if self.token and self._is_token_expired(self.token):
            # Token expired — try re-login if credentials available
            self.token = None

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
        """Parse JSON response with clear error messages for common failure modes."""
        body = response.text.strip()
        if not body:
            raise RuntimeError(
                f"{context} returned an empty response (HTTP {response.status_code}). "
                f"Check that CMC_URL '{self.url}' is correct and the CMC service is running."
            )
        if body.startswith("<!") or body.startswith("<html") or body.startswith("<HTML"):
            raise RuntimeError(
                f"{context} returned HTML instead of JSON (HTTP {response.status_code}). "
                f"This usually means the CMC session expired or the URL is redirecting to a login page. "
                f"Check CMC_URL, CMC_USER, and CMC_PASSWORD."
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
