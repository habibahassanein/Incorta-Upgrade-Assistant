from __future__ import annotations
"""
Cloud Portal API Client.

Authentication: OAuth 2.0 Authorization Code + PKCE flow via the
`cloud_portal_connect` MCP tool. After the user completes login through
their browser, the JWT is cached per-user on the server at:
    data/tokens/{email}.json

Token refresh is handled automatically — the client tries the refresh
token before requiring a full re-login.
"""

import base64
import hashlib
import json
import os
import secrets
import time
from pathlib import Path
from urllib.parse import urlencode, urlparse

import jwt
import requests
from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Environment presets — staging vs production
# ---------------------------------------------------------------------------
ENVIRONMENTS = {
    "staging": {
        "portal_url": "https://cloudstaging.incortalabs.com",
        "auth0_domain": "auth-staging.incortalabs.com",
        # Cloud Admin Portal client ID — used for cp- API access (search_instances etc.)
        # NOT the Cloud Portal client ID (1H6oWlDKORKc6BmiYWSjECS8Zq6XesV8)
        "auth0_client_id": "0jXCrcpFe6PDm6sIMxDi7hunFCWeRLpt",
        "cmc_domain": "cloudstaging.incortalabs.com",
    },
    "production": {
        "portal_url": "https://cloud.incorta.com",
        "auth0_domain": "auth.incorta.com",
        "auth0_client_id": "NoUnC0eTxnqwc4I7hwFMp7PANP8Uju1Y",
        "cmc_domain": "cloud2.incorta.com",  # also matches cloud4.incorta.com
    },
}

AUTH0_SCOPES = (
    "openid profile email "
    "read:cluster create:cluster update:cluster delete:cluster manage:cluster "
    "offline_access"
)

# Auth0 / Cloud Portal config is resolved dynamically per request
# based on the CMC URL (staging vs production). See get_auth0_config().
# Override with AUTH0_DOMAIN, AUTH0_CLIENT_ID, CLOUD_PORTAL_URL env vars only
# if you need to force a specific environment.

# Directory for per-user token files
TOKENS_DIR = Path(os.getenv("TOKENS_DIR", "data/tokens"))


def _detect_environment_from_cmc_url(cmc_url: str) -> str:
    """Detect staging vs production from CMC URL.

    Production domains: cloud2.incorta.com, cloud4.incorta.com (and any future cloud*.incorta.com)
    Staging domain:     cloudstaging.incortalabs.com
    """
    if "cloud2.incorta.com" in cmc_url or "cloud4.incorta.com" in cmc_url:
        return "production"
    if "cloudstaging.incortalabs.com" in cmc_url:
        return "staging"
    return os.getenv("INCORTA_ENV", "staging")


def get_auth0_config(cmc_url: str = "") -> dict:
    """Get Auth0 config based on detected environment.

    Priority: explicit env var override > auto-detected preset from CMC URL.
    The old code used os.getenv("AUTH0_DOMAIN", "auth-staging...") which always
    returned the staging default, overriding the production preset even for
    cloud2.incorta.com URLs. Now we only override if the env var is explicitly set.
    """
    env = _detect_environment_from_cmc_url(cmc_url)
    preset = ENVIRONMENTS.get(env, ENVIRONMENTS["staging"])
    return {
        "domain": os.environ.get("AUTH0_DOMAIN") or preset["auth0_domain"],
        "client_id": os.environ.get("AUTH0_CLIENT_ID") or preset["auth0_client_id"],
        "audience": os.environ.get("AUTH0_AUDIENCE") or "https://cloud.server/api/",
        "portal_url": os.environ.get("CLOUD_PORTAL_URL") or preset["portal_url"],
    }


def infer_cloud_cluster_name(cmc_url: str) -> str:
    """Infer Cloud Portal cluster name from CMC URL subdomain."""
    if not cmc_url:
        return ""
    parsed = urlparse(cmc_url)
    hostname = parsed.hostname or ""
    parts = hostname.split(".")
    if len(parts) >= 2:
        return parts[0]
    return ""


# ---------------------------------------------------------------------------
# Per-user token storage helpers
# ---------------------------------------------------------------------------

def _token_path(email: str) -> Path:
    """Return the path for a user's token file."""
    # Sanitize email for use as filename
    safe_email = email.lower().replace("/", "_").replace("\\", "_")
    TOKENS_DIR.mkdir(parents=True, exist_ok=True)
    return TOKENS_DIR / f"{safe_email}.json"


def save_token(email: str, access_token: str, refresh_token: str = None, claims: dict = None):
    """Save a JWT and refresh token for a specific user."""
    claims = claims or {}
    exp = claims.get("exp")
    user_id = claims.get("https://namespace/uuid") or claims.get("sub")

    data = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "email": email,
        "user_id": user_id,
        "exp": exp,
        "cached_at": int(time.time()),
    }
    _token_path(email).write_text(json.dumps(data, indent=2))


def load_token(email: str) -> dict | None:
    """Load cached token data for a user. Returns None if missing or unreadable."""
    path = _token_path(email)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def delete_token(email: str):
    """Delete cached token for a user."""
    path = _token_path(email)
    if path.exists():
        path.unlink()


def is_token_expired(access_token: str) -> bool:
    """Check if a JWT access token is expired (with 5-min buffer)."""
    try:
        claims = jwt.decode(access_token, options={"verify_signature": False})
        exp = claims.get("exp")
        if exp is None:
            return False
        return time.time() > (exp - 300)  # 5-min buffer
    except jwt.DecodeError:
        return True


def refresh_access_token(email: str, cmc_url: str = "") -> str | None:
    """
    Attempt silent refresh using the stored refresh token.

    Returns new access token string on success, None on failure.
    Deletes the token file if the refresh token is also expired.
    """
    data = load_token(email)
    if not data:
        return None

    refresh_token = data.get("refresh_token")
    if not refresh_token:
        return None

    config = get_auth0_config(cmc_url)

    try:
        response = requests.post(
            f"https://{config['domain']}/oauth/token",
            json={
                "grant_type": "refresh_token",
                "client_id": config["client_id"],
                "refresh_token": refresh_token,
            },
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        if response.status_code != 200:
            # Refresh token expired or revoked — clear stored token
            delete_token(email)
            return None

        resp_data = response.json()
        new_access = resp_data.get("access_token")
        new_refresh = resp_data.get("refresh_token", refresh_token)

        if not new_access:
            return None

        try:
            new_claims = jwt.decode(new_access, options={"verify_signature": False})
        except jwt.DecodeError:
            new_claims = {}

        save_token(email, new_access, new_refresh, new_claims)
        return new_access

    except Exception:
        return None


def get_valid_token(email: str, cmc_url: str = "") -> str | None:
    """
    Get a valid access token for a user.

    1. Check cached token — return if still valid.
    2. Try silent refresh if expired.
    3. Return None if full re-auth is needed.
    """
    data = load_token(email)
    if not data:
        return None

    access_token = data.get("access_token")
    if not access_token:
        return None

    if not is_token_expired(access_token):
        return access_token

    # Try silent refresh
    return refresh_access_token(email, cmc_url)


# ---------------------------------------------------------------------------
# PKCE helpers
# ---------------------------------------------------------------------------

def generate_code_verifier() -> str:
    return secrets.token_urlsafe(64)[:128]


def generate_code_challenge(code_verifier: str) -> str:
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


# ---------------------------------------------------------------------------
# OAuth URL builder
# ---------------------------------------------------------------------------

def build_authorize_url(redirect_uri: str, cmc_url: str = "") -> dict:
    """
    Build an OAuth Authorization Code + PKCE authorize URL.

    Returns dict with:
        - authorize_url: URL the user must open in their browser
        - state: CSRF state token (caller must store and validate)
        - code_verifier: PKCE code verifier (for token exchange)
        - redirect_uri: the redirect URI used
    """
    config = get_auth0_config(cmc_url)
    state = secrets.token_urlsafe(32)
    code_verifier = generate_code_verifier()
    code_challenge = generate_code_challenge(code_verifier)

    params = {
        "response_type": "code",
        "client_id": config["client_id"],
        "redirect_uri": redirect_uri,
        "audience": config["audience"],
        "scope": AUTH0_SCOPES,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }

    authorize_url = f"https://{config['domain']}/authorize?{urlencode(params)}"
    return {
        "authorize_url": authorize_url,
        "state": state,
        "code_verifier": code_verifier,
        "redirect_uri": redirect_uri,
        "config": config,
    }


def exchange_code_for_token(
    code: str,
    redirect_uri: str,
    code_verifier: str,
    cmc_url: str = "",
) -> dict:
    """
    Exchange an authorization code for tokens (access + refresh).

    Returns the full token response dict from Auth0.
    Raises RuntimeError on failure.
    """
    config = get_auth0_config(cmc_url)

    response = requests.post(
        f"https://{config['domain']}/oauth/token",
        json={
            "grant_type": "authorization_code",
            "client_id": config["client_id"],
            "code": code,
            "redirect_uri": redirect_uri,
            "code_verifier": code_verifier,
        },
        headers={"Content-Type": "application/json"},
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Token exchange failed (HTTP {response.status_code}): {response.text}"
        )

    return response.json()


# ---------------------------------------------------------------------------
# CloudPortalClient — API methods
# ---------------------------------------------------------------------------

class CloudPortalClient:
    """
    Client for the Cloud Portal API.

    Accepts a bearer_token directly (obtained from the per-user token cache).
    All Cloud Portal API calls use this token.
    """

    def __init__(self, bearer_token: str, cmc_url: str = ""):
        config = get_auth0_config(cmc_url)
        portal_url = config["portal_url"]
        self.base_url = f"{portal_url}/api/v2"
        self.bearer_token = bearer_token
        self._user_id = None

        # Decode user_id from claims
        try:
            claims = jwt.decode(bearer_token, options={"verify_signature": False})
            self._user_id = claims.get("https://namespace/uuid") or claims.get("sub")
        except jwt.DecodeError:
            pass

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.bearer_token}",
            "Accept": "application/json",
        }

    def get_user_id(self) -> str:
        if not self._user_id:
            raise RuntimeError(
                "Cannot determine user_id from JWT. "
                "Set CLOUD_PORTAL_USER_ID or re-authenticate via cloud_portal_connect."
            )
        return self._user_id

    @staticmethod
    def _build_cp_base_url(cmc_url: str = "") -> str:
        """Derive cp- prefixed URL from Cloud Portal URL."""
        config = get_auth0_config(cmc_url)
        portal_url = config["portal_url"]
        parsed = urlparse(portal_url)
        cp_hostname = f"cp-{parsed.hostname}"
        return f"{parsed.scheme}://{cp_hostname}/api/v2"

    def search_instances(self, cluster_name: str, cmc_url: str = "") -> dict | None:
        """
        Search for an instance by name.

        Uses the regular Cloud Portal /users/{user_id}/clustersinfo endpoint
        (cloudstaging.incortalabs.com) which works with a standard user token.

        Falls back to the cp- admin endpoint only if the regular endpoint
        doesn't find the cluster (requires elevated permissions).
        """
        # Try regular portal first — works with any authenticated user
        try:
            user_id = self.get_user_id()
            instance = self.find_cluster(user_id, cluster_name)
            if instance:
                return instance
        except Exception:
            pass

        # Fallback: cp- admin endpoint (requires Clusters permission)
        try:
            cp_base = self._build_cp_base_url(cmc_url)
            r = requests.get(
                f"{cp_base}/instances",
                headers=self._headers(),
                params={"search": cluster_name},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            for instance in data.get("instances", []):
                if instance.get("name") == cluster_name:
                    return instance
        except Exception:
            pass

        return None

    def get_clusters_info(self, user_id: str) -> dict:
        r = requests.get(
            f"{self.base_url}/users/{user_id}/clustersinfo",
            headers=self._headers(),
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def get_consumption(self, user_id: str, instance_uuid: str) -> dict:
        r = requests.get(
            f"{self.base_url}/users/{user_id}/instances/{instance_uuid}/consumption",
            headers=self._headers(),
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def get_authorized_users(self, user_id: str, cluster_name: str) -> dict:
        r = requests.get(
            f"{self.base_url}/users/{user_id}/instances/{cluster_name}/autherizedusers",
            headers=self._headers(),
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def get_instance_details(self, user_id: str, cluster_name: str) -> dict:
        r = requests.get(
            f"{self.base_url}/users/{user_id}/instances/{cluster_name}",
            headers=self._headers(),
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def find_cluster(self, user_id: str, cluster_name: str) -> dict | None:
        clusters_info = self.get_clusters_info(user_id)
        for item in clusters_info.get("instances", []):
            instance = item.get("instance", {})
            if instance.get("name") == cluster_name:
                return instance
        return None

    def find_cluster_uuid(self, user_id: str, cluster_name: str) -> str | None:
        instance = self.find_cluster(user_id, cluster_name)
        return instance.get("id") if instance else None
