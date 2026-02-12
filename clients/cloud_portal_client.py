"""
Cloud Portal API Client
Provides access to cloud-specific metadata, consumption tracking, and user management.

Authentication: Uses OAuth Authorization Code flow with a local callback server.
When no valid token is available, opens the browser for Cloud Portal login (with MFA),
captures the token automatically via localhost callback, and caches it locally.
"""

import json
import os
import secrets
import time
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs

import jwt
import requests
from dotenv import load_dotenv

load_dotenv()

# Auth0 configuration (defaults for staging, overridable via env)
AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN", "auth-staging.incortalabs.com")
AUTH0_CLIENT_ID = os.getenv("AUTH0_CLIENT_ID", "1H6oWlDKORKc6BmiYWSjECS8Zq6XesV8")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE", "https://cloud.server/api/")
AUTH0_SCOPES = (
    "openid profile email "
    "read:cluster create:cluster update:cluster delete:cluster manage:cluster "
    "offline_access"
)
CALLBACK_PORT = int(os.getenv("AUTH0_CALLBACK_PORT", "8910"))
CLOUD_PORTAL_URL = os.getenv("CLOUD_PORTAL_URL", "https://cloudstaging.incortalabs.com")
TOKEN_CACHE_PATH = Path.home() / ".incorta_cloud_token.json"


class CloudPortalClient:
    def __init__(self, bearer_token=None):
        self.base_url = f"{CLOUD_PORTAL_URL}/api/v2"
        self.bearer_token = bearer_token or os.getenv("CLOUD_PORTAL_TOKEN")
        self.user_id = os.getenv("CLOUD_PORTAL_USER_ID")

        # If no static token provided, try loading from cache
        if not self.bearer_token:
            self._load_cached_token()

    # ------------------------------------------------------------------
    # Token caching
    # ------------------------------------------------------------------

    def _load_cached_token(self):
        """Load token from local cache file if it exists and hasn't expired."""
        if not TOKEN_CACHE_PATH.exists():
            return
        try:
            data = json.loads(TOKEN_CACHE_PATH.read_text())
            token = data.get("access_token")
            if token and not self._is_token_expired(token):
                self.bearer_token = token
                if not self.user_id:
                    self.user_id = data.get("user_id")
        except (json.JSONDecodeError, KeyError):
            pass

    def _save_token(self, token):
        """Cache token to local file and extract user_id from JWT claims."""
        try:
            claims = jwt.decode(token, options={"verify_signature": False})
            user_id = claims.get("https://namespace/uuid")
            exp = claims.get("exp")
            if user_id:
                self.user_id = user_id
        except jwt.DecodeError:
            user_id = self.user_id
            exp = None

        cache_data = {
            "access_token": token,
            "user_id": user_id,
            "exp": exp,
            "cached_at": int(time.time()),
        }
        TOKEN_CACHE_PATH.write_text(json.dumps(cache_data, indent=2))

    @staticmethod
    def _is_token_expired(token):
        """Check if a JWT token is expired (with 5-minute buffer)."""
        try:
            claims = jwt.decode(token, options={"verify_signature": False})
            exp = claims.get("exp")
            if exp is None:
                return False
            return time.time() > (exp - 300)  # 5-min buffer
        except jwt.DecodeError:
            return True

    # ------------------------------------------------------------------
    # OAuth browser login with automatic callback capture
    # ------------------------------------------------------------------

    def login(self):
        """Authenticate via browser-based OAuth Authorization Code flow.

        Opens the Cloud Portal login page in the browser. After the user
        completes login (including MFA), Auth0 redirects to a local callback
        server which captures the authorization code and exchanges it for
        an access token. Fully automatic — no manual copy/paste needed.

        Returns:
            str: The bearer token

        Raises:
            RuntimeError: If login fails or times out
        """
        state = secrets.token_urlsafe(32)
        redirect_uri = f"http://localhost:{CALLBACK_PORT}/callback"
        auth_code = None
        server_error = None

        class CallbackHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                nonlocal auth_code, server_error
                parsed = urlparse(self.path)
                params = parse_qs(parsed.query)

                if parsed.path != "/callback":
                    self.send_response(404)
                    self.end_headers()
                    return

                # Validate state parameter
                returned_state = params.get("state", [None])[0]
                if returned_state != state:
                    server_error = "State mismatch — possible CSRF attack"
                    self._send_html(400, "Login failed: state mismatch.")
                    return

                # Check for errors from Auth0
                error = params.get("error", [None])[0]
                if error:
                    error_desc = params.get("error_description", [error])[0]
                    server_error = error_desc
                    self._send_html(400, f"Login failed: {error_desc}")
                    return

                code = params.get("code", [None])[0]
                if not code:
                    server_error = "No authorization code in callback"
                    self._send_html(400, "Login failed: no code received.")
                    return

                auth_code = code
                self._send_html(200,
                    "Login successful! You can close this tab and return to your terminal."
                )

            def _send_html(self, status, message):
                self.send_response(status)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                html = f"<html><body><h2>{message}</h2></body></html>"
                self.wfile.write(html.encode())

            def log_message(self, format, *args):
                pass  # Suppress request logs

        # Start local callback server
        server = HTTPServer(("localhost", CALLBACK_PORT), CallbackHandler)
        server.timeout = 120  # 2-minute timeout for user to complete login

        # Build Auth0 authorize URL
        authorize_params = {
            "response_type": "code",
            "client_id": AUTH0_CLIENT_ID,
            "redirect_uri": redirect_uri,
            "audience": AUTH0_AUDIENCE,
            "scope": AUTH0_SCOPES,
            "state": state,
        }
        authorize_url = f"https://{AUTH0_DOMAIN}/authorize?{urlencode(authorize_params)}"

        print(f"\nOpening browser for Cloud Portal login...")
        print(f"If the browser doesn't open, visit:\n{authorize_url}\n")
        print("Waiting for login (up to 2 minutes)...")
        webbrowser.open(authorize_url)

        # Wait for callback
        while auth_code is None and server_error is None:
            server.handle_request()

        server.server_close()

        if server_error:
            raise RuntimeError(f"Cloud Portal login failed: {server_error}")

        # Exchange authorization code for access token
        token_response = requests.post(
            f"https://{AUTH0_DOMAIN}/oauth/token",
            json={
                "grant_type": "authorization_code",
                "client_id": AUTH0_CLIENT_ID,
                "code": auth_code,
                "redirect_uri": redirect_uri,
            },
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        if token_response.status_code != 200:
            raise RuntimeError(
                f"Token exchange failed (HTTP {token_response.status_code}): "
                f"{token_response.text}"
            )

        data = token_response.json()
        self.bearer_token = data.get("access_token")

        if not self.bearer_token:
            raise RuntimeError("No access_token in Auth0 token response")

        self._save_token(self.bearer_token)

        # Show confirmation with expiry info
        try:
            claims = jwt.decode(self.bearer_token, options={"verify_signature": False})
            exp = claims.get("exp")
            if exp:
                remaining = (exp - time.time()) / 3600
                print(f"\n✓ Login successful! Token cached. Expires in {remaining:.1f} hours.")
                print(f"  You won't need to log in again until it expires.\n")
        except jwt.DecodeError:
            print("\n✓ Login successful! Token cached.\n")

        return self.bearer_token

    # ------------------------------------------------------------------
    # Headers (lazy login, same pattern as CMCClient)
    # ------------------------------------------------------------------

    def _headers(self):
        if not self.bearer_token or self._is_token_expired(self.bearer_token):
            self.login()
        return {
            'Authorization': f'Bearer {self.bearer_token}',
            'Accept': 'application/json'
        }

    # ------------------------------------------------------------------
    # User ID
    # ------------------------------------------------------------------

    def get_user_id(self):
        """Get user ID from JWT claims, env var, or by triggering login.

        Returns:
            str: The user UUID

        Raises:
            RuntimeError: If user_id cannot be determined
        """
        if self.user_id:
            return self.user_id

        # Trigger login which extracts user_id from JWT
        if not self.bearer_token or self._is_token_expired(self.bearer_token):
            self.login()
            if self.user_id:
                return self.user_id

        # Try decoding existing token
        if self.bearer_token:
            try:
                claims = jwt.decode(
                    self.bearer_token, options={"verify_signature": False}
                )
                self.user_id = claims.get("https://namespace/uuid")
                if self.user_id:
                    return self.user_id
            except jwt.DecodeError:
                pass

        raise RuntimeError(
            "Cannot determine user_id. Set CLOUD_PORTAL_USER_ID in .env, "
            "or log in via browser to obtain it from the JWT."
        )

    # ------------------------------------------------------------------
    # API methods (unchanged)
    # ------------------------------------------------------------------

    def get_clusters_info(self, user_id):
        """Get all clusters for a user"""
        url = f"{self.base_url}/users/{user_id}/clustersinfo"
        r = requests.get(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def get_consumption(self, user_id, instance_uuid):
        """Get consumption data for an instance"""
        url = f"{self.base_url}/users/{user_id}/instances/{instance_uuid}/consumption"
        r = requests.get(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def get_authorized_users(self, user_id, cluster_name):
        """Get authorized users for a cluster"""
        url = f"{self.base_url}/users/{user_id}/instances/{cluster_name}/autherizedusers"
        r = requests.get(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def get_instance_details(self, user_id, cluster_name):
        """Get detailed instance information"""
        url = f"{self.base_url}/users/{user_id}/instances/{cluster_name}"
        r = requests.get(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def find_cluster_uuid(self, user_id, cluster_name):
        """Helper: Find UUID for a cluster by name"""
        clusters_info = self.get_clusters_info(user_id)

        for item in clusters_info.get("instances", []):
            instance = item.get("instance", {})
            if instance.get("name") == cluster_name:
                return instance.get("id")

        return None
