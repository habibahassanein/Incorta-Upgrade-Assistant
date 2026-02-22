"""
Cloud Portal API Client
Provides access to cloud-specific metadata, consumption tracking, and user management.

Authentication: Uses OAuth Authorization Code flow with a local callback server.
When no valid token is available, opens the browser for Cloud Portal login (with MFA),
captures the token automatically via localhost callback, and caches it locally.
"""

import base64
import hashlib
import json
import os
import secrets
import threading
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
AUTH0_CLIENT_ID = os.getenv("AUTH0_CLIENT_ID", "0jXCrcpFe6PDm6sIMxDi7hunFCWeRLpt")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE", "https://cloud.server/api/")
AUTH0_SCOPES = (
    "openid profile email "
    "read:cluster create:cluster update:cluster delete:cluster manage:cluster "
    "offline_access"
)
CALLBACK_PORT = int(os.getenv("AUTH0_CALLBACK_PORT", "8910"))
CLOUD_PORTAL_URL = os.getenv("CLOUD_PORTAL_URL", "https://cp-cloudstaging.incortalabs.com")
TOKEN_CACHE_PATH = Path(os.getenv("TOKEN_CACHE_PATH", str(Path.home() / ".incorta_cloud_token.json")))


def infer_cloud_cluster_name():
    """Infer Cloud Portal cluster name from CMC_URL subdomain.

    CMC_URL pattern: https://{cluster_name}.cloudstaging.incortalabs.com/cmc
    Returns the subdomain (e.g., 'habibascluster') or empty string.
    """
    cmc_url = os.getenv("CMC_URL", "")
    if not cmc_url:
        return ""
    parsed = urlparse(cmc_url)
    hostname = parsed.hostname or ""
    parts = hostname.split(".")
    if len(parts) >= 2:
        return parts[0]
    return ""


class CloudPortalClient:
    def __init__(self, bearer_token=None):
        self.base_url = f"{CLOUD_PORTAL_URL}/api/v2"
        self.bearer_token = bearer_token or os.getenv("CLOUD_PORTAL_TOKEN")
        self.user_id = os.getenv("CLOUD_PORTAL_USER_ID")
        self.refresh_token = None

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
            # Always load refresh token (it has a longer lifetime than access token)
            self.refresh_token = data.get("refresh_token")
            if not self.user_id:
                self.user_id = data.get("user_id")
            token = data.get("access_token")
            if token and not self._is_token_expired(token):
                self.bearer_token = token
        except (json.JSONDecodeError, KeyError):
            pass

    def _save_token(self, token, refresh_token=None):
        """Cache token to local file and extract user_id from JWT claims."""
        try:
            # Signature verification skipped: token obtained from Auth0 over HTTPS.
            # We only read claims (user_id, exp), not verify authenticity.
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
            "refresh_token": refresh_token,
            "user_id": user_id,
            "exp": exp,
            "cached_at": int(time.time()),
        }
        TOKEN_CACHE_PATH.write_text(json.dumps(cache_data, indent=2))

    @staticmethod
    def _is_token_expired(token):
        """Check if a JWT token is expired (with 5-minute buffer)."""
        try:
            # Signature verification skipped: only reading exp claim for expiry check.
            claims = jwt.decode(token, options={"verify_signature": False})
            exp = claims.get("exp")
            if exp is None:
                return False
            return time.time() > (exp - 300)  # 5-min buffer
        except jwt.DecodeError:
            return True

    # ------------------------------------------------------------------
    # PKCE helpers (required for Auth0 SPA clients)
    # ------------------------------------------------------------------

    @staticmethod
    def _generate_code_verifier():
        """Generate a PKCE code_verifier (43-128 chars of unreserved URI characters)."""
        return secrets.token_urlsafe(64)[:128]

    @staticmethod
    def _generate_code_challenge(code_verifier):
        """Generate S256 PKCE code_challenge from a code_verifier."""
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

    # ------------------------------------------------------------------
    # Headless environment detection
    # ------------------------------------------------------------------

    @staticmethod
    def _is_headless():
        """Detect if running in a headless environment (Docker, CI, SSH without display)."""
        if os.getenv("HEADLESS", "").lower() in ("1", "true", "yes"):
            return True
        if os.path.exists("/.dockerenv"):
            return True
        if os.name == "posix" and not os.getenv("DISPLAY") and not os.getenv("WAYLAND_DISPLAY"):
            import platform
            if platform.system() == "Linux":
                return True
        return False

    # ------------------------------------------------------------------
    # Token refresh (headless-compatible)
    # ------------------------------------------------------------------

    def _refresh_access_token(self):
        """Use refresh token to get a new access token without browser login.

        Returns True on success, False on failure.
        """
        refresh_token = self.refresh_token or os.getenv("CLOUD_PORTAL_REFRESH_TOKEN")
        if not refresh_token:
            return False

        try:
            response = requests.post(
                f"https://{AUTH0_DOMAIN}/oauth/token",
                json={
                    "grant_type": "refresh_token",
                    "client_id": AUTH0_CLIENT_ID,
                    "refresh_token": refresh_token,
                },
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code != 200:
                return False

            data = response.json()
            new_access_token = data.get("access_token")
            # Auth0 may rotate the refresh token
            new_refresh_token = data.get("refresh_token", refresh_token)

            if not new_access_token:
                return False

            self.bearer_token = new_access_token
            self.refresh_token = new_refresh_token
            self._save_token(new_access_token, new_refresh_token)
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Device Code Flow (headless-compatible, multi-user)
    # ------------------------------------------------------------------

    def device_login(self):
        """Start Device Authorization Flow — no browser on server needed.

        Requests a device code from Auth0 and returns login instructions.
        The caller should display the URL + code to the user, then call
        poll_device_login() to wait for completion.

        Returns:
            dict: Contains device_code, user_code, verification_uri,
                  expires_in, and interval for polling.

        Raises:
            RuntimeError: If the device code request fails (e.g., grant type not enabled).
        """
        response = requests.post(
            f"https://{AUTH0_DOMAIN}/oauth/device/code",
            json={
                "client_id": AUTH0_CLIENT_ID,
                "scope": AUTH0_SCOPES,
                "audience": AUTH0_AUDIENCE,
            },
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Device code request failed (HTTP {response.status_code}): {response.text}\n"
                "Ensure the Auth0 client has the 'Device Code' grant type enabled."
            )

        data = response.json()
        return {
            "device_code": data["device_code"],
            "user_code": data["user_code"],
            "verification_uri": data.get("verification_uri_complete") or data["verification_uri"],
            "expires_in": data.get("expires_in", 900),
            "interval": data.get("interval", 5),
        }

    def poll_device_login(self, device_code, interval=5, expires_in=900):
        """Poll Auth0 until the user completes device login.

        Blocks until the user finishes authentication or the code expires.

        Args:
            device_code: The device_code from device_login().
            interval: Seconds between poll attempts.
            expires_in: Maximum time to wait in seconds.

        Returns:
            str: The bearer token.

        Raises:
            RuntimeError: If login fails, is denied, or times out.
        """
        deadline = time.time() + expires_in

        while time.time() < deadline:
            time.sleep(interval)
            response = requests.post(
                f"https://{AUTH0_DOMAIN}/oauth/token",
                json={
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": device_code,
                    "client_id": AUTH0_CLIENT_ID,
                },
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code == 200:
                data = response.json()
                self.bearer_token = data.get("access_token")
                self.refresh_token = data.get("refresh_token")
                if self.bearer_token:
                    self._save_token(self.bearer_token, self.refresh_token)
                    return self.bearer_token
                raise RuntimeError("No access_token in Auth0 token response")

            error = response.json().get("error", "")
            if error == "authorization_pending":
                continue
            elif error == "slow_down":
                interval += 5
                continue
            elif error == "expired_token":
                raise RuntimeError("Device code expired. Please try logging in again.")
            elif error == "access_denied":
                raise RuntimeError("Login was denied by the user.")
            else:
                raise RuntimeError(f"Unexpected error during device login: {error}")

        raise RuntimeError("Login timed out. Please try again.")

    # ------------------------------------------------------------------
    # OAuth browser login with automatic callback capture
    # ------------------------------------------------------------------

    def login(self):
        """Authenticate via browser-based OAuth Authorization Code flow.

        Opens the Cloud Portal login page in the browser. After the user
        completes login (including MFA), Auth0 redirects to a local callback
        server which captures the authorization code and exchanges it for
        an access token. Fully automatic — no manual copy/paste needed.

        In headless environments (Docker, CI), raises RuntimeError with
        instructions to use environment variables instead.

        Returns:
            str: The bearer token

        Raises:
            RuntimeError: If login fails, times out, or running in headless mode
        """
        if self._is_headless():
            raise RuntimeError(
                "AUTHENTICATION_REQUIRED: No valid token available.\n"
                "Please call the 'cloud_portal_login' tool first to authenticate."
            )

        state = secrets.token_urlsafe(32)
        code_verifier = self._generate_code_verifier()
        code_challenge = self._generate_code_challenge(code_verifier)
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

        # Build Auth0 authorize URL (with PKCE for SPA client compatibility)
        authorize_params = {
            "response_type": "code",
            "client_id": AUTH0_CLIENT_ID,
            "redirect_uri": redirect_uri,
            "audience": AUTH0_AUDIENCE,
            "scope": AUTH0_SCOPES,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
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

        # Exchange authorization code for access token (with PKCE code_verifier)
        token_response = requests.post(
            f"https://{AUTH0_DOMAIN}/oauth/token",
            json={
                "grant_type": "authorization_code",
                "client_id": AUTH0_CLIENT_ID,
                "code": auth_code,
                "redirect_uri": redirect_uri,
                "code_verifier": code_verifier,
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
        self.refresh_token = data.get("refresh_token")

        if not self.bearer_token:
            raise RuntimeError("No access_token in Auth0 token response")

        self._save_token(self.bearer_token, self.refresh_token)

        # Show confirmation with expiry info
        try:
            # Signature verification skipped: token just obtained from Auth0 token endpoint.
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
    # Non-blocking OAuth login for MCP tool use
    # ------------------------------------------------------------------

    def build_authorize_url(self, redirect_uri: str):
        """Build an OAuth Authorization Code + PKCE authorize URL.

        Use this when the callback will be handled externally (e.g., by a
        custom route on the MCP server itself). No local HTTP server is started.

        Args:
            redirect_uri: The redirect URI Auth0 will send the code to.

        Returns:
            dict with keys:
                - authorize_url (str): URL the user must open in their browser
                - state (str): CSRF state token (caller must store and validate)
                - code_verifier (str): PKCE code verifier for token exchange
                - redirect_uri (str): The redirect URI used
        """
        state = secrets.token_urlsafe(32)
        code_verifier = self._generate_code_verifier()
        code_challenge = self._generate_code_challenge(code_verifier)

        authorize_params = {
            "response_type": "code",
            "client_id": AUTH0_CLIENT_ID,
            "redirect_uri": redirect_uri,
            "audience": AUTH0_AUDIENCE,
            "scope": AUTH0_SCOPES,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        authorize_url = f"https://{AUTH0_DOMAIN}/authorize?{urlencode(authorize_params)}"

        return {
            "authorize_url": authorize_url,
            "state": state,
            "code_verifier": code_verifier,
            "redirect_uri": redirect_uri,
        }

    def login_for_mcp(self):
        """Start Authorization Code + PKCE flow for MCP tool use.

        Does NOT open a browser. Returns the authorize URL for the MCP tool
        to display, plus internal state for a background callback server.

        The MCP tool calls this once, returns the URL to the user, then on a
        subsequent call checks event.is_set() to see if the user completed login.

        Returns:
            dict with keys:
                - authorize_url (str): URL the user must open in their browser
                - event (threading.Event): Set when callback is received
                - auth_code_holder (dict): {"code": str|None, "error": str|None}
                - code_verifier (str): PKCE code verifier for token exchange
                - redirect_uri (str): The callback URI
                - server (HTTPServer): The callback server (for cleanup)
                - server_thread (threading.Thread): Background thread

        Raises:
            RuntimeError: If the callback server cannot start (port in use).
        """
        state = secrets.token_urlsafe(32)
        code_verifier = self._generate_code_verifier()
        code_challenge = self._generate_code_challenge(code_verifier)
        redirect_uri = f"http://localhost:{CALLBACK_PORT}/callback"

        auth_code_holder = {"code": None, "error": None}
        login_complete = threading.Event()

        class CallbackHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlparse(self.path)
                params = parse_qs(parsed.query)

                if parsed.path != "/callback":
                    self.send_response(404)
                    self.end_headers()
                    return

                # Validate state parameter
                returned_state = params.get("state", [None])[0]
                if returned_state != state:
                    auth_code_holder["error"] = "State mismatch — possible CSRF attack"
                    self._send_html(400, "Login failed: state mismatch.")
                    login_complete.set()
                    return

                # Check for errors from Auth0
                error = params.get("error", [None])[0]
                if error:
                    error_desc = params.get("error_description", [error])[0]
                    auth_code_holder["error"] = error_desc
                    self._send_html(400, f"Login failed: {error_desc}")
                    login_complete.set()
                    return

                code = params.get("code", [None])[0]
                if not code:
                    auth_code_holder["error"] = "No authorization code in callback"
                    self._send_html(400, "Login failed: no code received.")
                    login_complete.set()
                    return

                auth_code_holder["code"] = code
                self._send_html(200,
                    "Login successful! You can close this tab and return to Claude."
                )
                login_complete.set()

            def _send_html(self, status, message):
                self.send_response(status)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                html = f"<html><body><h2>{message}</h2></body></html>"
                self.wfile.write(html.encode())

            def log_message(self, format, *args):
                pass  # Suppress request logs

        # Start HTTP server
        try:
            server = HTTPServer(("localhost", CALLBACK_PORT), CallbackHandler)
        except OSError as e:
            raise RuntimeError(
                f"Cannot start callback server on port {CALLBACK_PORT}: {e}. "
                f"Check if another process is using port {CALLBACK_PORT}."
            )
        server.timeout = 1  # Short timeout so we can check the Event regularly

        def serve_until_done():
            """Run the HTTP server until the callback is received."""
            while not login_complete.is_set():
                server.handle_request()

        server_thread = threading.Thread(target=serve_until_done, daemon=True)
        server_thread.start()

        # Build authorize URL with PKCE
        authorize_params = {
            "response_type": "code",
            "client_id": AUTH0_CLIENT_ID,
            "redirect_uri": redirect_uri,
            "audience": AUTH0_AUDIENCE,
            "scope": AUTH0_SCOPES,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        authorize_url = f"https://{AUTH0_DOMAIN}/authorize?{urlencode(authorize_params)}"

        return {
            "authorize_url": authorize_url,
            "event": login_complete,
            "auth_code_holder": auth_code_holder,
            "code_verifier": code_verifier,
            "redirect_uri": redirect_uri,
            "server": server,
            "server_thread": server_thread,
        }

    def exchange_code_for_token(self, auth_code, redirect_uri, code_verifier):
        """Exchange an authorization code for an access token (with PKCE).

        Used by the MCP tool after the callback server captures the auth code.

        Args:
            auth_code: The authorization code from the callback.
            redirect_uri: The redirect URI used in the authorize request.
            code_verifier: The PKCE code verifier.

        Returns:
            str: The bearer token.

        Raises:
            RuntimeError: If the token exchange fails.
        """
        token_response = requests.post(
            f"https://{AUTH0_DOMAIN}/oauth/token",
            json={
                "grant_type": "authorization_code",
                "client_id": AUTH0_CLIENT_ID,
                "code": auth_code,
                "redirect_uri": redirect_uri,
                "code_verifier": code_verifier,
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
        self.refresh_token = data.get("refresh_token")

        if not self.bearer_token:
            raise RuntimeError("No access_token in Auth0 token response")

        self._save_token(self.bearer_token, self.refresh_token)
        return self.bearer_token

    # ------------------------------------------------------------------
    # Headers (lazy login, same pattern as CMCClient)
    # ------------------------------------------------------------------

    def _headers(self):
        if not self.bearer_token or self._is_token_expired(self.bearer_token):
            if not self._refresh_access_token():
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

        # Try refreshing token first (works in headless environments)
        if not self.bearer_token or self._is_token_expired(self.bearer_token):
            if not self._refresh_access_token():
                self.login()
            if self.user_id:
                return self.user_id

        # Try decoding existing token
        if self.bearer_token:
            try:
                # Signature verification skipped: only reading user_id claim.
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
        """Get consumption data for an instance.

        Args:
            user_id: User UUID from JWT claims.
            instance_uuid: Instance UUID (NOT name). Use find_cluster_uuid() to resolve.
        """
        url = f"{self.base_url}/users/{user_id}/instances/{instance_uuid}/consumption"
        r = requests.get(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def get_authorized_users(self, user_id, cluster_name):
        """Get authorized users for a cluster.

        Args:
            user_id: User UUID from JWT claims.
            cluster_name: Instance name (NOT UUID) as shown in Cloud Portal.
        """
        url = f"{self.base_url}/users/{user_id}/instances/{cluster_name}/autherizedusers"
        r = requests.get(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def get_instance_details(self, user_id, cluster_name):
        """Get detailed instance information.

        Args:
            user_id: User UUID from JWT claims.
            cluster_name: Instance name (NOT UUID) as shown in Cloud Portal.
        """
        url = f"{self.base_url}/users/{user_id}/instances/{cluster_name}"
        r = requests.get(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def find_cluster(self, user_id, cluster_name):
        """Find a cluster's full instance dict by name.

        Returns:
            dict or None: The full instance dict, or None if not found.
        """
        clusters_info = self.get_clusters_info(user_id)
        for item in clusters_info.get("instances", []):
            instance = item.get("instance", {})
            if instance.get("name") == cluster_name:
                return instance
        return None

    def find_cluster_uuid(self, user_id, cluster_name):
        """Helper: Find UUID for a cluster by name."""
        instance = self.find_cluster(user_id, cluster_name)
        return instance.get("id") if instance else None
