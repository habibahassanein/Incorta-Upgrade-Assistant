"""
One-time Cloud Portal login script.

Run this ONCE to cache a token with refresh token support.
After this, the MCP server will auto-refresh tokens silently.

Usage:
    python scripts/cloud_login.py
"""

import os
import sys

# Add parent directory to path so we can import clients
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from clients.cloud_portal_client import CloudPortalClient


def main():
    print("Cloud Portal Login")
    print("=" * 40)
    print()

    client = CloudPortalClient()

    # Check if we already have a valid token
    if client.bearer_token and not client._is_token_expired(client.bearer_token):
        print("You already have a valid token cached.")
        print(f"  User ID: {client.user_id}")
        print(f"  Refresh token: {'Yes' if client.refresh_token else 'No'}")
        print()
        print("To force a new login, delete ~/.incorta_cloud_token.json and run again.")
        return

    print("Opening browser for Auth0 login...")
    print("Complete the login (including MFA if required).")
    print()

    try:
        client.login()
        print()
        print("Login successful!")
        print(f"  User ID: {client.user_id}")
        print(f"  Refresh token: {'Cached' if client.refresh_token else 'Not available'}")
        print()
        if client.refresh_token:
            print("The MCP server will now auto-refresh tokens silently.")
            print("You should not need to run this script again.")
        else:
            print("WARNING: No refresh token received. You may need to log in again when the token expires.")
            print("Ensure 'offline_access' scope is configured in Auth0.")
    except RuntimeError as e:
        print(f"\nLogin failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
