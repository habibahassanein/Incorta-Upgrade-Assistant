"""
wrapper to the login, rather than the mcp based login
"""

import os
import base64
import requests
from dotenv import load_dotenv

load_dotenv()

CMC_URL = os.getenv("CMC_URL", "").rstrip("/")
CMC_USER = os.getenv("CMC_USER", "")
CMC_PASSWORD = os.getenv("CMC_PASSWORD", "")
VERIFY_SSL = os.getenv("VERIFY_SSL", "true").lower() in ("1", "true", "yes")


class CMCClient:
    def __init__(self, url=None, user=None, password=None):
        self.url = url or CMC_URL
        self.user = user or CMC_USER
        self.password = password or CMC_PASSWORD
        self.token = None
    
    def login(self):
        """login to CMC with Basic Auth, get JWT token"""
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
            raise RuntimeError(f"CMC login failed: {r.status_code}")
        
        self.token = r.json().get("token")
        if not self.token:
            raise RuntimeError("No token in response")
        
        return self.token
    
    def _headers(self):
        if not self.token:
            self.login()
        return {
            "Accept": "application/json",
            "Authorization": f"bearer {self.token}",
        }
    
    def get_cluster(self, cluster_name):
        # get full cluster details
        r = requests.get(
            f"{self.url}/api/v1/clusters/{cluster_name}",
            headers=self._headers(),
            verify=VERIFY_SSL,
            timeout=60,
        )
        r.raise_for_status()
        return r.json()
    
    def get_clusters_brief(self):
        # list all clusters
        r = requests.get(
            f"{self.url}/api/v1/clusters/brief",
            headers=self._headers(),
            verify=VERIFY_SSL,
            timeout=60,
        )
        r.raise_for_status()
        return r.json()
