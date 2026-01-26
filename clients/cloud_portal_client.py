"""
Cloud Portal API Client
Provides access to cloud-specific metadata, consumption tracking, and user management
"""

import requests
import os


class CloudPortalClient:
    def __init__(self, bearer_token=None):
        self.base_url = "https://cloudstaging.incortalabs.com/api/v2"
        self.bearer_token = bearer_token or os.getenv("CLOUD_PORTAL_TOKEN")
        
    def _headers(self):
        return {
            'Authorization': f'Bearer {self.bearer_token}',
            'Accept': 'application/json'
        }
    
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
