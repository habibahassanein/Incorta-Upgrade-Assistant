"""
Test CMC client directly.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from clients.cmc_client import CMCClient


def main():
    client = CMCClient()
    cluster_name = os.getenv("CMC_CLUSTER_NAME", "customCluster")
    
    print("Testing CMC Client")
    print("-" * 40)
    
    print("\n1. Login...")
    token = client.login()
    print(f"   JWT: {token[:30]}...")
    
    print("\n2. Get clusters brief...")
    clusters = client.get_clusters_brief()
    names = [c.get("name") for c in clusters]
    print(f"   Found: {names}")
    
    print(f"\n3. Get cluster: {cluster_name}...")
    data = client.get_cluster(cluster_name)
    print(f"   Name: {data.get('name')}")
    print(f"   Spark: {data.get('spark_status')}")
    print(f"   Nodes: {len(data.get('nodes', []))}")


if __name__ == "__main__":
    main()
