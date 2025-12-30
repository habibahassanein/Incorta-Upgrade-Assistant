"""
Test file for workflow - run this to test without MCP server.
"""

import os
import sys

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from workflows.pre_upgrade_validation import run_validation


def main():
    cluster_name = os.getenv("CMC_CLUSTER_NAME", "customCluster")
    
    print(f"Running pre-upgrade validation for: {cluster_name}")
    print("-" * 50)
    
    report = run_validation(cluster_name)
    print(report)


if __name__ == "__main__":
    main()
