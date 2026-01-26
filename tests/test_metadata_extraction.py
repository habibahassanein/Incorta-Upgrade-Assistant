#!/usr/bin/env python3
"""
Test script for cluster metadata extraction.
Demonstrates auto-detection capabilities.
"""

import json
from clients.cmc_client import CMCClient
from tools.extract_cluster_metadata import extract_cluster_metadata, format_metadata_report


def test_metadata_extraction():
    """Test metadata extraction from live cluster."""
    
    print("=" * 80)
    print("🤖 CLUSTER METADATA AUTO-DETECTION TEST")
    print("=" * 80)
    print()
    
    # Fetch cluster data
    print("📡 Fetching cluster data from CMC...")
    client = CMCClient()
    cluster_data = client.get_cluster("customCluster")
    print("✅ Cluster data retrieved\n")
    
    # Extract metadata
    print("🔍 Extracting metadata (auto-detection)...")
    metadata = extract_cluster_metadata(cluster_data)
    print("✅ Metadata extracted\n")
    
    # Generate formatted report
    print("=" * 80)
    print("📊 FORMATTED REPORT")
    print("=" * 80)
    print()
    report = format_metadata_report(metadata)
    print(report)
    print()
    
    # Show structured JSON
    print("=" * 80)
    print("🗂️  STRUCTURED JSON")
    print("=" * 80)
    print()
    print(json.dumps(metadata, indent=2))
    print()
    
    # Save outputs
    with open("cluster_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    
    with open("cluster_metadata_report.md", "w") as f:
        f.write(report)
    
    print("=" * 80)
    print("💾 Outputs saved:")
    print("   - cluster_metadata.json (structured data)")
    print("   - cluster_metadata_report.md (readable report)")
    print("=" * 80)
    print()
    
    # Demonstrate auto-detection summary
    print("=" * 80)
    print("✨ AUTO-DETECTED INFORMATION (No User Input!):")
    print("=" * 80)
    print(f"✅ Deployment Type: {metadata['deployment_type']['deployment_type']}")
    print(f"✅ Cloud Provider: {metadata['deployment_type']['cloud_provider']}")
    print(f"✅ Database Type: {metadata['database']['db_type']}")
    print(f"✅ Migration Needed: {'Yes' if metadata['database']['migration_needed'] else 'No'}")
    print(f"✅ Topology: {metadata['topology']['topology_type']}")
    print(f"✅ Node Count: {metadata['topology']['node_count']}")
    print(f"✅ HA Enabled: {'Yes' if metadata['topology']['is_ha'] else 'No'}")
    print(f"✅ Notebook: {'Enabled' if metadata['features']['notebook'] else 'Disabled'}")
    print(f"✅ Spark: {'Enabled' if metadata['features']['spark'] else 'Disabled'}")
    print(f"✅ SQLi: {'Enabled' if metadata['features']['sqli'] else 'Disabled'}")
    print(f"✅ Connectors: {metadata['features']['connector_count']} enabled")
    print(f"✅ Spark Mode: {metadata['infrastructure']['spark_deployment']}")
    print(f"✅ Zookeeper: {metadata['infrastructure']['zookeeper_mode']}")
    print(f"✅ Risk Level: {metadata['risks']['risk_level']}")
    print(f"   - Blockers: {len(metadata['risks']['blockers'])}")
    print(f"   - Warnings: {len(metadata['risks']['warnings'])}")
    print("=" * 80)


if __name__ == "__main__":
    test_metadata_extraction()
