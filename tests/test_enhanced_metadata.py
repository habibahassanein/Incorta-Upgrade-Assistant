#!/usr/bin/env python3
"""
Test the enhanced metadata extraction with comparison.
"""

from tools.extract_cluster_metadata import extract_cluster_metadata, format_metadata_report
from clients.cmc_client import CMCClient
import json


def main():
    print("=" * 80)
    print("🧪 ENHANCED METADATA EXTRACTION TEST")
    print("=" * 80)
    print()
    
    # Fetch cluster data
    print("📡 Fetching cluster data from CMC...")
    client = CMCClient()
    cluster_data = client.get_cluster("customCluster")
    print("✅ Data retrieved\n")
    
    # Test WITHOUT API calls (baseline)
    print("📊 TEST 1: Baseline (without API calls)")
    print("-" * 80)
    metadata_baseline = extract_cluster_metadata(cluster_data, include_api_calls=False)
    print(f"✅ Fields extracted: {len(metadata_baseline.keys())}")
    print(f"   Keys: {', '.join(metadata_baseline.keys())}")
    print()
    
    # Test WITH API calls (enhanced)
    print("📊 TEST 2: Enhanced (with API calls)")
    print("-" * 80)
    metadata_enhanced = extract_cluster_metadata(cluster_data, include_api_calls=True)
    print(f"✅ Fields extracted: {len(metadata_enhanced.keys())}")
    print(f"   Keys: {', '.join(metadata_enhanced.keys())}")
    print()
    
    # Compare
    print("🔍 COMPARISON")
    print("-" * 80)
    baseline_keys = set(metadata_baseline.keys())
    enhanced_keys = set(metadata_enhanced.keys())
    new_keys = enhanced_keys - baseline_keys
    
    if new_keys:
        print(f"✨ NEW FIELDS ADDED: {', '.join(new_keys)}")
    else:
        print("ℹ️  No new fields added")
    print()
    
    # Show new data
    if "version_info" in metadata_enhanced:
        print("📋 VERSION INFO:")
        ver = metadata_enhanced["version_info"]
        print(f"   - Node Versions: {ver.get('node_versions', {})}")
        print(f"   - Version Consistent: {ver.get('version_consistent')}")
        print()
    
    if "tenant_storage" in metadata_enhanced:
        ts = metadata_enhanced["tenant_storage"]
        if ts.get("status") == "success":
            print("💾 TENANT STORAGE:")
            print(f"   - Total Tenants: {ts.get('total_tenants')}")
            print(f"   - Unlimited Quota: {ts.get('unlimited_quota_count')}")
            for tenant in ts.get("tenants", []):
                print(f"   - {tenant.get('name')}: {tenant.get('disk_quota')}")
            print()
    
    if "integrations" in metadata_enhanced:
        integ = metadata_enhanced["integrations"]
        if integ.get("status") == "success":
            print("🔌 INTEGRATIONS:")
            print(f"   - Total Config Items: {integ.get('total_config_items')}")
            print(f"   - Enabled Integrations: {len(integ.get('enabled_integrations', []))}")
            if integ.get("enabled_integrations"):
                print(f"   - Enabled: {', '.join(integ.get('enabled_integrations'))}")
            print()
    
    # Generate full report
    print("=" * 80)
    print("📄 FULL REPORT")
    print("=" * 80)
    print()
    report = format_metadata_report(metadata_enhanced)
    print(report)
    
    # Save comparison
    with open("metadata_comparison.json", "w") as f:
        json.dump({
            "baseline": metadata_baseline,
            "enhanced": metadata_enhanced,
            "new_fields": list(new_keys),
        }, f, indent=2)
    
    print("\n" + "=" * 80)
    print("💾 Comparison saved to: metadata_comparison.json")
    print("=" * 80)
    
    # Summary
    print()
    print("✅ TEST SUMMARY:")
    print(f"   - Baseline fields: {len(baseline_keys)}")
    print(f"   - Enhanced fields: {len(enhanced_keys)}")
    print(f"   - New fields added: {len(new_keys)}")
    print(f"   - Coverage improvement: +{len(new_keys)} data sources")


if __name__ == "__main__":
    main()
