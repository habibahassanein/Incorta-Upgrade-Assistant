"""
Connector Compatibility Checker

Checks connector compatibility with target Incorta version and JDK version.
Uses a static JSON mapping file since no API provides this information.

The CMC API only returns connectorName + connectorEnabled per connector.
The Cloud Portal API has zero connector fields.
"""

import json
import os
import re
from typing import Dict, List, Optional

# Module-level cache
_compat_data: Optional[dict] = None

_DATA_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "connector_compatibility.json",
)


def load_compatibility_data() -> dict:
    """Load and cache the connector compatibility mapping."""
    global _compat_data
    if _compat_data is not None:
        return _compat_data

    if not os.path.exists(_DATA_FILE):
        _compat_data = {"connectors": {}, "jdk_version_by_incorta_version": {}}
        return _compat_data

    with open(_DATA_FILE) as f:
        _compat_data = json.load(f)
    return _compat_data


def _normalize_version(version: str) -> str:
    """Extract the base version number (e.g., '2024.7.0' from '2024.7.0-hotfix')."""
    match = re.match(r"(\d{4}\.\d+\.\d+)", version)
    return match.group(1) if match else version


def get_target_jdk(to_version: str) -> str:
    """Return the JDK version for a given Incorta version.

    Returns:
        JDK version string (e.g., '17') or 'Unknown'.
    """
    data = load_compatibility_data()
    mapping = data.get("jdk_version_by_incorta_version", {})
    base = _normalize_version(to_version)
    return mapping.get(base, "Unknown")


def is_jdk_upgrade(from_version: str, to_version: str) -> bool:
    """Return True if the upgrade crosses a JDK version boundary."""
    from_jdk = get_target_jdk(from_version)
    to_jdk = get_target_jdk(to_version)
    if from_jdk == "Unknown" or to_jdk == "Unknown":
        return False
    return from_jdk != to_jdk


def check_connector_compatibility(
    connectors: List[dict],
    from_version: str = "",
    to_version: str = "",
) -> dict:
    """Check connector compatibility with the target version/JDK.

    Args:
        connectors: Raw CMC connector list
            [{"connectorName": str, "connectorEnabled": bool}, ...]
        from_version: Source Incorta version.
        to_version: Target Incorta version.

    Returns:
        dict with categorized connectors, blockers, and warnings.
    """
    data = load_compatibility_data()
    compat_map = data.get("connectors", {})

    from_jdk = get_target_jdk(from_version) if from_version else "Unknown"
    to_jdk = get_target_jdk(to_version) if to_version else "Unknown"
    jdk_upgrade = is_jdk_upgrade(from_version, to_version) if from_version and to_version else False

    compatible = []
    incompatible = []
    unknown = []
    blockers = []
    warnings = []

    for c in connectors:
        name = c.get("connectorName", "")
        enabled = c.get("connectorEnabled", False)

        # Case-insensitive lookup
        entry = compat_map.get(name) or compat_map.get(name.lower())
        if entry is None:
            # Try stripping common suffixes
            stripped = re.sub(r"(?i)(connector|plugin)$", "", name).strip()
            entry = compat_map.get(stripped) or compat_map.get(stripped.lower())

        info = {
            "name": name,
            "enabled": enabled,
            "notes": entry.get("notes", "") if entry else "",
        }

        if entry is None:
            info["status"] = "unknown"
            unknown.append(info)
            if enabled:
                warnings.append(
                    f"{name} (enabled) — not in compatibility matrix, verify manually"
                )
            else:
                warnings.append(
                    f"{name} (disabled) — not in compatibility matrix"
                )
        elif entry.get("jdk17_compatible") is True:
            info["status"] = "compatible"
            compatible.append(info)
        elif entry.get("jdk17_compatible") is False:
            info["status"] = "incompatible"
            incompatible.append(info)
            if enabled:
                blockers.append(
                    f"{name} (enabled) is INCOMPATIBLE with JDK {to_jdk}"
                )
            else:
                warnings.append(
                    f"{name} (disabled) is incompatible with JDK {to_jdk} — flag if customer plans to re-enable"
                )
        elif entry.get("jdk17_compatible") is None:
            info["status"] = "unknown"
            unknown.append(info)
            note = entry.get("notes", "")
            if enabled:
                warnings.append(
                    f"{name} (enabled) — compatibility unknown{f': {note}' if note else ', verify manually'}"
                )

    return {
        "jdk_upgrade": jdk_upgrade,
        "from_jdk": from_jdk,
        "to_jdk": to_jdk,
        "total_connectors": len(connectors),
        "enabled_count": sum(1 for c in connectors if c.get("connectorEnabled")),
        "disabled_count": sum(1 for c in connectors if not c.get("connectorEnabled")),
        "compatible": compatible,
        "incompatible": incompatible,
        "unknown": unknown,
        "blockers": blockers,
        "warnings": warnings,
    }
