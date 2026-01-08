#!/usr/bin/env python3
"""
Demonstration of the version parameter in retrieve functions.

This script shows how to use the new version parameter to retrieve
specific versions of data instead of always getting the latest version.
"""

from openghg.retrieve import get_obs_surface, search

# Example 1: Default behavior (backward compatible)
print("=" * 70)
print("Example 1: Default behavior - retrieves latest version")
print("=" * 70)
print("Code: get_obs_surface(site='bsd', species='co2', inlet='248m')")
print()

# Example 2: Explicit latest version
print("=" * 70)
print("Example 2: Explicit latest version")
print("=" * 70)
print("Code: get_obs_surface(site='bsd', species='co2', inlet='248m', version='latest')")
print()

# Example 3: Search to see available versions
print("=" * 70)
print("Example 3: Check available versions")
print("=" * 70)
print("Code:")
print("  results = search(site='bsd', species='co2', inlet='248m')")
print("  metadata = list(results.metadata.values())[0]")
print("  print(f\"Latest version: {metadata['latest_version']}\")")
print("  print(f\"All versions: {list(metadata['versions'].keys())}\")")
print()

# Example 4: Retrieve specific version
print("=" * 70)
print("Example 4: Retrieve specific version")
print("=" * 70)
print("Code:")
print("  obs_data = get_obs_surface(")
print("      site='bsd',")
print("      species='co2',")
print("      inlet='248m',")
print("      version='v1'  # Retrieve version 1 specifically")
print("  )")
print()

# Example 5: All get_* functions support version parameter
print("=" * 70)
print("Example 5: Version parameter works with all get_* functions")
print("=" * 70)
print("All these functions now accept a version parameter:")
print()
print("  - get_obs_surface(..., version='v1')")
print("  - get_obs_column(..., version='v2')")
print("  - get_flux(..., version='latest')")
print("  - get_bc(..., version='v1')")
print("  - get_footprint(..., version='latest')")
print()

print("=" * 70)
print("Key Features")
print("=" * 70)
print("1. Default behavior unchanged: version='latest' by default")
print("2. Backward compatible: existing code works without changes")
print("3. Version validation: raises ValueError for invalid versions")
print("4. Consistent interface: all get_* functions have same parameter")
print()
