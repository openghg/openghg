# Implementation Summary: Version Parameter for Retrieve Functions

## Overview
Successfully implemented the ability to retrieve specific data versions in all `get_*` functions within `openghg.retrieve._access`, instead of always defaulting to the latest version.

## Files Modified

### 1. `openghg/retrieve/_access.py` (Core Implementation)
**Changes:**
- Modified `_get_generic()` function to accept `version` parameter (default: "latest")
- Updated all five `get_*` functions to accept `version` parameter:
  - `get_obs_surface()`
  - `get_obs_column()`
  - `get_flux()`
  - `get_bc()`
  - `get_footprint()`
- Updated all docstrings to document the new parameter
- Version is passed through the call chain: `get_*()` → `_get_generic()` → `SearchResults.retrieve_all(version)` → `Datasource.get_data(version)`

**Lines Changed:** 18 additions, 1 deletion

### 2. `tests/retrieve/test_version.py` (New Test Suite)
**Created comprehensive test suite with 10 tests:**
1. `test_get_obs_surface_default_version` - Default behavior (backward compatibility)
2. `test_get_obs_surface_explicit_latest_version` - Explicit "latest" version
3. `test_get_obs_surface_specific_version` - Specific version retrieval
4. `test_get_obs_surface_invalid_version` - Error handling for invalid versions
5. `test_get_footprint_with_version` - Footprint function with version
6. `test_get_flux_with_version` - Flux function with version
7. `test_get_bc_with_version` - Boundary conditions function with version
8. `test_get_obs_column_with_version` - Column observation function with version
9. `test_version_parameter_backwards_compatibility` - Backward compatibility test
10. `test_version_parameter_in_search_results` - SearchResults integration test

**Test Results:** All 10 tests passing ✓

### 3. `examples/version_parameter_demo.py` (Documentation)
**Created demo script showing:**
- Default behavior example
- Explicit latest version example
- Version checking example
- Specific version retrieval example
- Overview of all functions supporting version parameter

## Technical Implementation Details

### Version Flow
```
User calls: get_obs_surface(site='bsd', species='co2', version='v1')
     ↓
_get_generic(version='v1')
     ↓
results.retrieve_all(version='v1')
     ↓
Datasource.get_data(version='v1')
```

### Version Validation
- Validation happens at the `Datasource.get_data()` level
- Invalid versions raise `ValueError` with message showing available versions
- Version strings expected: "latest", "v1", "v2", etc.

### Backward Compatibility
- Default value: `version="latest"`
- Existing code continues to work without any changes
- No breaking changes introduced

## Testing & Validation

### Code Quality Checks
- ✓ **black**: All files properly formatted
- ✓ **flake8**: No linting errors
- ✓ **mypy**: No type errors in modified files

### Test Coverage
- ✓ **New tests**: 10/10 passing
- ✓ **Existing tests**: All retrieve tests passing
- ✓ **Integration**: Verified with existing test suite

### Code Review
- ✓ **Initial review**: 2 comments about misleading test comments
- ✓ **Final review**: No issues found

## Usage Examples

### Example 1: Default Behavior (Backward Compatible)
```python
from openghg.retrieve import get_obs_surface

# Gets latest version by default
data = get_obs_surface(site='bsd', species='co2', inlet='248m')
```

### Example 2: Explicit Latest Version
```python
from openghg.retrieve import get_obs_surface

# Explicitly request latest version
data = get_obs_surface(site='bsd', species='co2', inlet='248m', version='latest')
```

### Example 3: Check Available Versions
```python
from openghg.retrieve import search

# Search to see available versions
results = search(site='bsd', species='co2', inlet='248m')
metadata = list(results.metadata.values())[0]
print(f"Latest version: {metadata['latest_version']}")
print(f"All versions: {list(metadata['versions'].keys())}")
```

### Example 4: Retrieve Specific Version
```python
from openghg.retrieve import get_obs_surface

# Retrieve version 1 specifically
data = get_obs_surface(
    site='bsd',
    species='co2',
    inlet='248m',
    version='v1'
)
```

### Example 5: All Functions Support Version
```python
from openghg.retrieve import *

# All these functions now accept version parameter
obs_data = get_obs_surface(..., version='v1')
column_data = get_obs_column(..., version='v2')
flux_data = get_flux(..., version='latest')
bc_data = get_bc(..., version='v1')
fp_data = get_footprint(..., version='latest')
```

## Key Features
1. ✓ Minimal surgical changes - only added one parameter to 6 functions
2. ✓ Consistent interface across all retrieve functions
3. ✓ Proper error handling with helpful messages
4. ✓ Comprehensive test coverage
5. ✓ Backward compatible - no breaking changes
6. ✓ Well documented with examples

## Commits
1. Initial exploration and planning
2. Add version parameter to all get_* retrieve functions
3. Add comprehensive tests for version parameter
4. Add demo script for version parameter usage
5. Fix misleading comments based on code review feedback
6. Final validation - all tests passing

## Summary
The implementation successfully adds version selection capability to all retrieve functions while maintaining full backward compatibility. The changes are minimal, well-tested, and follow the existing patterns in the codebase.
