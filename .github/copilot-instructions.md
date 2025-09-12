# OpenGHG Development Instructions

OpenGHG is a Python-based platform for greenhouse gas data analysis and collaboration. This repository contains the core OpenGHG package with scientific libraries for data processing, standardization, and analysis.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Environment Setup - CRITICAL NOTES
**NETWORK DEPENDENCY WARNING**: This project has severe network timeout issues in CI/constrained environments:
- Full pip installs (`pip install -r requirements-dev.txt`) FAIL consistently due to 15+ minute timeouts
- Full conda environment creation (`conda env create -f environment-dev.yaml`) takes 20+ minutes and often gets killed  
- Even `tox` environments fail due to pip timeout issues

### Recommended Development Setup (Minimal)
Use this approach for code quality validation without full package installation:

```bash
# Basic environment setup - NEVER CANCEL, takes ~18 seconds
conda create --name openghg_dev python=3.12 pytest black flake8 mypy -y

# Activate environment  
eval "$(conda shell.bash hook)" && conda activate openghg_dev

# Install core scientific packages - NEVER CANCEL, takes ~35 seconds
conda install -c conda-forge pandas xarray numpy tinydb toml rich msgpack-python -y
```

### Code Quality and Validation - ALWAYS WORKS
These commands work reliably in the minimal environment:

```bash
# Code formatting check - takes ~2 seconds
black --check openghg/

# Fix code formatting
black openghg/

# Lint code - takes ~1 second  
flake8 openghg/ --count --statistics

# Type checking - takes ~3 seconds (will show import errors for missing deps)
mypy --python-version 3.12 openghg/
```

### Full Installation Attempts (Often Fail)
**ONLY attempt these if you have reliable network and 60+ minute timeouts:**

```bash  
# Full conda environment - takes 20+ minutes, may fail
conda env create -f environment-dev.yaml

# Full pip development install - frequently fails with timeouts
pip install -r requirements-dev.txt
pip install -e .

# Tox environments - fail due to pip timeouts inside tox
tox -e lint  # DO NOT use - fails with pip timeouts
tox -e type  # DO NOT use - fails with pip timeouts
```

### Testing and Package Functionality
**CRITICAL**: Full test suite and package imports require complete dependency installation which often fails:

```bash
# These WILL FAIL without full installation:
pytest -v tests/                    # Requires all scientific dependencies
python -m openghg --help           # Requires addict and other dependencies  
python -c "import openghg"         # Requires full dependency tree
```

### Special Test Categories
The project has special pytest markers for optional tests:
```bash
# CF compliance tests (require system package libudunits2-0)
pytest -v --run-cfchecks tests/

# ICOS tests (require network access, run sparingly) 
pytest -v --run-icos tests/
```

### CI/CD and Automation
- **GitHub Actions**: `.github/workflows/workflow.yaml`
- **Pre-commit hooks**: `.pre-commit-config.yaml` 
- **Tox environments**: `tox.ini` (lint, type, test environments)

### Documentation Build
```bash
# Documentation requirements
pip install -r requirements-doc.txt

# Build docs (requires full OpenGHG installation)
cd doc && make html
```

## Validation Workflow

For code changes without full installation:
1. `black --check openghg/` - Format validation (2 seconds)
2. `flake8 openghg/ --count --statistics` - Linting (1 second)  
3. `mypy --python-version 3.12 openghg/` - Type checking (3 seconds)

**NEVER rely on full package installation in CI environments** - use the minimal setup above.

## Repository Structure

Key directories and files:
- `openghg/` - Main Python package (17 submodules)
- `tests/` - Comprehensive test suite (17 test directories)
- `requirements*.txt` - Pip dependency specifications
- `environment*.yaml` - Conda environment specifications  
- `pyproject.toml` - Modern Python project configuration
- `tox.ini` - Testing automation configuration
- `.github/workflows/` - CI/CD pipeline definitions
- `doc/` - Sphinx documentation source

## Critical Timing and Timeout Requirements

**NEVER CANCEL these operations:**
- Conda environment creation: 18 seconds (minimal) to 20+ minutes (full)
- Conda package installation: 20-35 seconds per batch
- Pip installs: 15+ minutes before timeout (often fail)
- Full environment resolution: 20+ minutes (may get killed)

**Set timeouts to 60+ minutes for:**
- Any conda env create operations
- Any pip install operations  
- Any tox environment setup

**Fast operations (1-3 seconds):**
- Code formatting and linting with existing tools
- Basic Python syntax validation

## Known Issues and Limitations

1. **Network timeouts**: Pip installs fail consistently in CI environments
2. **Complex dependencies**: Full conda solve takes 20+ minutes and may fail  
3. **Import dependencies**: Package has circular imports requiring full dependency tree
4. **System requirements**: Some features need `libudunits2-0`, `xesmf` C libraries
5. **Test isolation**: Tests require complex helper infrastructure and full package

## Development Workflow

**For most code changes:**
1. Use minimal conda environment setup
2. Edit code files
3. Run black, flake8, mypy validation  
4. Submit changes (CI will handle full testing)

**For testing changes:**
1. Attempt full installation if network permits (60+ minute timeout)
2. Run specific test files: `pytest tests/specific_test.py`
3. Use CI for comprehensive validation

## CLI and Configuration

**OpenGHG CLI** (requires full installation):
```bash
# Create config file
openghg --quickstart

# Default config
openghg --default-config
```

**Configuration location**: `~/.config/openghg/openghg.conf`

## Detailed Repository Structure

### Main Package Structure (150 Python files)
```
openghg/
├── analyse/        # Data analysis and modeling scenarios
├── data/           # Data handling and configuration files  
├── data_processing/ # Processing and resampling utilities
├── dataobjects/    # Core data object classes (ObsData, FootprintData, etc.)
├── datapack/       # Data packaging and distribution
├── objectstore/    # Object storage and metastore functionality
├── plotting/       # Visualization utilities
├── retrieve/       # Data retrieval and export functionality
├── service/        # Service layer interfaces
├── standardise/    # Data standardization modules (9 submodules)
├── store/          # Data storage operations
├── transform/      # Data transformation utilities
├── tutorial/       # Tutorial and example data
├── types/          # Type definitions and schemas
└── util/           # Utility functions and CLI interface
```

### Test Structure (92 test files)
```
tests/
├── analyse/        # Analysis module tests
├── data/           # Test data files (13 subdirectories)
├── data_processing/ # Processing tests
├── dataobjects/    # Data object tests
├── datapack/       # Data packaging tests
├── db_integrity/   # Database integrity tests
├── helpers/        # Test helper utilities
├── objectstore/    # Object store tests
├── retrieve/       # Data retrieval tests
├── standardise/    # Standardization tests (9 subdirectories)
├── store/          # Storage operation tests
├── transform/      # Transform utility tests
├── tutorial/       # Tutorial tests
├── types/          # Type definition tests
└── util/           # Utility function tests
```

## Common Development Patterns

### Key Import Patterns
```python
# Common utility imports
from openghg.util import create_config, cli
from openghg.dataobjects import ObsData, FootprintData, FluxData
from openghg.objectstore import get_object_store
from openghg.standardise import standardise_surface

# Testing imports (with full installation)
from helpers import clear_test_stores, get_info_datapath
```

### Configuration and Data Paths
- Object store config: `~/.config/openghg/openghg.conf`
- Default object store: `~/openghg_store`
- Test data: `tests/data/` (extensive test datasets)
- Package data: `openghg/data/`

### Test Categories and Markers
```bash
# Standard tests
pytest tests/

# CF compliance tests (requires libudunits2-0)
pytest -v --run-cfchecks tests/

# ICOS data tests (requires network, run sparingly)
pytest -v --run-icos tests/

# Specific test timeouts configured (300 seconds default)
pytest --timeout=300 tests/
```

## File Patterns and Conventions

### Code Organization
- **Main modules**: Each subdirectory in `openghg/` is a functional module
- **Test structure**: Mirrors main package structure exactly
- **Imports**: Heavy interdependencies between modules
- **Configuration**: TOML-based configuration files
- **Data handling**: Extensive use of xarray, pandas, netCDF4

### Common File Locations for Changes
- **Adding new standardizers**: `openghg/standardise/`
- **Data object modifications**: `openghg/dataobjects/`  
- **Utility functions**: `openghg/util/`
- **Object store changes**: `openghg/objectstore/`
- **Test helpers**: `tests/helpers/`

### Configuration Files
- **Project config**: `pyproject.toml` (modern Python packaging)
- **Legacy setup**: `setup.cfg` (minor configuration)
- **Conda environments**: `environment.yaml`, `environment-dev.yaml`
- **Dependencies**: `requirements*.txt` files
- **CI/CD**: `.github/workflows/workflow.yaml`
- **Quality tools**: `.pre-commit-config.yaml`, `tox.ini`, `mypy.ini`

## Manual Validation Scenarios

Since automated testing often fails due to network constraints, use these manual validation approaches:

### Code Quality Validation (Always Works)
1. Check formatting: `black --check openghg/`
2. Check linting: `flake8 openghg/ --count --statistics`  
3. Check types: `mypy --python-version 3.12 openghg/`

### File-level Validation
1. Check import structure: `python -c "import ast; ast.parse(open('file.py').read())"`
2. Check syntax: `python -m py_compile openghg/module/file.py`
3. Manual inspection of test structure alignment

### Documentation Verification
1. Check docstrings are present in new functions
2. Verify README.md updates for API changes
3. Check that new modules are documented

## Summary for Coding Agents

**ALWAYS start with minimal conda environment setup** - never attempt full pip installations in CI environments.

**Use these reliable commands for validation:**
- Environment: 18-second conda environment creation
- Code quality: black (2s), flake8 (1s), mypy (3s)  
- Manual verification: syntax checking, import testing

**NEVER attempt in CI environments:**
- Full pip installations (15+ minute timeouts)
- Pre-commit hook installation (fails with pip timeouts)
- Tox environment setup (fails with pip timeouts)
- Full conda environment creation (20+ minutes, may fail)

**For testing changes:** Submit code with basic validation - let CI handle full integration testing with properly configured network and timeout settings.