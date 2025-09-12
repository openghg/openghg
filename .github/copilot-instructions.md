# OpenGHG Development Instructions

OpenGHG is a Python-based platform for greenhouse gas data analysis and collaboration. This repository contains the core OpenGHG package with scientific libraries for data processing, standardization, and analysis.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Environment Setup - PREFERRED APPROACH
**Recommended Development Setup**: Use pip for full development environment setup with micromamba for environment management:

```bash
# Create environment with micromamba (preferred over conda)
micromamba create --name openghg_dev python=3.12 -y

# Activate environment  
micromamba activate openghg_dev

# Install development dependencies (preferred approach)
pip install -r requirements.txt -r requirements-dev.txt
pip install -e .
```

### Alternative Setup (Minimal Environment)
If you encounter network issues or prefer a minimal setup for code quality validation only:

```bash
# Basic environment setup with micromamba
micromamba create --name openghg_dev python=3.12 pytest black flake8 mypy -y

# Activate environment  
micromamba activate openghg_dev

# Install core scientific packages
micromamba install -c conda-forge pandas xarray numpy tinydb toml rich msgpack-python -y
```

### Code Quality and Validation
These commands work reliably with the full development environment:

```bash
# Code formatting check
black --check openghg/

# Fix code formatting
black openghg/

# Lint code
flake8 openghg/ --count --statistics

# Type checking
mypy --python-version 3.12 openghg/
```

### Alternative Environment Setup (Conda/Older Systems)
If micromamba is not available, you can fall back to conda:

```bash  
# Full conda environment (may take longer)
conda env create -f environment-dev.yaml

# Alternative conda-based setup
conda create --name openghg_dev python=3.12 -y
conda activate openghg_dev
pip install -r requirements.txt -r requirements-dev.txt
pip install -e .
```

### Testing and Package Functionality
With the full development environment installed, you can run targeted tests:

```bash
# Run individual test files (preferred approach)
pytest -v tests/util/test_config.py
pytest -v tests/standardise/test_surface.py

# Run tests for a specific module
pytest -v tests/dataobjects/

# OpenGHG CLI commands
python -m openghg --help
python -c "import openghg"

# Note: Avoid tox for development as it can be quite slow
# Use direct pytest commands instead of tox -e lint or tox -e type
```

### Testing Best Practices
**Prefer targeted testing over full test suite runs:**

```bash
# Good: Run tests related to your changes
pytest -v tests/standardise/test_surface.py  # Single test file
pytest -v tests/dataobjects/                 # Module directory

# Avoid: Running the entire test suite (slow and unnecessary for most development)
# pytest tests/  # This runs all 92 test files and can be very slow
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
- **Tox environments**: `tox.ini` (lint, type, test environments - avoid for local development due to slowness)

### Documentation Build
```bash
# Install additional documentation dependencies
pip install -r requirements-doc.txt

# Build docs (requires full OpenGHG installation)
# Note: This is quite slow as the docs use the sphinx jupyter directive,
# which downloads tutorial data during the build process
cd doc && make html
```

**Documentation Sites:**
- Main documentation: https://docs.openghg.org
- Getting started, tutorials, and releases: https://openghg.org

## Validation Workflow

For standard development with full installation:
1. `black --check openghg/` - Format validation
2. `flake8 openghg/ --count --statistics` - Linting  
3. `mypy --python-version 3.12 openghg/` - Type checking
4. `pytest tests/specific_module/` or `pytest tests/specific_test_file.py` - Run targeted tests (preferred over full suite)

For minimal environment setup:
1. Use the alternative minimal setup above
2. Validate formatting and linting only
3. Rely on CI for full integration testing

## Repository Structure

Key directories and files:
- `openghg/` - Main Python package (17 submodules)
- `tests/` - Comprehensive test suite (17 test directories)
- `requirements*.txt` - Pip dependency specifications
- `environment*.yaml` - Conda/micromamba environment specifications  
- `pyproject.toml` - Modern Python project configuration
- `tox.ini` - Testing automation configuration (mainly for CI - avoid for local development)
- `.github/workflows/` - CI/CD pipeline definitions
- `doc/` - Sphinx documentation source

## Critical Timing and Network Considerations

**Standard operations (recommended):**
- Micromamba environment creation: ~18 seconds
- Pip dependency installation: Variable, typically 2-5 minutes
- Full development setup: 5-10 minutes total

**Alternative conda operations:**
- Conda environment creation: 18 seconds (minimal) to 20+ minutes (full)
- Conda package installation: 20-35 seconds per batch

**Note**: In CI or constrained network environments, pip installations may occasionally timeout. The alternative minimal setup provides a reliable fallback for code quality validation.

## Known Issues and Limitations

1. **Network timeouts**: Occasionally pip installs may timeout in CI environments (use alternative setup)
2. **Complex dependencies**: Full conda solve can take 20+ minutes  
3. **Import dependencies**: Package has circular imports requiring full dependency tree
4. **System requirements**: Some features need `libudunits2-0`, `xesmf` C libraries
5. **Test isolation**: Tests require complex helper infrastructure and full package

## Development Workflow

**For most code changes:**
1. Use preferred pip-based development environment setup
2. Edit code files
3. Run black, flake8, mypy validation  
4. Run relevant tests with pytest (individual test files or modules, not full suite)
5. Submit changes

**For minimal validation only:**
1. Use alternative minimal micromamba environment setup
2. Edit code files
3. Run black, flake8, mypy validation
4. Submit changes (CI will handle full testing)

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
# Run individual test files (preferred)
pytest tests/util/test_config.py
pytest tests/standardise/test_surface.py

# Run tests for specific modules
pytest tests/dataobjects/
pytest tests/standardise/

# CF compliance tests (requires libudunits2-0)
pytest -v --run-cfchecks tests/specific_test_file.py

# ICOS data tests (requires network, run sparingly)
pytest -v --run-icos tests/specific_test_file.py

# Specific test timeouts configured (300 seconds default)
pytest --timeout=300 tests/specific_module/
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
- **Micromamba/Conda environments**: `environment.yaml`, `environment-dev.yaml`
- **Dependencies**: `requirements*.txt` files
- **CI/CD**: `.github/workflows/workflow.yaml`
- **Quality tools**: `.pre-commit-config.yaml`, `tox.ini` (CI only - avoid for local dev), `mypy.ini`

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
4. Reference documentation sites:
   - Main docs: https://docs.openghg.org
   - Getting started and tutorials: https://openghg.org

## Summary for Coding Agents

**PREFERRED approach**: Use pip-based development environment with micromamba for environment management.

**Use these commands for standard development:**
- Environment: `micromamba create --name openghg_dev python=3.12` + pip installs
- Full development: `pip install -r requirements.txt -r requirements-dev.txt && pip install -e .`
- Code quality: black, flake8, mypy  
- Testing: `pytest tests/module/` or `pytest tests/test_file.py` (prefer individual tests over full suite)

**ALTERNATIVE approach for minimal validation:**
- Minimal micromamba environment with core packages
- Code quality validation only: black, flake8, mypy
- Submit changes and rely on CI for full testing

**Prefer micromamba over conda** for faster environment creation and dependency resolution.