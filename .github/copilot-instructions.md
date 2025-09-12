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