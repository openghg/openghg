# OpenGHG Development Instructions

OpenGHG is a Python-based platform for greenhouse gas data analysis and collaboration. This repository contains the core OpenGHG package with scientific libraries for data processing, standardization, and analysis.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Initial Setup
- Bootstrap environment and dependencies:
  - `python --upgrade pip wheel setuptools` -- takes ~5 seconds
  - `conda create --name openghg_dev python=3.12 pytest black flake8 mypy -y` -- takes ~18 seconds. NEVER CANCEL.
  - `eval "$(conda shell.bash hook)" && conda activate openghg_dev`
  - `conda install -c conda-forge pandas xarray numpy tinydb toml -y` -- takes ~35 seconds. NEVER CANCEL.

### Development Environment Options
**CRITICAL**: Full conda environment setup using `environment-dev.yaml` takes 20+ minutes and may fail due to complex dependencies. Use the simplified setup above for development work.

**Alternative pip setup** (may fail due to network timeouts):
- `pip install -r requirements-dev.txt` -- OFTEN FAILS due to network timeouts
- `pip install -e .` -- OFTEN FAILS due to network timeouts

### Code Quality and Testing
- Format code: `black --check openghg/` -- takes ~2 seconds
- Format code (fix): `black openghg/`
- Lint code: `flake8 openghg/ --count --statistics` -- takes ~1 second  
- Type check: `mypy --python-version 3.12 openghg/` -- takes ~3 seconds
- Run tests: `pytest -v tests/` -- requires full environment setup

**CRITICAL TIMING**: 
- NEVER CANCEL conda environment creation (may take 20+ minutes)
- NEVER CANCEL pip installs (may timeout after 15+ minutes)
- Set timeout to 60+ minutes for any environment setup commands
- Basic linting and formatting tools are fast (1-3 seconds)

### Pre-commit and CI/CD
- Pre-commit hooks: configured in `.pre-commit-config.yaml`
- Run pre-commit: `pre-commit run --all-files`
- CI workflow: `.github/workflows/workflow.yaml` runs black, flake8, mypy, pytest

## Validation
- ALWAYS run `black --check openghg/` and `flake8 openghg/` before committing
- Type checking with mypy will show import errors for missing dependencies (expected in minimal setup)
- Full test suite requires complete environment setup with all scientific dependencies

## Repository Structure
- `openghg/` - Main Python package
- `tests/` - Test suite  
- `requirements*.txt` - Pip dependency files
- `environment*.yaml` - Conda environment files
- `pyproject.toml` - Modern Python project configuration
- `.github/workflows/` - CI/CD configuration

## Known Issues
- Network timeouts common with pip installs in CI environments
- Full conda environment setup extremely slow and may fail
- Some dependencies (like `xesmf`) require system libraries and may need special setup
- CF compliance tests require `libudunits2-0` system package

## Common Development Tasks
- Code changes: Edit files, run `black` and `flake8`, test locally
- Adding dependencies: Update `pyproject.toml` and `requirements*.txt`
- Testing: Run specific test files with `pytest tests/path/to/test.py`

## Performance Notes
Command timing (in minimal environment):
- Environment creation: ~18 seconds  
- Dependency install: ~35 seconds
- Code formatting check: ~2 seconds
- Linting: ~1 second
- Type checking: ~3 seconds