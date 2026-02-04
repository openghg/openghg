![OpenGHG logo](https://github.com/openghg/logo/raw/main/OpenGHG_Logo_Landscape.png)

## OpenGHG - a Cloud Platform for Greenhouse Gas Data Analysis and Collaboration

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![OpenGHG tests](https://github.com/openghg/openghg/workflows/OpenGHG%20tests/badge.svg?branch=master)

OpenGHG is a platform for collaboration and analysis of greenhouse gas (GHG) data, inspired by the [HUGS platform](https://github.com/hugs-cloud/hugs). It allows researchers to analyze and collaborate on large datasets using the scalability of the cloud.

For more information, please visit [our documentation](https://docs.openghg.org/).

---

## Install OpenGHG

OpenGHG supports Python 3.8 and later on Linux or MacOS. To install the package, you can use either `uv` (recommended for its environment management abilities) or `conda`.

### Installing with `uv`

`uv` simplifies environment creation and dependency management, making it easy to manage your setup. To install OpenGHG using `uv`:

1. **Install `uv`:**

   The `uv` tool from Astral streamlines Python management, virtual environments, and package installation. Follow the recommended steps from the official `uv` docs:

   1. Install from link`uv` (Recommended "robust, Python-independent"):
   - macOS/Linux:
     ```bash
     curl -LsSf https://astral.sh/uv/install.sh | sh
     ```

   2. Install from pip (Alternative "requires existing Python + pip"):
   ```bash
   pip install uv
   ```

2. **Create and activate an environment for OpenGHG:**
   ```bash
   uv venv openghg-env
   ```
   Additionally, a specific python version can be specified while creating the
   environment as follows.
   ```bash
   uv venv openghg-env --python 3.11
   ```
   To activate:
   ```bash
   source openghg-env/bin/activate
   ```

3. **Install OpenGHG:**
   ```bash
   uv pip install openghg
   ```

This installs OpenGHG and all optional dependencies for full functionality.

### Installing with `conda`

To get OpenGHG installed using `conda`, follow these steps:

1. **Create and activate a `conda` environment:**
   ```bash
   conda create --name openghg_env
   conda activate openghg_env
   ```

2. **Install OpenGHG and its dependencies using the `conda-forge` and `openghg` channels:**
   ```bash
   conda install --channel conda-forge --channel openghg openghg
   ```

Note: The optional `xesmf` library is pre-installed when using `conda`. No additional steps are required for regridding functionality.

---

## Quickstart Configuration

Once OpenGHG is installed, you need to configure the object store and user data. OpenGHG stores its configuration file by default at:
`~/.config/openghg/openghg.conf`.

### Configure via CLI:
```bash
openghg --quickstart
```

### Configure via Python:
```python
from openghg.util import create_config

create_config()
```

When prompted, you can specify the path to the object store. Leave the field blank to use the default directory at `~/openghg_store`.

---

## Developers

If you'd like to contribute to OpenGHG, here are the steps to set up a development environment. You can use either `uv` or `conda`.

### Using `uv` for Development

1. **Clone the repository:**
   ```bash
   git clone https://github.com/openghg/openghg.git
   cd openghg
   ```

2. **Create and activate an environment for OpenGHG:**
   ```bash
   uv venv
   ```
   A python environment with name can also be created, as showed in non-developer instance previously.
   Additionally, a specific python version can be specified while creating the
   environment as follows.
   ```bash
   uv venv --python 3.11
   ```
> **Note:**
> If the virtual environment is not named, the .venv folder is  created at the directory level, and using commands like "uv add" or "uv pip install" will automatically detect the environment and install the packages.

   To activate:
   ```bash
   source .venv/bin/activate
   ```

3. **Install development dependencies and the package in editable mode:**
   ```bash
   uv sync --all-extras
   ```
   This ensures that the local repository is installed in **editable mode**, meaning changes to the source code are immediately reflected. It will also ensure that all the dev and documentation dependencies are installed in the environment.

   For more details, please refer to the [UV Documentation (sync)](https://docs.astral.sh/uv/concepts/projects/sync/#syncing-the-environment).

### Using `conda` for Development

1. **Clone the repository:**
   ```bash
   git clone https://github.com/openghg/openghg.git
   cd openghg
   ```

2. **Create and activate a `conda` environment:**
   ```bash
   conda create --name openghg-dev python=3.12
   conda activate openghg-dev
   ```

3. **Install development dependencies:**
   ```bash
   pip install --upgrade pip wheel setuptools
   pip install -e ".[dev]"
   ```

---

### Running Tests

OpenGHG uses `pytest` for testing. After setting up the development environment, you can run tests as follows:
```bash
pytest -v tests/
```

#### Additional Testing:

- **CF Checker Tests:** Install the `udunits2` library for certain tests:
   ```bash
   sudo apt-get install libudunits2-0
   pytest -v --run-cfchecks tests/
   ```

- **ICOS Tests:** These tests access the ICOS Carbon Portal and should be run sparingly:
   ```bash
   pytest -v --run-icos tests/
   ```

If you encounter issues, please [open a GitHub issue](https://github.com/openghg/openghg/issues/new).

---

## Additional Functionality

OpenGHG's optional functionality includes the `xesmf` module for map regridding.

- When using `uv`, these dependencies are installed automatically with the extras.
- When using `conda`, the `xesmf` library is included in the installation.

For further details, refer to [our documentation](https://docs.openghg.org/).

---

## Community and Contributions

We encourage contributions and are happy to assist where needed. Raise issues and pull requests in [our repository](https://github.com/openghg/openghg).

For further information, check out [our documentation](https://docs.openghg.org/).
