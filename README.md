![OpenGHG logo](https://github.com/openghg/logo/raw/main/OpenGHG_Logo_Landscape.png)

## OpenGHG - a cloud platform for greenhouse gas data analysis and collaboration

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![codecov](https://codecov.io/gh/openghg/openghg/branch/devel/graph/badge.svg)](https://codecov.io/gh/openghg/openghg) ![OpenGHG tests](https://github.com/openghg/openghg/workflows/OpenGHG%20tests/badge.svg?branch=master)

OpenGHG is a project based on the prototype [HUGS platform](https://www.hugs-cloud.com) which aims to be a platform for collaboration and analysis
of greenhouse gas (GHG) data.

The platform will be built on open-source technologies and will allow researchers to collaborate on large datasets by harnessing the
power and scalability of the cloud.

For more information please see [our documentation](https://docs.openghg.org/).

## Cloud

You can login to our [OpenGHG Cloud JupyterHub](https://hub.openghg.org) and use OpenGHG in the cloud. This will allow you to use the full power of OpenGHG from your local device. Once you're logged in please checkout [some of our tutorials](https://docs.openghg.org/tutorials/index.html) to help you get started.

## Install locally

To run OpenGHG locally you'll need Python 3.8 or later on Linux or MacOS, we don't currently support Windows.

You can install OpenGHG using `pip` or `conda`, though `conda` allows the complete functionality to be accessed at once.

## Using `pip`

To use `pip`, first create a virtual environment

```bash
python -m venv openghg_env
```

Then activate the environment

```bash
source openghg_env/bin/activate
```

It's best to make sure you have the most up to date versions of the packages that `pip` will use behind the scenes when installing OpenGHG.

```bash
pip install --upgrade pip wheel setuptools
```

Then we can install OpenGHG itself

```bash
pip install openghg
```

Each time you use OpenGHG please make sure to activate the environment using the `source` step above.


> **_NOTE:_**  Some functionality is not completely accessible when OpenGHG is installed with `pip`. This only affects some map regridding functionality. See the Additional Functionality section below for more information.

## Using `conda`

To get OpenGHG installed using `conda` we'll first create a new environment

```bash
conda create --name openghg_env
```

Then activate the environment

```bash
conda activate openghg_env
```

Then install OpenGHG and its dependencies from our [conda channel](https://anaconda.org/openghg/openghg)
and conda-forge.

```bash
conda install --channel conda-forge --channel openghg openghg
```

Note: the `xesmf` library is already incorporated into the conda install from vx.x onwards and so does not need to be installed separately.

**Additional functionality**

Some optional functionality is available within OpenGHG to allow for multi-dimensional regridding of map data (`openghg.tranform` sub-module). This makes use of the [`xesmf` package](https://xesmf.readthedocs.io/en/latest/). This Python library is built upon underlying FORTRAN and C libraries (ESMF) which cannot be installed directly within a Python virtual environment.

To use this functionality these libraries must be installed separately. One suggestion for how to do this is as follows.

If still within the created virtual environment, exit this using
```bash
deactivate
```

We will need to create a `conda` environment to contain just the additional C and FORTRAN libraries necessary for the `xesmf` module (and dependencies) to run. This can be done by installing the `esmf` package using `conda`
```bash
conda create --name openghg_add esmf -c conda-forge
```

Then activate the Python virtual environment in the same way as above:
```bash
source openghg_env/bin/activate
```

Run the following lines to link the Python virtual environment to the installed dependencies, doing so by installing the `esmpy` Python wrapper (a dependency of `xesmf`):
```bash
ESMFVERSION='v'$(conda list -n openghg_add esmf | tail -n1 | awk '{print $2}')
$ export ESMFMKFILE="$(conda env list | grep openghg_add | awk '{print $2}')/lib/esmf.mk"
$ pip install "git+https://github.com/esmf-org/esmf.git@${ESMFVERSION}#subdirectory=src/addon/ESMPy/"
```

**Note**: The pip install command above for `esmf` module may produce an AttributeError. At present (19/07/2022) an error of this type is expected and may not mean the `xesmf` module cannot be installed. This error will be fixed if [PR #49](https://github.com/esmf-org/esmf/pull/49) is merged.

Now the dependencies have all been installed, the `xesmf` library can be installed within the virtual environment

```bash
pip install xesmf
```

## Setting the object store path

On first import OpenGHG will create a configuration file at `~/.config/openghg/openghg.conf`. This file contains the path to the object store, which by default is set to `~/openghg_store`.

The contents of the file will look like this

```toml
[object_store]
local_store = "/home/your_username/openghg_store"
```

or on macOS

```toml
[object_store]
local_store = "/Users/your_username/openghg_store"
```

Change this path to set the object store path.

## Developers

If you'd like to contribute to OpenGHG please see the contributing section of our documentation. If you'd like to take a look at the source and run the tests follow the steps below.

### Clone

```bash
git clone https://github.com/openghg/openghg.git
```

### Install dependencies

We recommend you create a virtual environment first

```bash
python -m venv openghg_env
```

Then activate the environment

```bash
source openghg_env/bin/activate
```

Then install the dependencies

```bash
cd openghg
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt -r requirements-dev.txt
```

Next you can install OpenGHG in editable mode using the `-e` flag. This installs the package from
the local path and means any changes you make to the code will be immediately available when
using the package.

```bash
pip install -e .
```

OpenGHG should now be installed in your virtual environment.

See above for additional steps to install the `xesmf` library as required.

### Run the tests

To run the tests

```bash
pytest -v tests/
```

> **_NOTE:_**  Some of the tests require the [udunits2](https://www.unidata.ucar.edu/software/udunits/) library to be installed.

The `udunits` package is not `pip` installable so we've added a separate flag to specifically run these tests. If you're on Debian / Ubuntu you can do

```bash
sudo apt-get install libudunits2-0
```

You can then run the `cfchecks` marked tests using

```bash
pytest -v --run-cfchecks tests/
```

If all the tests pass then you're good to go. If they don't please [open an issue](https://github.com/openghg/openghg/issues/new) and let us
know some details about your setup.

## Documentation

For further documentation and tutorials please visit [our documentation](https://docs.openghg.org/).

## Community

If you'd like further help or would like to talk to one of the developers of this project, please join
our Gitter at gitter.im/openghg/lobby.
