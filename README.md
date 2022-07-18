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

To run OpenGHG locally you'll need Python 3.7 or later on Linux or MacOS, we don't currently support Windows.

### Install OpenGHG

You can install OpenGHG using `pip` or `conda`, though `conda` allows the complete functionality to be accessed at once.

If using `pip` or `conda`, we recommend creating a virtual environment first and installing `openghg` into this environment.

#### pip

To use `pip`, first create a virtual environment using the following
```
$ python -m venv openghg_env
```

Then activate the environment

```
$ source openghg_env/bin/activate
```

Then install OpenGHG

```
$ pip install openghg
```

This will allow the majority of functionality to be accessed but see below for more details on accessing optional regridding (`tranform`) functionality introduced in v.x.x.

**Additional functionality**

Some optional functionality is available within OpenGHG to allow for multi-dimensional regridding of map data (`openghg.tranform` sub-module). This makes use of the [`xesmf` package](https://xesmf.readthedocs.io/en/latest/). This Python library is built upon underlying FORTRAN and C libraries (ESMF) which cannot be installed directly within a Python virtual environment.

To use this functionality these libraries must be installed separately. One suggestion for how to do this is as follows.

If still within the created virtual environment, exit this using
```
$ deactivate
```

We will need to create a `conda` environment to contain just the additional C and FORTRAN libraries necessary for the `xesmf` module (and dependencies) to run. This can be done by installing the `esmf` package using `conda`
```
$ conda create --name openghg_add esmf -c conda-forge
```

Then activate the Python virtual environment in the same way as above:
```
$ source openghg_env/bin/activate
```

Run the following lines to link the Python virtual environment to the installed dependencies, doing so by installing the `esmpy` Python wrapper (a dependency of `xesmf`):
```
$ ESMFVERSION=$(conda list -n openghg_add esmf | tail -n1 | awk '{print $2}')
$ export ESMFMKFILE="$(conda env list | grep openghg_add | awk '{print $2}')/lib/esmf.mk"
$ pip install "git+https://github.com/esmf-org/esmf.git@${ESMFVERSION}#subdirectory=src/addon/ESMPy/"
```

Now the dependencies have all been installed, the `xesmf` library can be installed within the virtual environment
```
$ pip install xesmf
```

#### conda

Create a conda environment called `openghg_env` and enable the use of conda-forge

```
$ conda create --name openghg_env
```

Activate the environment

```
$ conda activate openghg_env
```

Then install OpenGHG and its dependencies from our [conda channel](https://anaconda.org/openghg/openghg)
and conda-forge.

```
$ conda install --channel conda-forge --channel openghg openghg
```

Note: the `xesmf` library is already incorporated into the conda install from vx.x onwards and so does not need to be installed separately.

### Set environment variable

OpenGHG expects an environment variable `OPENGHG_PATH` to be set. This tells OpenGHG where to place the local object store.

Please add the following line to your shell profile (`~/.bashrc`, `~/.profile`, ...).

```
OPENGHG_PATH=/your/selected/path
```

We recommend a path such as `/home/your_username/openghg_store`.

## Developers

If you'd like to contribute to OpenGHG please see the contributing section of our documentation. If you'd like to take a look at the source and run the tests follow the steps below.

### Clone

```
$ git clone https://github.com/openghg/openghg.git
```

### Install dependencies

We recommend you create a virtual environment first

```
$ python -m venv openghg_env
```

Then activate the environment

```
$ source openghg_env/bin/activate
```

Then install the dependencies

```
$ cd openghg
$ pip install --upgrade pip wheel
$ pip install -r requirements-dev.txt
```

See above for additional steps to install the `xesmf` library as required.

### Run the tests

To run the tests

```
$ pytest -v tests/
```

> **_NOTE:_**  Some of the tests require the [udunits2](https://www.unidata.ucar.edu/software/udunits/) library to be installed.

The `udunits` package is not `pip` installable so we've added a separate flag to specifically run these tests. If you're on Debian / Ubuntu you can do

```
$ sudo apt-get install libudunits2-0
```

You can then run the `cfchecks` marked tests using

```
$ pytest -v --run-cfchecks tests/
```

If all the tests pass then you're good to go. If they don't please [open an issue](https://github.com/openghg/openghg/issues/new) and let us
know some details about your setup.

## Documentation

For further documentation and tutorials please visit [our documentation](https://docs.openghg.org/).

## Community

If you'd like further help or would like to talk to one of the developers of this project, please join
our Gitter at gitter.im/openghg/lobby.
