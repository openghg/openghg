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

You can install OpenGHG using `pip` or `conda`. We recommend you create a virtual environment first

#### pip

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

#### conda

Create a conda environemnt called `openghg_env` and enable the use of conda-forge

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
