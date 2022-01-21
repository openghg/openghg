[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![codecov](https://codecov.io/gh/openghg/openghg/branch/devel/graph/badge.svg)](https://codecov.io/gh/openghg/openghg) ![OpenGHG tests](https://github.com/openghg/openghg/workflows/OpenGHG%20tests/badge.svg?branch=master)

## OpenGHG - a cloud platform for greenhouse gas data analysis and collaboration

OpenGHG is a project based on the prototype [HUGS platform](https://www.hugs-cloud.com) which aims to be a platform for collaboration and analysis
of greenhouse gas (GHG) data.

The platform will be built on open-source technologies and will allow researchers to collaborate on large datasets by harnessing the
power and scalability of the cloud.

For more information please see [our documentation](https://docs.openghg.org/).

## Cloud

You can login to our [OpenGHG Cloud JupyterHub](https://hub.openghg.org) and use OpenGHG in the cloud. This will allow you to use the full power of OpenGHG from your local device. Once you're logged in please checkout [some of our tutorials](https://docs.openghg.org/tutorials/index.html) to help you get started.

## Install locally

To run OpenGHG locally you'll need Python 3.7 or later on Linux or MacOS, we don't currently support Windows.

### Clone Acquire

First we need to clone [Acquire](https://github.com/openghg/acquire) is required to be in the parent directory. To do this

```
$ git clone https://github.com/openghg/acquire.git
```

### Install OpenGHG

Next, in the same directory do

```
$ git clone https://github.com/openghg/openghg.git
$ cd openghg
$ pip install .
```

### Set environment variable

OpenGHG expects an environment variable `OPENGHG_PATH` to be set. This tells OpenGHG where to place the local object store.

Please add the following line to your shell profile (`~/.bashrc`, `~/.profile`, ...).

```
OPENGHG_PATH=/your/selected/path
```

### Run the tests

Making sure you're in the `openghg` directory we need to install the developer requirements, this makes sure we have everything
we need to run the tests.

```
$ pip install -r requirements-dev.txt
```

Then

```
$ pytest -v tests/
```

> **_NOTE:_**  Some of the tests require the [udunits2](https://www.unidata.ucar.edu/software/udunits/) library to be installed.

The `udunits` package is not `pip` installable but if you're on Debian / Ubuntu you can do

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

