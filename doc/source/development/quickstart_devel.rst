======================
Quickstart - developer
======================

This quickstart guide will get you quickly setup with a development environment so you can use and contribute to OpenGHG.
The main repository for OpenGHG can be found on `GitHub <https://github.com/openghg/openghg>`__.

Setting up your computer
=========================

You'll need `git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`_ and Python >= 3.10, so please make sure you have both installed before continuing
further.

Clone OpenGHG
-------------

First we'll clone the repository and make sure we're on the ``devel`` branch. This makes sure we're on the most up to date version of OpenGHG.

.. code-block:: bash

   git clone https://github.com/openghg/openghg.git
   cd openghg
   git checkout devel

Next we'll set up a development environment using ``pixi``, ``uv``, or ``conda``.

Environments
------------

Here we cover the creation of an environment and the installation of OpenGHG into it. Installation here means adding OpenGHG to the environment.
We'll install it in developer mode so that any changes you make to the code will automatically be available when you run commands. Similarly, if you
run a ``git pull`` on the ``devel`` branch all changes made will be available to you straight away, without having to reinstall or update OpenGHG within
the environment.

``pixi``
^^^^^^^^

Pixi is the recommended development environment when working with
NetCDF, HDF5, or Zarr data. It installs the compiled scientific,
HDF5, and NetCDF stack from ``conda-forge`` and keeps this OpenGHG
checkout editable.

Install Pixi directly with one of the following commands.

On macOS or Linux, use the official installer:

.. code-block:: bash

   curl -fsSL https://pixi.sh/install.sh | sh

If ``curl`` is unavailable, use ``wget``:

.. code-block:: bash

   wget -qO- https://pixi.sh/install.sh | sh

On macOS with Homebrew:

.. code-block:: bash

   brew install pixi

Then create the editable OpenGHG development environment from this
checkout:

.. code-block:: bash

   pixi install --locked -e dev
   pixi run --locked -e dev python -c "import openghg, h5py, h5netcdf, netCDF4, xarray, zarr"

Useful development commands:

.. code-block:: bash

   pixi run --locked -e dev test
   pixi run --locked -e dev test-storage
   pixi run --locked -e dev lint
   pixi run --locked -e dev typecheck

Avoid running commands such as ``pip install -U h5py h5netcdf netcdf4``
inside the Pixi environment. That can replace Pixi's conda-forge
HDF5/NetCDF packages with PyPI wheels and reintroduce binary
incompatibilities.

OpenGHG should now be installed, you can check this by opening ``ipython`` and running

.. code-block:: ipython

   In [1]: import openghg

``uv``
^^^^^^

For routine development that does not require Pixi's compiled scientific
stack, `uv <https://docs.astral.sh/uv/>`__ provides a fast project environment.
From the repository root, create ``.venv`` and install OpenGHG in editable mode
with the locked development dependencies:

.. code-block:: bash

   uv sync --extra dev --locked

You can activate this environment and run tools directly:

.. code-block:: bash

   source .venv/bin/activate
   pytest tests/path/to/test_file.py

Alternatively, leave the shell unchanged and use ``uv run`` to select the same
environment for each command:

.. code-block:: bash

   uv run --no-sync pytest tests/path/to/test_file.py

After the explicit locked sync, ``--no-sync`` prevents validation commands from
changing the environment or lock file. Activation is usually more convenient
for an interactive terminal and editor; ``uv run`` is useful for automation and
tools whose command invocations use separate shells. Both routes use the same
``.venv``. A virtual environment isolates Python packages; it is not a security
sandbox.

``conda``
^^^^^^^^^

Making sure you're in the ``openghg`` repository folder run

.. code-block:: bash

   conda env create -f environment.yaml -f environment-dev.yaml

Once ``conda`` finishes its installation process you can activate the environment:


.. code-block:: bash

   conda activate openghg_dev_env

The combined environment files include ``conda-build``. Install OpenGHG in
development mode:

.. code-block:: bash

   conda develop .

Now OpenGHG is installed we'll move onto configuring the object store.

Configuration
=============

OpenGHG needs to know where to create the object store it uses to store data, it does this by reading a configuration file in your home
directory. As part of the setup process we need to create this configuration file using either the `openghg.util.create_config` function
or the command line interface.

Python
------

You can use the `create_config` function to help you make a config file. First import

.. code-block:: ipython3

    In [1]: from openghg.util import create_config

    In [2]: create_config()

    OpenGHG configuration
    ---------------------

    Enter path for object store (default /home/gareth/openghg_store):
    INFO:openghg.util:Creating config at /home/gareth/.config/openghg/openghg.conf

    INFO:openghg.util:Configuration written to /home/gareth/.config/openghg/openghg.conf

Here I left the path to the object store blank to use the default path in my home directory.

Command line
------------

You can also use the `openghg` command line tool to get the configuration file setup.

.. code-block:: bash

    openghg --quickstart

    OpenGHG configuration
    ---------------------

    Enter path for object store (default /home/gareth/openghg_store):
    INFO:openghg.util:Creating config at /home/gareth/.config/openghg/openghg.conf

    INFO:openghg.util:Configuration written to /home/gareth/.config/openghg/openghg.conf

A configuration file has been created and you're ready to run OpenGHG. If you ever want to modify the configuration file
you can find it at ``~/.config/openghg/openghg.conf``. My configuration file looks like this

.. code-block:: toml

    user_id = "47363762-2963-4a2d-8afc-dejh05380f19"

    [object_store]
    local_store = "/home/gareth/openghg_store"


Jupyter
=======

If you want to use Jupyter notebooks to interact with OpenGHG, you can install ``jupyterlab``.

.. code-block:: bash

   pip install jupyterlab

Then just run ``jupyter notebook`` to get started.

What next?
==========

Now you've got OpenGHG setup please take a look at our :ref:`tutorials section<Tutorials>` and if you'd like to start
contributing to OpenGHG `open an issue <https://github.com/openghg/openghg/issues>`_ and submit a pull request!
