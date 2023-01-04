============
Installation
============

The easiest way to install OpenGHG is using pip, first ensure you have a recent version of
Python installed.

Checking your Python installation
=================================

OpenGHG is developed and `tested on Linux and MacOS <https://github.com/openghg/openghg/actions>`__,
support for Windows is planned.

To install OpenGHG, you first need to install Python >= 3.8. To check
if you have Python 3.8 installed type;

.. code-block:: bash

    python -V

This should print out the version number of the Python you have available.
If the version number is ``2.x`` then you must use the ``python3`` command, if this this is the case, try;

.. code-block:: bash

    python3 -V

and see if you have a Python 3 that has a version number >= 3.8. If so, please use ``python3`` instead of ``python``.

If you don't have Python >= 3.8 installed, then you can install Python either via your package manager if using Linux or
`Homebrew on MacOS <https://docs.brew.sh/Homebrew-and-Python>`__. An alternative for both platforms is `anaconda <https://anaconda.org>`__.

Installation
============

We highly recommend creating a separate virtual environment for ``openghg``. This ensures the correct versions
of libraries can be installed without making changes to versions of libraries needed for other projects / programs.

pip
---

First create a virtual environment

.. code-block:: bash

    python -m venv /path/to/env/openghg_env
    # Activate the environment
    source /path/to/env/bin/activate

Here change ``/path/to/env`` with your own path.

Next install OpenGHG

.. code-block:: bash

    pip install openghg


conda
-----

First create and activate a conda environment

.. code-block:: bash

    conda create --name openghg_env
    conda activate openghg_env

Next install OpenGHG from our conda channel

.. code-block:: bash

    conda install --channel conda-forge --channel openghg openghg


Configuration
=============

Once OpenGHG is installed use the `openghg-quickstart` command line tool to get your configuration file setup.

.. code-block:: bash

    openghg-quickstart

You should now see text telling you you haven't got a configuration file (unless you've run the quickstart previously)
and a prompt asking you enter a path for the object store. We'll just use the default path and hit return when prompted.

.. code-block:: bash

    No configuration file found, please see installation instructions.

    Please enter a path for the object store (default: /home/gareth/openghg_store):
    Creating config at /home/gareth/.config/openghg/openghg.conf


A configuration file has been created and you're ready to run OpenGHG. If you ever want to modify the configuration file
you can find it at ``~/.config/openghg/openghg.conf``. My configuration file looks like this

.. code-block:: toml

    user_id = "47363762-2963-4a2d-8afc-dejh05380f19"

    [object_store]
    local_store = "/home/gareth/openghg_store"

Deprecation of ``OPENGHG_PATH``
-------------------------------

If you've previously used OpenGHG and worked through our tutorials you might have encountered
the need to set the ``OPENGHG_PATH`` environment variable. Now that we've moved to a configuration
file this is not longer used. If you previously set a custom path using the variable please update
the configuration file as below.


Developers
==========

For developers please see the :doc:`development/python_devel` documentation.
