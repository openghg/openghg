=========================
Installation instructions
=========================

The easiest way to install OpenGHG is using pip, first ensure you have a recent version of
Python installed.

Checking your Python installation
=================================

OpenGHG is developed and `tested on Linux and MacOS <https://github.com/openghg/openghg/actions>`__,
support for Windows is planned.

To install OpenGHG, you first need to install Python >= 3.7. To check
if you have Python 3.7 installed type;

.. code-block:: bash

    python -V

This should print out the version number of the Python you have available.
If the version number is ``2.x`` then you must use the ``python3`` command, if this this is the case, try;

.. code-block:: bash

    python3 -V

and see if you have a Python 3 that has a version number >= 3.7. If so, please use ``python3`` instead of ``python``.

If you don't have Python >=3.7 installed, then you can install Python either via your package manager if using Linux or
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
    # Activate the environment
    conda activate openghg_env

Next install OpenGHG from our conda channel

.. code-block:: bash

    conda install --channel conda-forge --channel openghg openghg


Developers
==========

For developers please see the :doc:`development/python_devel` documentation.
