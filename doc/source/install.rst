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

Virtual environment
-------------------

We highly recommend creating a separate virtual environment for ``openghg``. This ensures the correct versions
of libraries can be installed without making changes to versions of libraries needed for other projects / programs.

.. code-block:: bash

    python -m venv /path/to/env
    # Activate the environment
    source /path/to/end/bin/activate

Clone and install
-----------------

OpenGHG is currently very early in its development process so the only way to install
the library is by cloning the repository and installing manually using pip.

.. code-block:: bash

   git clone https://github.com/openghg/openghg.git
   cd openghg
   pip install . 

Upgrade
-------

To upgrade a currently installed version

.. code-block:: bash

    pip install --upgrade openghg

Developers
==========

For developers please follow the instructions for creation of a virtual environment above and then follow the instructions below.

.. code-block:: bash

    git clone https://github.com/openghg/openghg
    cd openghg
    pip install -r requirements.txt
    pip install -r requirements-dev.txt
    
Tests
-----

As a developer you may want to run the tests and add new tests for functions you'd like to 
contribute the project. To do this type:

.. code-block:: bash

    pytest tests

Documentation
-------------

To build the documentation you will need to install `pandoc <https://pandoc.org/>`__, a standalone document converter tool.
Please see the `pandoc installation instructions <https://pandoc.org/installing.html>`__.

You can now build the documentation.

.. code-block:: bash

    cd doc
    make


