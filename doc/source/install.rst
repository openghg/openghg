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
If the version number is ``2.???`` then you have a very old Python 2. If
this is the case, try;

.. code-block:: bash

    python3 -V

and see if you have a Python 3 that has a version number >= 3.7. If so,
please use ``python3`` instead of ``python``.

If you don't have Python >=3.7 installed, then you can install Python
either via your package manager if using Linux or `Homebrew on MacOS <https://docs.brew.sh/Homebrew-and-Python>`__.
An alternative for both platforms is `anaconda <https://anaconda.org>`__.

Installation
============

OpenGHG is currently very early in its development process so the only way to install
the library is by cloning the repository and installing manually using pip.

.. code-block:: bash

   git clone https://github.com/openghg/openghg.git
   cd openghg
   pip install . 


For developers
==============

You can clone the Pack and Doc repository to your computer and install from
there;

.. code-block:: bash

    git clone https://github.com/openghg/openghg
    cd openghg
    pip install -r requirements.txt
    pip install -r requirements-dev.txt

From this point you can compile as if you have downloaded from source.
As a developer you may want to run the tests and build the documentation.
To do this type;

.. code-block:: bash

    pytest tests
    cd doc
    make

