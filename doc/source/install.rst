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

Installation with pip
=====================

Once you have a working Python >= 3.7, the easiest way to install
openghg is using
`pip <https://pip.pypa.io/en/stable/>`__.

.. code-block:: bash

    pip install openghg

(if this doesn't work, then you may need to use the command ``pip3``,
or you may have to `install pip <https://pip.pypa.io/en/stable/installing/>`__.

If you have trouble installing pip then we recommend that you download
and install `anaconda <https://anaconda.org>`__, which has pip included)

To install a specific version, e.g. 0.0.1, type

.. code-block:: bash

    pip install pack_and_doc==0.0.1

This will install a binary version of OpenGHG if it is avaiable for your
operating system / processor / version of python. 

Source install
==============

You can download a source release of Pack and Doc from the
`project release page <https://github.com/openghg/openghg/releases>`__.

Once you have downloaded the file you can unpack it and change into
that directory using;

.. code-block:: bash

   tar -zxvf openghg-X.Y.Z.tar.gz
   cd openghg-X.Y.Z

where ``X.Y.Z`` is the version you downloaded. For the 1.4.0 release
this would be;

.. code-block:: bash

    tar -zxvf openghg-0.0.1.tar.gz
    cd openghg-0.0.1

Next you need to install the dependencies of Pack and Doc. Do this by typing;

.. code-block:: bash

    pip install -r requirements.txt
    pip install -r requirements-dev.txt


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
    make doc

