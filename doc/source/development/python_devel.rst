=============
Getting setup
=============

Here we'll cover getting your development environment setup for contributing to OpenGHG.
The source code for OpenGHG is available on `GitHub <https://github.com/openghg/openghg>`__.

Setting up your computer
=========================

You'll need `git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`_ and Python >= 3.8, so please make sure you have both installed before continuing
further.


Clone OpenGHG
-------------

First we'll clone the repository and make sure we're on the ``devel`` branch. This makes sure we're on the most up to date version of OpenGHG.

.. code-block:: bash

   git clone https://github.com/openghg/openghg.git
   cd openghg
   git checkout devel

Next we'll get a virtual environment setup using either ``pip`` or ``conda``.

Environments
------------

Here we cover the creation of an environment and the installation of OpenGHG into it. Installation here means adding OpenGHG to the environment.
We'll install it in developer mode so that any changes you make to the code will automatically be available when you run commands. Similarly, if you
run a ``git pull`` on the ``devel`` branch all changes made will be available to you straight away, without having to reinstall or update OpenGHG within
the environment.

``pip``
^^^^^^^

It is recommended that you develop OpenGHG in a Python
`virtual environment <https://docs.python.org/3/tutorial/venv.html>`__.
Here we'll create a new folder called ``envs`` in our home directory and create
a new ``openghg_devel`` environment in it.

.. code-block:: bash

    mkdir -p ~/envs/openghg_devel
    python -m venv ~/envs/openghg_devel

Virtual environments provide sandboxes which make it easier to develop
and test code. They also allow you to install Python modules without
interfering with other Python installations.

We activate our new environment using

.. code-block:: bash

    source ~/envs/openghg_devel/bin/activate


Now we can install OpenGHG's requirements and its developer requirements.

.. code-block:: bash

   pip install -r requirements.txt -r requirements-dev.txt

Finally install OpenGHG itself. The ``-e`` / ``--editable`` flag here tells ``pip`` to install the OpenGHG repo in develop mode.

.. code-block:: bash

   pip install -e .

OpenGHG should now be installed, you can check this by opening ``ipython`` and running

.. code-block:: ipython

   In [1]: import openghg

``conda``
^^^^^^^^^

Making sure you're in the ``openghg`` repository folder run

.. code-block:: bash

   conda env create -f environment.yaml

Once ``conda`` finishes its installation process you can activate the enironment


.. code-block:: bash

   conda activate openghg_env

Next install ``conda-build`` which allows us to install packages in develop mode

.. code-block:: bash

   conda install conda-build

And finally install OpenGHG

.. code-block::bash

   conda develop .

OpenGHG should now be installed, you can check this by opening ``ipython`` and running

.. code-block:: ipython

   In [1]: import openghg


Run tests
---------

To ensure everything is working on your system running the tests is a good idea. To do this run

.. code-block:: bash

    pytest -v tests


Coding Style
============

OpenGHG is written in Python 3 (>= 3.8). We aim as much as possible to follow a
`PEP8 <https://www.python.org/dev/peps/pep-0008/>`__ python coding style and
recommend that use a linter such as `flake8 <https://flake8.pycqa.org/en/latest/>`__.

This code has to run on a wide variety of architectures, operating
systems and machines - some of which don't have any graphic libraries,
so please be careful when adding a dependency.

With this in mind, we use the following coding conventions:

Naming
------

We follow a Python style naming convention.

* Packages: lowercase, singleword
* Classes: CamelCase
* Methods: snake_case
* Functions: snake_case
* Variables: snake_case
* Source Files: snake_case with a leading underscore

Functions or variables that are private should be named with a leading
underscore. This prevents them from being prominantly visible in Python's
help and tab completion.

Modules
-------

OpenGHG consists of the main module, e.g. ``openghg``, plus
a ``openghg.submodule`` module.

To make OpenGHG easy for new developers
to understand, we have a set of rules that will ensure that only
necessary public functions, classes and implementation details are
exposed to the Python help system.

* Module files containing implementation details are prefixed with
  an underscore, i.e. ``_parameters.py``

* Each module file contains an ``__all__`` variable that lists the
  specific items that should be imported.

* The package ``__init__.py`` can be used to safely expose the required
  functionality to the user with:

.. code-block:: python

   from module import function_a, function_b

This results in a clean API and documentation, with all extraneous information,
e.g. external modules, hidden from the user. This is important when working
interactively, since `IPython <https://ipython.org>`__
and `Jupyter <https://jupyter.org>`__
do not respect the ``__all__`` variable when auto-completing, meaning that the
user will see a full list of the available names when hitting tab. When
following the conventions above, the user will only be able to access the
exposed names. This greatly improves the clarity of the package, allowing
a new user to quickly determine the available functionality. Any user wishing
expose further implementation detail can, of course, type an underscore to
show the hidden names when searching.

Type hinting
------------

Throughout the OpenGHG project we use type hinting which allows us to declare the type of the objects
that are going to be passed to and returned from functions. This helps improve user understanding of the code
and when used in conjunction with tools like `mypy <https://mypy.readthedocs.io/en/stable/>`__ can help
catch bugs.

If we are writing a function that accepts takes a string and returns a string we can add the types like so

.. code-block:: python

    def greeter(name: str) -> str:
        """ Greets the user

            Args:
                name: Name of user
            Returns:
                str: Greeting string
        """
        return 'Hello ' + name

For a function that takes either a string or a list as its argument and returns a list we can write it as

.. code-block:: python

    def search(search_terms: Union[str, List]) -> List:
        """ A function that searches

            Args:
                search_terms: Search terms
            Returns:
                list: List of data found
        """
        return ["found_item"]


Workflow
========

Feature branches
----------------

First make sure that you are on the development branch of OpenGHG:

.. code-block:: bash

   git checkout devel

Now create and switch to a feature branch. This should be prefixed with
*feature*, e.g.

.. code-block:: bash

   git checkout -b feature-process

Pre-commit
----------

This project uses `pre-commit <https://pre-commit.com/>`__ to ensure code is linted and formatted using tools such as flake8,
black and others. This ensures errors are caught before the code is checked in the CI pipeline.

To install the hook

.. code-block:: bash

   pre-commit install

The hook should now run each time you make a commit.

Testing
=======

When working on your feature it is important to write tests to ensure that it
does what is expected and doesn't break any existing functionality. All code added to the
project must be covered by tests and tests should be placed inside the ``tests`` directory, creating an appropriately
named sub-directory for any new submodules.

The test suite is intended to be run using
`pytest <https://docs.pytest.org/en/latest/contents.html>`__.
When run, ``pytest`` searches for tests in all directories and files
below the current directory, collects the tests together, then runs
them. Pytest uses name matching to locate the tests. Valid names start
or end with *test*\ , e.g.:

::

   # Files:
   test_file.py       file_test.py

.. code-block:: python

   # Functions:
   def test_func():
      # code to perform tests...

   def func_test():
      # code to perform tests...

We use the convention of ``test_*`` when naming files and functions.

Running tests
-------------

To run the full test suite, simply type:

.. code-block:: bash

   pytest tests/


To get more detailed information about each test, run pytests using the
*verbose* flag, e.g.:

.. code-block:: bash

   pytest -v tests/

For more information on the capabilties of ``pytest`` please see the
`pytest documentation <https://docs.pytest.org/en/stable/contents.html>`__.

Continuous integration and delivery
-----------------------------------

We use GitHub Actions to run a full continuous integration (CI)
on all pull requests to devel and
master, and all pushes to devel and master. We will not merge a pull
request until all tests pass. We only accept pull requests to devel.

Documentation
=============

OpenGHG is fully documented using a combination of hand-written files
(in the ``doc`` folder) and auto-generated api documentation created from
Google `style docstrings <https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`__.
for details. The documentation is automatically built using `Sphinx <http://sphinx-doc.org>`__. Whenever a commit is pushed to devel the
documentation is automatically rebuilt and updated.

To build the documentation locally you will first need to install some
additional packages. If you haven't yet installed the documentation requirements please do so by running

.. code-block:: bash

   pip install -r requirements-doc.txt

Next ensure you have `pandoc <https://pandoc.org/>`__ installed. Installation instructions
can be `found here <https://pandoc.org/installing.html>`__

Then move to the ``doc`` directory and run:

.. code-block:: bash

   make

When finished, point your browser to ``build/html/index.html``.

Committing
==========

If you create new tests, please make sure that they pass locally before
commiting. When happy, commit your changes, e.g.

.. code-block:: bash

   git commit openghg/_new_feature.py tests/test_feature \
       -m "Implementation and test for new feature."

If your edits don't change the OpenGHG source code e.g. fixing typos in the documentation,
then please add ``[skip ci]`` to your commit message.

.. code-block:: bash

   git commit -a -m "Updating docs [skip ci]"

This will avoid unnecessarily running the
`GitHub Actions <https://github.com/openghg/openghg/actions>`__, e.g. running all the tests
and rebuilding the documentation of the OpenGHG package etc. GitHub actions are configured in the file
``.github/workflows/main.yaml``).

Next, push your changes to the remote server:

.. code-block:: bash

   # Push to the feature branch on the main OpenGHG repo, if you have access.
   git push origin feature

   # Push to the feature branch your own fork.
   git push fork feature

When the feature is complete, create a *pull request* on GitHub so that the
changes can be merged back into the development branch.
For information, see the documentation
`here <https://help.github.com/articles/about-pull-requests>`__.
