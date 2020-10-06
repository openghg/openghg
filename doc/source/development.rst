=================
Developer's guide
=================

The source code for Pack and Doc is available on
`GitHub <https://github.com/openghg/openghg>`__.

Setting up your computer
=========================

Pack and Doc requires Python >= 3.7, so please install this before continuing
further. `Anaconda <https://anaconda.org>`__ provides easy installers for
Python that work on a range of operating systems and that don't need
root or admin permissions.

Virtual environments
--------------------

It is recommended that you develop Pack and Doc in a Python
`virtual environment <https://docs.python.org/3/tutorial/venv.html>`__.
You can create a new environment in the directory ``venvs/pack_and_doc-devel``
by typing;

.. code-block:: bash

   mkdir venvs
   python -m venv venvs/pack_and_doc-devel

Feel free to place the environment in any directory you want.

Virtual environments provide sandboxes which make it easier to develop
and test code. They also allow you to install Python modules without
interfering with other Python installations.

You activate you environment by typing;

.. code-block:: bash

    source venvs/pack_and_doc-devel/bin/activate

This will update your shell so that all python commands (such as
``python``, ``pip`` etc.) will use the virtual environment. You can
deactivate the environment and return to the "standard" Python using;

.. code-block:: bash

   deactivate

If you no longer want the environment then you can remove it using

.. code-block:: bash

  rm -rf venvs/pack_and_doc-devel

Coding Style
============

Pack and Doc is written in Python 3 (>= 3.7), with time-critical functions
written using `Cython <https://cython.org>`__ and parallelised using
`OpenMP <https://openmp.org>`__. The Cython code is written strictly
in comformant C, meaning that the package should compile and work on
any system on which Python >= 3.7 runs.
The program is tested with CI/CD on Windows 10, and
we thank the windows users who've helped us make the tutorial
cross-platform.

We aim as much as possible to follow a
`PEP8 <https://www.python.org/dev/peps/pep-0008/>`__ python coding style and
recommend that developers install and use
a linter such as `flake8 <https://flake8.pycqa.org/en/latest/>`__.

For the Cython pyx code we also try to maintain a PEP8 style where possible,
and recommend using a tool such as `autopep8 <https://pypi.org/project/autopep8/>`__
to keep this style (it is used by the lead developers, so contributions
will be formatted using it eventually ;-)).

We require that non-python code is strictly C. While C++ is an excellent
language, it makes it more
challenging to create portable binary distributions.

For ease of installation and support, we also minimise or bundle
external dependencies).
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

Relative imports should be used at all times, with imports ideally delayed
until they are needed.

For example, to import the Network object into a function that is in the
utils module, you would type

.. code-block:: python

   from .._network import Network

   network = Network()
   network.run(...)

or to import the Parameters from code that is the main Pack and Doc package,
you would type

.. code-block:: python

   from ._parameters import Parameters

   parameters = Parameters()
   parameters.add_seeds(filename="additional_seeds.dat")

Note that many classes are Python
`dataclasses <https://realpython.com/python-data-classes/>`__, which
are really useful for quick and safe development of code.
Python dataclasses should be preferred over writing your own
data-style classes.

Modules
-------

Pack and Doc consists of the main module, e.g. ``pack_and_doc``, plus
a ``pack_and_doc.submodule`` module.

In addition, there is a ``pack_and_doc.scripts`` module which contains the
code for the various command-line applications.

To make Pack and Doc easy for new developers
to understand, we have a set of rules that will ensure that only
necessary public functions, classes and implementation details are
exposed to the Python help system.

* Module files containing implementation details are prefixed with
  an underscore, i.e. ``_parameters.py``

* Where possible, external packages are hidden inside each module,
  e.g. ``import sys as _sys``

* Each module file contains an ``__all__`` variable that lists the
  specific items that should be imported.

* The package ``__init__.py`` can be used to safely expose the required
  functionality to the user with:

.. code-block:: python

   from module import *

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

Workflow
========

Feature branches
----------------

First make sure that you are on the development branch of Pack and Doc:

.. code-block:: bash

   git checkout devel

Now create and switch to a feature branch. This should be prefixed with
*feature*, e.g.

.. code-block:: bash

   git checkout -b feature-process

While working on your feature branch you won't want to continually re-install
in order to make the changes active. To avoid this, you can either make use
of ``PYTHONPATH``, e.g.

.. code-block:: bash

   make
   PYTHONPATH=./build/lib.{XXX} python script.py
   PYTHONPATH=./build/lib.{XXX} pytest tests

(where ``{XXX}`` is the build directory for Cython on your computer, e.g.
``./build/lib.macosx-10.9-x86_64-3.7`` - remember that you need to
type ``make`` to rebuild any Cython code and to copy your updated
files into that directory)

or use the ``develop`` argument when running the ``setup.py`` script, i.e.

.. code-block:: bash

   python setup.py develop

(this installs your current version of Pack and Doc into your current python
environment)

Testing
=======

When working on your feature it is important to write tests to ensure that it
does what is expected and doesn't break any existing functionality. Tests
should be placed inside the ``tests`` directory, creating an appropriately
named sub-directory for any new packages.

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
      return

   def func_test():
      # code to perform tests...
      return

We use the convention of ``test_*`` when naming files and functions.

Running tests
-------------

To run the full test suite, simply type:

.. code-block:: bash

   pytest tests

To run tests for a specific sub-module, e.g.:

.. code-block:: bash

   pytest tests/utils

To only run the unit tests in a particular file, e.g.:

.. code-block:: bash

   pytest tests/test_integration.py

To run a specific unit tests in a particular file, e.g.:

.. code-block:: bash

   pytest tests/test_read_variables.py::test_parameterset

To get more detailed information about each test, run pytests using the
*verbose* flag, e.g.:

.. code-block:: bash

   pytest -v

More details regarding how to invoke ``pytest`` can be
found `here <https://docs.pytest.org/en/latest/usage.html>`__.

Writing tests
^^^^^^^^^^^^^

Basics
""""""

Try to keep individual unit tests short and clear. Aim to test one thing, and
test it well. Where possible, try to minimise the use of ``assert`` statements
within a unit test. Since the test will return on the first failed assertion,
additional contextual information may be lost.

Floating point comparisons
""""""""""""""""""""""""""

Make use of the
`approx <https://docs.pytest.org/en/latest/builtin.html#comparing-floating-point-numbers>`__
function from the ``pytest`` package for performing floating
point comparisons, e.g:

.. code-block:: python

   from pytest import approx

   assert 0.1 + 0.2 == approx(0.3)

By default, the ``approx`` function compares the result using a
relative tolerance of 1e-6. This can be changed by passing a keyword
argument to the function, e.g:

.. code-block:: python

   assert 2 + 3 == approx(7, rel=2)

Skipping tests
""""""""""""""

If you are using
`test-driven development <https://en.wikipedia.org/wiki/Test-driven_development>`__
it might be desirable to write your tests before implementing the functionality,
i.e. you are asserting what the *output* of a function should be, not how it should
be *implemented*. In this case, you can make use of
the ``pytest`` *skip* decorator
to flag that a unit test should be skipped, e.g.:

.. code-block:: python

   @pytest.mark.skip(reason="Not yet implemented.")
   def test_new_feature():
       # A unit test for an, as yet, unimplemented feature.
       ...

Parametrizing tests
"""""""""""""""""""

Often it is desirable to run a test for a range of different input parameters.
This can be achieved using the ``parametrize`` decorator, e.g.:

.. code-block:: python

   import pytest
   from operator import mul

   @pytest.mark.parametrize("x", [1, 2])
   @pytest.mark.parametrize("y", [3, 4])
   def test_mul(x, y):
       """ Test the mul function. """
       assert mul(x, y) == mul(y, x)

Here the function test_mul is parametrized with two parameters, ``x`` and ``y``.
By marking the test in this manner it will be executed using all possible
parameter pairs ``(x, y)``\ , i.e. ``(1, 3), (1, 4), (2, 3), (2, 4)``.

Alternatively:

.. code-block:: python

   import pytest
   from operator import sub
   @pytest.mark.parametrize("x, y, expected",
                           [(1, 2, -1),
                            (7, 3,  4),
                            (21, 58, -37)])
   def test_sub(x, y, expected):
       """ Test the sub function. """
       assert sub(x, y) == -sub(y, x) == expected

Here we are passing a list containing different parameter sets, with the names
of the parameters matched against the arguments of the test function.

Testing exceptions
""""""""""""""""""

Pytest provides a way of testing your code for known exceptions. For example,
suppose we had a function that raises an ``IndexError``\ :

.. code-block:: python

   def indexError():
       """ A function that raises an IndexError. """
       a = []
       a[3]

We could then write a test to validate that the error is thrown as expected:

.. code-block:: python

   def test_indexError():
       with pytest.raises(IndexError):
           indexError()

Custom attributes
"""""""""""""""""

It's possible to mark test functions with any attribute you like. For example:

.. code-block:: python

   @pytest.mark.slow
   def test_slow_function():
       """ A unit test that takes a really long time. """
       ...

Here we have marked the test function with the attribute ``slow`` in order to
indicate that it takes a while to run. From the command line it is possible
to run or skip tests with a particular mark.

.. code-block:: bash

   pytest mypkg -m "slow"        # only run the slow tests
   pytest mypkg -m "not slow"    # skip the slow tests

The custom attribute can just be a label, as in this case, or could be your
own function decorator.

Continuous integration and delivery
-----------------------------------

We use GitHub Actions to run a full continuous integration (CI)
on all pull requests to devel and
main, and all pushes to devel and main. We will not merge a pull
request until all tests pass. We only accept pull requests to devel.
We only allow pull requests from devel to main. In addition to CI,
we also perform a build of the website on pushes to devel and tags
to main. The website is versioned, so that old the docs for old
versions of the code are always available. Finally, we have set up
continuous delivery (CD) on pushes to main and devel, which build the
pypi source and binary wheels for Windows, Linux (manylinux2010)
and OS X. These are manually uploaded to pypi when we tag
releases, but we expect to automate this process soon.

Documentation
=============

Pack and Doc is fully documented using a combination of hand-written files
(in the ``doc`` folder) and auto-generated api documentation created from
`NumPy <https://numpy.org>`__ style docstrings.
See `here <https://numpydoc.readthedocs.io/en/latest/format.html#docstring-standard>`__
for details. The documentation is automatically built using
`Sphinx <http://sphinx-doc.org>`__ whenever a commit is pushed to devel, which
will then update this website.

To build the documentation locally you will first need to install some
additional packages.

.. code-block:: bash

   pip install sphinx sphinx_issues sphinx_rtd_theme

Then move to the ``doc`` directory and run:

.. code-block:: bash

   make html

When finished, point your browser to ``build/html/index.html``.

Committing
==========

If you create new tests, please make sure that they pass locally before
commiting. When happy, commit your changes, e.g.

.. code-block:: bash

   git commit src/pack_and_doc/_new_feature.py tests/test_feature \
       -m "Implementation and test for new feature."

Remember that it is better to make small changes and commit frequently.

If your edits don't change the Pack and Doc source code, or documentation,
e.g. fixing typos, then please add ``ci skip`` to your commit message, e.g.

.. code-block:: bash

   git commit -a -m "Updating docs [ci skip]"

This will avoid unnecessarily running the
`GitHub Actions <https://github.com/openghg/openghg/actions>`__, e.g.
building a new Pack and Doc package, updating the website, etc.
(the GitHub actions are configured in the file
``.github/workflows/main.yaml``). To this end, we
have provided a git hook that will append ``[ci skip]`` if the commit only
modifies files in a blacklist that is specified in the file ``.ciignore``
(analagous to the ``.gitignore`` used to ignore untracked files). To enable
the hook, simply copy it into the ``.git/hooks`` directory:

.. code-block:: bash

    cp git_hooks/commit-msg .git/hooks

Any additional files or paths that shouldn't trigger a re-build can be added
to the ``.ciignore`` file.

Next, push your changes to the remote server, e.g.

.. code-block:: bash

   # Push to the feature branch on the main Pack and Doc repo, if you have access.
   git push origin feature

   # Push to the feature branch your own fork.
   git push fork feature

When the feature is complete, create a *pull request* on GitHub so that the
changes can be merged back into the development branch.
For information, see the documentation
`here <https://help.github.com/articles/about-pull-requests>`__.

Thanks
======

First, thanks to you for your interest in Pack and Doc and for reading this
far. We hope you enjoy having a play with the code and having a go
at adding new functionality, fixing bugs, writing docs etc.

We would also like to thank Lester Hedges and the
`BioSimSpace <https://biosimspace.org>`__ team who provided great advice
to set up the above, and from whose
`GitHub repo <https://github.com/michellab/biosimspace>`__
most of the procedures, scripts and documentation above is derived.
