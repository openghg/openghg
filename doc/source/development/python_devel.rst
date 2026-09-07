==================
Python development
==================

Follow :doc:`quickstart_devel` for the canonical development-environment setup.
This page defines the coding, validation, and contribution conventions used by
OpenGHG.

Coding Style
============

OpenGHG is written in Python 3 (>= 3.10). We aim as much as possible to follow a
`PEP8 <https://peps.python.org/pep-0008/>`__ Python coding style and
use `Ruff <https://docs.astral.sh/ruff/>`__ for linting and formatting.

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
* Source files: snake_case; private implementation modules normally begin with
  an underscore

Functions or variables that are private should be named with a leading
underscore. This prevents them from being prominently visible in Python's
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

* Import-heavy package ``__init__.py`` files should expose public names
  lazily. Keep the public names in ``__all__`` and add a matching
  ``_EXPORTS`` mapping from each public name to the private implementation
  module that defines it:

.. code-block:: python

   from importlib import import_module
   from typing import Any

   __all__ = ["function_a", "ClassB"]

   _EXPORTS = {
       "function_a": "._module_a",
       "ClassB": "._module_b",
   }

   def __getattr__(name: str) -> Any:
       try:
           module_name = _EXPORTS[name]
       except KeyError as exc:
           raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

       value = getattr(import_module(module_name, __name__), name)
       globals()[name] = value
       return value

   def __dir__() -> list[str]:
       return sorted(__all__)

The invariant is ``set(__all__) == set(_EXPORTS)``. Tests should enforce
this for each package using the lazy export pattern.

Packages using this pattern must also include a sibling ``__init__.pyi``
stub that re-exports the same public names from their implementation
modules. The runtime ``__getattr__`` necessarily returns ``Any``, and the
stub keeps ``mypy`` and other static checkers from losing the real function
and class types while preserving lazy runtime imports.

* The top-level ``openghg`` package uses the same idea for subpackages:
  subpackages are listed in ``__all__`` and imported only when first
  accessed.

* Do not rely on importing a package ``__init__.py`` to trigger subclass
  registration, xarray accessor registration, or other implementation-module
  side effects. Discovery that must work before implementation modules are
  imported should use declarative metadata instead. For example, store data
  type discovery is maintained in ``openghg.store.spec`` rather than by
  eagerly importing every store class.

* If a previous import-time side effect is still useful, provide an explicit
  opt-in helper rather than restoring the eager import. For example,
  ``openghg.enable_pint_xarray()`` imports ``pint_xarray`` and registers the
  xarray ``.pint`` accessor without making ``import openghg`` import xarray.

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

Use type annotations for new and modified production functions and methods.
Annotations make interfaces clearer and allow tools such as
`mypy <https://mypy.readthedocs.io/en/stable/>`__ to catch mistakes. Prefer
modern built-in types and union syntax:

.. code-block:: python

   from collections.abc import Sequence

   def normalise_names(names: str | Sequence[str]) -> list[str]:
       """Return names stripped of surrounding whitespace."""
       if isinstance(names, str):
           names = [names]
       return [name.strip() for name in names]

Docstrings and programming style
--------------------------------

Use `Google-style docstrings
<https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings>`__
for every public function, method, and class and every complicated private
function, method, or class. Give simple private helpers and tests at least one meaningful
sentence. Describe the behaviour or test scenario, rather than repeating the
name; for example:

.. code-block:: python

   def test_align_footprints_keeps_periods():
       """Keep inferred sampling periods when footprints are aligned."""
       ...

A docstring's summary is a sentence ending in punctuation. Describe the
caller-visible contract rather than implementation details. Include scientific
semantics that annotations cannot express, such as units, dimensions,
coordinates, accepted values, mutation, lazy or eager behaviour, and relevant
exceptions. Do not repeat annotated types in ``Args`` or ``Returns`` sections.

The following selected conventions are particularly important in OpenGHG:

* Use ``ValueError``, ``TypeError``, or a project exception for invalid runtime
  inputs. Do not use ``assert`` for runtime validation.
* Catch the narrowest expected exception and keep ``try`` blocks small. Catch
  ``Exception`` only at a documented isolation boundary or when immediately
  re-raising.
* Avoid mutable or call-expression defaults. Use ``None`` and create the value
  inside the function when necessary.
* Comments explain a non-obvious reason or scientific intent, not Python syntax.
  New TODOs should identify an issue or a concrete removal condition.
* Use a specific error code for ``# type: ignore`` and ``# noqa``. Explain a
  non-obvious suppression.
* Resolve package and test-data paths relative to the package or test helpers;
  do not hard-code paths from a developer's machine.

Ruff's formatting and lint settings in ``pyproject.toml`` are authoritative.
In particular, OpenGHG uses a 110-character line length rather than the Google
Python Style Guide's 80-character recommendation.


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

This project uses `pre-commit <https://pre-commit.com/>`__ to run the configured
repository checks, including Ruff, mypy, and secret scanning. CI separately
checks Graphify freshness. Pre-commit is an aggregate check rather than an alias
for linting or tests; some hooks may modify files.

To install the hook

.. code-block:: bash

   pre-commit install

The hook should now run each time you make a commit.

You can run file-oriented hooks on selected working-tree files without
committing or installing the Git hook:

.. code-block:: bash

   pre-commit run --files openghg/example.py tests/example_test.py

``pre-commit run --all-files`` is useful for an intentional repository-wide
audit, but it can expose unrelated baseline issues and rewrite fixture files.
It is not the default check for a focused change. Review the diff after any
pre-commit run. For a faster or more specific cycle, run individual tools
directly:

.. code-block:: bash

   ruff format --check openghg/example.py
   ruff check openghg/example.py
   mypy openghg
   pytest tests/example_test.py::test_case

Prefix these commands with ``uv run --no-sync`` or
``pixi run --locked -e dev`` when using those environment managers.

To simulate the commit hook without creating a commit, stage the intended files
and run ``pre-commit run``. This checks the index, including Gitleaks. Review and
re-stage any files modified by hooks before committing. The Gitleaks hook
examines the staged snapshot, not unstaged files supplied via ``--files``.

Testing
=======

Add or update tests for changed behaviour. A regression test should fail without
the fix when practicable. Place tests inside the ``tests`` directory, mirroring
the source area when practical.

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

For more information on the capabilities of ``pytest`` please see the
`pytest documentation <https://docs.pytest.org/en/stable/contents.html>`__.

Some test groups require explicit options or additional dependencies:

* Tests marked ``cfchecks`` require ``--run-cfchecks``.
* Tests marked ``icos`` require ``--run-icos`` and may access the network.
* Tests marked ``xesmf`` require the optional regridding dependencies.

Testing multiple Python versions
--------------------------------

Use tox when intentionally testing an isolated installation against Python
3.10, 3.11, or 3.12. It is not the default fast development loop. List the
available environments with ``tox list``. For example, run the Python 3.12
suite, or pass a focused path to pytest, with:

.. code-block:: bash

   tox run -e py312
   tox run -e py312 -- tests/analyse/test_scenario.py

Continuous integration and delivery
-----------------------------------

We use GitHub Actions to run a full continuous integration (CI)
on all pull requests to devel and
master, and all pushes to devel and master. We will not merge a pull
request until all tests pass. We only accept pull requests to devel.

Documentation
=============

OpenGHG combines hand-written files in ``doc/source`` with API documentation
generated from Google-style docstrings. `Sphinx <https://www.sphinx-doc.org/>`__
builds the documentation in continuous integration.

To build the documentation locally you will first need to install the
documentation dependencies. If you haven't yet installed the documentation dependencies please do so by running

.. code-block:: bash

   uv sync --extra dev --extra doc --locked

Next ensure you have `pandoc <https://pandoc.org/>`__ installed. Installation instructions
can be `found here <https://pandoc.org/installing.html>`__

Then move to the ``doc`` directory and run:

.. code-block:: bash

   uv run --no-sync make html

When finished, point your browser to ``build/html/index.html``.

Committing
==========

If you create new tests, please make sure that they pass locally before
committing. When happy, commit your changes, e.g.

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
``.github/workflows/workflow.yaml``).

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
