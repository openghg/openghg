=================
Python docstrings
=================

Docstrings are OpenGHG's source-level API reference. They describe the contract a
caller or maintainer needs; tutorials, workflows, design history, and extended
scientific explanation belong in the authored documentation and should be linked
when useful.

OpenGHG uses `Google-style docstrings
<https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`__.
Type annotations are the source of truth for Python types, so do not repeat those
types in ``Args`` or ``Returns`` entries.

What requires a docstring
=========================

``Public`` means exposed through a supported package facade or the generated API
reference. Check ``__all__``, lazy ``_EXPORTS`` mappings, and sibling ``.pyi`` files;
a leading underscore alone is not sufficient evidence that an object is private.

* Every public function, method, and class requires a Google-style docstring.
* A public package or module requires a meaningful summary when it appears in the
  generated reference. Mention important scope, relationships, or import-time effects
  only when readers need them.
* A complicated private object requires the contract, invariant, algorithmic reason,
  or failure condition needed for safe maintenance.
* Give a simple private helper or test at least one meaningful sentence when it is
  added or materially changed. Describe behaviour or the tested scenario; do not
  merely restate the name.

What the contract contains
==========================

Document only the caller-visible details that apply to the object:

* the meaning and constraints of parameters and results;
* accepted values, defaults whose effect is not evident, and lifecycle preconditions;
* mutation, side effects, caching, and eager or lazy computation;
* scientific units, dimensions, coordinates, alignment, metadata, missing-data
  behaviour, ordering, tolerances, assumptions, and limitations; and
* exceptions that are an intentional outcome callers should handle or an important
  recovery branch.

Do not enumerate incidental exceptions from internal operations. Put an implementation
reason or workaround in a source comment instead of a docstring.

Classes and inheritance
=======================

Use the class docstring as the single home for constructor parameters, important
state, lifecycle, invariants, and useful public attributes. Do not duplicate this
material in ``__init__``.

Do not repeat an inherited method's docstring when its contract remains accurate. An
override documents the differences it introduces, including changed state, results,
exceptions, or scientific semantics.

Maintenance and review
======================

Review the affected docstring in the same change whenever a public name, signature,
default, accepted value, result, mutation, side effect, exception, performance
characteristic, scientific contract, lifecycle, inheritance, deprecation, export, or
generated-reference location changes. If an implementation change has no docstring
impact, record that conclusion briefly in the pull request when it is not evident.

Ruff checks that public production classes, methods, and functions have docstrings.
Tests are exempt from these presence checks while the existing suite is migrated; the
review rule above still applies to new and materially changed tests. Ruff cannot decide
whether a docstring is true or scientifically complete, so source, tests, annotations,
exports, and rendered API pages remain part of review.

Run the focused checks before handoff:

.. code-block:: bash

   ruff check path/to/changed_module.py
   sphinx-build -W --keep-going -b html doc/source doc/build/html

Use the repository's configured environment and narrow the Sphinx command when the
full documentation baseline is not clean. Changes to scientific meaning, uncertainty,
or validation limits require an appropriate domain reviewer.
