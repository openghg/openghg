==================
Packaging releases
==================

Pack and Doc is now fully tested and deployed using GitHub actions.
The development process should be;

* New features are developed on feature branches, called ``feature-{feature}``,
  either in the `main Pack and Doc repository <https://github.com/openghg/openghg>`__
  for authorised developers, or in personal forks for
  new developers.
* Bug fixes or issue fixes are developed on fix branches, called
  ``fix-issue-{number}`` (again in either the main repository or forks).
* Pull requests are issued from these branches to ``devel``. All merge conflicts
  must be fixed in the branch and all tests must pass before the pull
  request can be merged into ``devel``.

The result of this is that "devel" should contain the fully-working and
tested, and most up-to-date version of ``pack and doc``. However, this
version should not be used for production runs.

Defining a release
------------------

We will release ``pack and doc`` regularly. Releases aim to be backwards
compatible and capable of being used for production runs, at least for
the functionality that is fully described in the tutorial.

We use `semantic versioning <https://semver.org>`__ and take care
not to cause breaking changes in the public API.

Creating a release
------------------

To create a release first checkout the "main" branch.

.. code-block:: bash

   git checkout master
   git pull

Next, merge in all changes from the "devel" branch.

.. code-block:: bash

   git pull origin devel

Next, update the :doc:`changelog` with details about this release. This
should include the link at the top of the release that shows the commit
differences between versions. This can be easily copied from a previous
release and updated, e.g.

::

  `0.7.0 <https://github.com/openghg/openghg/compare/0.6.2...0.7.0>`__ - May 11th 2020


could be changed to

::

  `0.7.1 <https://github.com/openghg/openghg/compare/0.7.0...0.7.1>`__ - May 18th 2020

when moving from the 0.7.0 to 0.7.1 release.

Now push this change back to GitHub, using;

.. code-block:: bash

   git push

This will trigger a CI/CD run which will build and test everything on Linux for Python 3.9 - 3.12.
Everything should work, as "devel" should have been in a release-ready state.

Tagging a new release
---------------------

Now that you are happy that the release is ready, you can tag the new
version. Do this using the ``git tag`` command, e.g.

.. code-block:: bash

   git tag -a {VERSION} -m "{VERSION} release"

replacing ``{VERSION}`` with the version number. For this 0.12.0 release
the command would be;

.. code-block:: bash

   git tag -a 0.7.1 -m "0.7.1 release"

Next, push your tag to GitHub;

.. code-block:: bash

   git push --tags

The tag will be used by automatic versioning script to generate
the version numbers of the code. Building the package
(as happens below) will automatically update the _version.py
that is included in the package to tag versions.

This will also trigger a full CI/CD to test and build the new version.
Again, it should work as this tag was taken from your fully-tested
"main" branch.