==================
Packaging releases
==================

OpenGHG is now fully tested and deployed using GitHub actions.
The development process should be;

* New features are developed on feature branches, called ``feature-{feature}``,
  either in the `devel OpenGHG repository <https://github.com/openghg/openghg>`__
  for authorised developers.
* Bug fixes or issue fixes are developed on fix branches, called
  ``fix-issue-{number}``.
* Pull requests are issued from these branches to ``devel``. All merge conflicts
  must be fixed in the branch and all tests must pass before the pull
  request can be merged into ``devel``.

The result of this is that "devel" should contain the fully-working and
tested, and most up-to-date version of ``OpenGHG``. However, this
version should not be used for production runs.

Defining a release
------------------

Releases aim to be backwards compatible and capable of being used for production runs, at least for
the functionality that is fully described in the tutorial.

We use `semantic versioning <https://semver.org>`__ and take care
not to cause breaking changes in the public API.

Creating a release
------------------

To create a release first checkout the "devel" branch.

.. code-block:: bash

   git checkout devel
   git pull

Make sure to have a fully working devel branch.

Now create a PR to merge "devel" into "master".
Make sure all of the "changelog" is updated.

Upon PR approval the changes from "devel" can be merged into "master".

Tagging a new release
---------------------

Now that you are happy that the release is ready, you can tag the new
version. Do this using the ``git tag`` command, e.g.

.. code-block:: bash

   git checkout master
   git tag -a {VERSION} -m "{VERSION} release"

replacing ``{VERSION}`` with the version number. For this 0.12.0 release
the command would be;

.. code-block:: bash

   git tag -a 0.7.1 -m "OpenGHG release v0.7.1"

Next, push your tag to GitHub;

.. code-block:: bash

   git push --tags

The tag will be used by automatic versioning script to generate
the version numbers of the code. Building the package
(as happens below) will automatically update the _version.py
that is included in the package to tag versions.

This will also trigger a full CI/CD to test and build the new version.
Again, it should work as this tag was taken from your fully-tested
"master" branch.