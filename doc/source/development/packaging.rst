==================
Packaging releases
==================

OpenGHG is now fully tested and deployed using GitHub actions.
The most updated developer code lies in the `OpenGHG repository <https://github.com/openghg/openghg>`_

The development process should be:

#. New changes to any section of the code base should always be made using feature branches.
    .. code-block:: bash

       git checkout -b Iss{issue_number}-{branch_name}
#. Bug fixes are developed on fix branches.
    .. code-block:: bash

      git checkout -b fix{issue_number}-{branch_name}
#. Pull requests are issued from these branches to ``devel``. All merge conflicts must be fixed in the branch and all tests must pass before the pull request can be merged into ``devel``.

The results to have "devel" with fully-working and
tested code, and most up-to-date version of ``OpenGHG``. However, this
version should not be used for production runs.

Defining a release
------------------

Releases aim to be backwards compatible and capable of being used for production runs, at least for
the functionality that is fully described in the tutorial.

We use `semantic versioning <https://semver.org>`__ and take care
not to cause breaking changes in the public API.

Creating a release
------------------

The creation of the packages required by PyPI and conda is handled in the GitHub actions workflow files under ``.github/workflows/workflow.yaml``.
The package build and release workflow will only be triggered by a tagged commit.
Before we get to this step we need to make sure everything is set up to do a release. Ensure all issues in the release milestone are completed.

.. code-block:: bash

    git checkout devel
    git pull

Make sure to have a fully working devel branch.

Now update the "Unreleased" section of the ``Changelog.md`` with the "{release number} - {date of release}".
Ensure all the changes are included in the file. If not update it accordingly.

Also add "Unreleased" section at the top of ``Changelog.md`` along with the link to the latest diff as shown below:

.. code-block:: bash

   [Unreleased](https://github.com/openghg/openghg/compare/0.8.0...HEAD)
   [0.8.0] - 2024-03-19

Now create a PR to merge ``devel`` into ``master`` with name of PR as "Release ``Version``".

Upon PR approval the changes from "devel" can be merged into "master".

Tagging a new release
---------------------

Now that you are happy that the release is ready, you can tag the new
version. This can be done using the following commands:

.. code-block:: bash

   git checkout master
   git pull
   git tag -a {VERSION} -m "OpenGHG release v{VERSION}" && git push origin {VERSION}
replacing ``{VERSION}`` with the version number. E.g.

.. code-block:: bash

   git tag -a 0.8.0 -m "OpenGHG release v0.8.0" && git push origin 0.8.0
The tag will be used by an automatic versioning script to generate
the version numbers of the code. Building the package
(as happens below) will automatically update the _version.py
that is included in the package to tag versions.

This will also trigger a full CI/CD to test and build the new version.

Completing the release
----------------------

Once you are satisfied that all workflows have run successfully check the latest version of the ``OpenGHG`` is live on both `PyPi <https://pypi.org/project/openghg/>`_ as well as `Anaconda <https://anaconda.org/openghg/openghg>`_.

Now, you have successfully released a new version of ``OpenGHG``.
