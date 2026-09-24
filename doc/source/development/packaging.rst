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

Ensure ``devel`` is working and all issues in the release milestone are complete. In GitHub Actions, run **Prepare release** with the next version number (for example, ``0.20.0``). This creates a ``release/v0.20.0`` branch from ``devel``, runs Towncrier, and opens a preparation PR back into ``devel``. The workflow dispatches the OpenGHG, conda, and documentation checks because PRs created by ``GITHUB_TOKEN`` do not trigger those checks automatically.

Review the generated release entry and merge the preparation PR after its checks pass. The current ``Unreleased`` section contains notes written before Towncrier was introduced. For the first release using this workflow, move those notes into the new release section and leave ``Unreleased`` empty. Update its comparison link to the new version. Add future changes as news fragments rather than editing ``CHANGELOG.md`` in feature PRs.

To preview or assemble release notes manually from a checkout of ``devel``, run:

.. code-block:: bash

   towncrier build --version=0.20.0 --draft
   towncrier build --version=0.20.0 --yes

The build adds a new release entry to ``CHANGELOG.md`` and removes the consumed fragments. If preparing manually, commit both changes before opening the PR. The release workflow checks for a matching Towncrier entry, no pending fragments, and an empty ``Unreleased`` section before publishing from a version tag.

After the preparation PR merges, open a PR from ``devel`` into ``master`` named "Release ``Version``". Merge it after approval.

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
