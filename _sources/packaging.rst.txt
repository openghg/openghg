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

   git checkout main
   git pull

Next, merge in all changes from the "devel" branch.

.. code-block:: bash

   git pull origin devel

Next, update the :doc:`changelog` with details about this release. This
should include the link at the top of the release that shows the commit
differences between versions. This can be easily copied from a previous
release and updated, e.g.

::

  `0.11.2 <https://github.com/metawards/MetaWards/compare/0.11.1...0.11.2>`__ - May 11th 2020


could be changed to

::

  `0.12.0 <https://github.com/metawards/MetaWards/compare/0.11.2...0.12.0>`__ - May 18th 2020

when moving from the 0.11.2 to 0.12.0 release.

Now push this change back to GitHub, using;

.. code-block:: bash

   git push

This will trigger a CI/CD run which will build and test everything on
Windows, Mac and Linux for Python 3.7 and 3.8. Everything should work,
as "devel" should have been in a release-ready state.

Testing the packages
--------------------

`GitHub actions <https://github.com/metawards/MetaWards/actions>`__ will
produce the source and binary wheels for ``metawards`` on all supported
platforms. This will be in an artifact called ``dist`` which you should
download and unpack.

.. image:: images/github_artifacts.jpg
   :alt: Image of the GitHub Actions interface showing the dist artifact

You should unpack these into the ``dist`` directory, e.g.

.. code-block:: bash

   cd dist
   unzip ~/Downloads/dist.zip

This should result in six binary wheels and once source package, e.g.

::

    metawards-0.11.1+7.g52b3671-cp37-cp37m-macosx_10_14_x86_64.whl
    metawards-0.11.1+7.g52b3671-cp37-cp37m-manylinux1_x86_64.whl
    metawards-0.11.1+7.g52b3671-cp37-cp37m-win_amd64.whl
    metawards-0.11.1+7.g52b3671-cp38-cp38-macosx_10_14_x86_64.whl
    metawards-0.11.1+7.g52b3671-cp38-cp38-manylinux1_x86_64.whl
    metawards-0.11.1+7.g52b3671-cp38-cp38-win_amd64.whl
    metawards-0.11.1+7.g52b3671.tar.gz

Try to install the package related to you machine, just to double-check
that it is working, e.g.

.. code-block:: bash

   pip install ./metawards-0.11.1+7.g52b3671-cp37-cp37m-macosx_10_14_x86_64.whl
   cd ..
   pytest tests

Once it is working, remove these temporary packages from your ``dist`` folder,

.. code-block:: bash

   rm dist/*

Tagging a new release
---------------------

Now that you are happy that the release is ready, you can tag the new
version. Do this using the ``git tag`` command, e.g.

.. code-block:: bash

   git tag -a {VERSION} -m "{VERSION} release"

replacing ``{VERSION}`` with the version number. For this 0.12.0 release
the command would be;

.. code-block:: bash

   git tag -a 0.12.0 -m "0.12.0 release"

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

Uploading packages to pypi
--------------------------

While you are waiting for the CI/CD GitHub Actions to complete, make sure
that your version of twine is fully up to date;

.. code-block:: bash

   pip install --upgrade twine

Once GitHub actions is complete, you will see that another build artifact
is ready for download. Download this and unpack it into your ``dist``
directory as before. You should now have a ``dist`` directory that
contains six binary wheels and one source package, named according to
the release version. For example, for the 0.11.2 release we had;

.. code-block:: bash

   $ ls dist
    metawards-0.11.2-cp37-cp37m-macosx_10_14_x86_64.whl
    metawards-0.11.2-cp37-cp37m-manylinux1_x86_64.whl
    metawards-0.11.2-cp37-cp37m-win_amd64.whl
    metawards-0.11.2-cp38-cp38-macosx_10_14_x86_64.whl
    metawards-0.11.2-cp38-cp38-manylinux1_x86_64.whl
    metawards-0.11.2-cp38-cp38-win_amd64.whl
    metawards-0.11.2.tar.gz

Now you can upload to pypi using the command;

.. code-block:: bash

   python3 -m twine upload dist/*

.. note::

    You will need a username and password for pypi and to have
    permission to upload code to this project. Currently only
    the release manager has permission. If you would like
    join the release management team then please get in touch.

Testing the final release
-------------------------

Finally(!) test the release on a range of different machines by logging
in and typing;

.. code-block:: bash

   pip install metawards=={VERSION}

replacing ``{VERSION}`` with the version number, e.g. for 0.11.2

.. code-block:: bash

   pip install metawards==0.11.2

Play with the code, run the tests and run some examples. Everything should
work as you have performed lots of prior testing to get to this stage.
