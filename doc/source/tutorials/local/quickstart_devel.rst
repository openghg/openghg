======================
Quickstart - developer
======================

This quickstart guide will get you quickly setup with a development environment so you can use and contribute to OpenGHG.
The main repository for OpenGHG can be found on `GitHub <https://github.com/openghg/openghg>`__.

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

Next we'll get a virtual environment setup using either.

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


The object store
================

The object store is where OpenGHG stores its information. Metadata and binary data are stored with in a key-value setup.
As you'll be using OpenGHG locally this means all data will be stored on your local or network file system.

When you first import OpenGHG it creates a file in your home directory at ``~/.config/openghg/openghg.conf``. Open this file with
your chosen text editor and you'll see the contents of a `TOML <https://toml.io/en/>`_ file like.


.. code-block:: toml

   [object_store]
   local_store = "/home/gareth/openghg_store"

By default the object store will be created in your home directory, if you want to move it just change that path.


What next?
==========

Now you've got OpenGHG setup please take a look at our :ref:`tutorials section<Tutorials>` and if you'd like to start
contributing to OpenGHG open an issue and submit a pull request!
