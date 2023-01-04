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

We'll first install and update some of the installation tools

.. code-block:: bash

   pip install --upgrade pip wheel setuptools

Now, making sure we're in the root of the OpenGHG repository we just cloned, install OpenGHG's requirements and its developer requirements.

.. code-block:: bash

   pip install -r requirements.txt -r requirements-dev.txt

Finally install OpenGHG itself. The ``-e`` / ``--editable`` flag here tells ``pip`` to install the OpenGHG repo in develop mode.

.. code-block:: bash

   pip install -e .

Now OpenGHG is installed please move on to :ref:`Configuring the object store<Configuring the object store>`.

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

.. code-block:: bash

   conda develop .

Now OpenGHG is installed we'll move onto configuring the object store.

Configuring the object store
============================

The object store is where OpenGHG stores its information. Metadata and binary data are stored with in a key-value setup.
As you'll be using OpenGHG locally this means all data will be stored on your local or network file system.

To set the path to the object store you can run the following in the terminal

.. code-block:: bash

    openghg-quickstart

You should now see text telling you you haven't got a configuration file (unless you've run the quickstart previously)
and a prompt asking you enter a path for the object store. We'll just use the default path and hit return when prompted.

.. code-block:: bash

    No configuration file found, please see installation instructions.

    Please enter a path for the object store (default: /home/gareth/openghg_store):
    Creating config at /home/gareth/.config/openghg/openghg.conf


A configuration file has been created and you're ready to run OpenGHG. If you ever want to modify the configuration file
you can find it at ``~/.config/openghg/openghg.conf``. My configuration file looks like this

.. code-block:: toml

    user_id = "47363762-2963-4a2d-8afc-dejh05380f19"

    [object_store]
    local_store = "/home/gareth/openghg_store"


Jupyter
=======

If you want to use Jupyter notebooks to interact with OpenGHG, you can install ``jupyterlab``.

.. code-block:: bash

   pip install jupyterlab

Then just run ``jupyter notebook`` to get started.

What next?
==========

Now you've got OpenGHG setup please take a look at our :ref:`tutorials section<Tutorials>` and if you'd like to start
contributing to OpenGHG `open an issue <https://github.com/openghg/openghg/issues>`_ and submit a pull request!
