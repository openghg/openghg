Fn usage
--------

Move this into the notebook!

We're now going to build on what we covered in the :doc:`fn_devel` section. We'll cover calling OpenGHG
functions using the classes available in the ``openghg.client`` module. Using these classes we can do things
like upload data, search for data on the OpenGHG Cloud and perform data analysis on that data.

As this is the developent documentation we will cover some lower level usage of these functions.

First we'll look at the `Search <https://github.com/openghg/openghg/blob/devel/openghg/client/_search.py>`__ class.
This is used to search for data in the object store, whether that is in the cloud or locally. In this tutorial we'll

Firstly we'll need to setup our environment. Please make sure you've run the ``build_deploy.py`` script and
