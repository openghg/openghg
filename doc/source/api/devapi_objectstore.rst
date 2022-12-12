===========
Objectstore
===========

These functions are used by OpenGHG to store and retrieve data from the object store. OpenGHG will call either local object
store functions (that create an object store on disk) or cloud object store functions depending on its environment. Within the
`__init__.py` of `openghg.objectstore` a check is made to see if the environment is the OpenGHG hub or a serverless function.

Local
=====

These functions handle data storage on disk.

.. autofunction:: openghg.objectstore.delete_object

.. autofunction:: openghg.objectstore.exists

.. autofunction:: openghg.objectstore.clear_tutorial_store

.. autofunction:: openghg.objectstore.get_all_object_names

.. autofunction:: openghg.objectstore.get_bucket

.. autofunction:: openghg.objectstore.get_object

.. autofunction:: openghg.objectstore.get_object_from_json

.. autofunction:: openghg.objectstore.get_object_names

.. autofunction:: openghg.objectstore.get_local_objectstore_path

.. autofunction:: openghg.objectstore.get_tutorial_store_path

.. autofunction:: openghg.objectstore.query_store

.. autofunction:: openghg.objectstore.set_object

.. autofunction:: openghg.objectstore.set_object_from_file

.. autofunction:: openghg.objectstore.set_object_from_json

.. autofunction:: openghg.objectstore.visualise_store


.. Cloud
.. =====

.. .. autofunction:: openghg.objectstore.create_bucket

.. .. autofunction:: openghg.objectstore.create_par

.. .. autofunction:: openghg.objectstore.delete_object

.. .. autofunction:: openghg.objectstore.delete_par

.. .. autofunction:: openghg.objectstore.exists

.. .. autofunction:: openghg.objectstore.get_all_object_names

.. .. autofunction:: openghg.objectstore.get_bucket

.. .. autofunction:: openghg.objectstore.get_object

.. .. autofunction:: openghg.objectstore.get_object_from_json

.. .. autofunction:: openghg.objectstore.set_object

.. .. autofunction:: openghg.objectstore.set_object_from_file

.. .. autofunction:: openghg.objectstore.set_object_from_json

.. .. autofunction:: openghg.objectstore.upload
