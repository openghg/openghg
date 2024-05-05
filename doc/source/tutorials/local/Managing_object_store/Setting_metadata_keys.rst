Setting identifying metadata keys
=================================

When standardising data OpenGHG uses a portion of the metadata input to decide how this data is stored.
For each unique piece of information a ``Datasource`` is created and data assigned to it.
For example, for a CO2 footprint for Tacolneston at a height of 100m using the UKV
met model you may standardise the file by doing


.. code-block:: python

    standardise_footprint(filepath=fp_filepath,
                            site="TAC",
                            domain="EUROPE",
                            model="NAME",
                            height="100m",
                            met_model"UKV")


Behind the scenes OpenGHG performs a lookup using the metadata you've passed in. We use this metadata to know where
to store the data. For each data type we use a set of keys that can be found in the ``metadata_keys.toml`` file in the ``config``
folder of the object store. Each time a file is standardised this file is read and the keys stored within are used to decide
which ``Datasource`` the data should be assigned to.

Say we have an object store in our home directory at ``/home/gareth/openghg_store``, you can find the config file at
``/home/gareth/openghg_store/config/metadata_keys.toml``. Let's have a look at the default file OpenGHG creates
when the store is first setup.

TODO - include the default file here!

NOTE this is only showing the keys for the ``footprints`` data type.

.. code-block:: toml

    [footprints.required.site]
    type = [ "str",]

    [footprints.required.model]
    type = [ "str",]

    [footprints.required.inlet]
    type = [ "height",]

    [footprints.required.domain]
    type = [ "str",]

    [footprints.required.time_resolved]
    type = [ "bool",]

    [footprints.required.high_spatial_resolution]
    type = [ "bool",]

    [footprints.required.short_lifetime]
    type = [ "bool",]

    [footprints.required.species]
    type = [ "str", "species",]

    [footprints.required.met_model]
    type = [ "str",]

Notice that for each data type we have a ``required`` and an ``optional`` key. If you're unfamiliar with TOML the Python
dictionary equivalent for each data type is 

    .. code-block:: python

        metakeys = {
            "footprints": {
                            "required": {"site": {"type": ["str"]},
                                        "model": {"type": ["str"]},
                                        "inlet": {"type": ["height"]},
                                        "domain": {"type": ["str"]},
                                        ...
                                        },
                            "optional": {},
                          }
        }

For each key we require a type. These types are not the standard Python types but metadata types that OpenGHG
will read to process the data you give it. For example if you want to pass in a date value for some data that should be
parsed as a timestamp you would give the type as ``timestamp``. For an inlet height the type would be ``height``.

The list of types available:

    1. height
    2. species
    3. timestamp
    4. etc etc
    5. this is not a fixed list, change it however you want
    6. what types do we want?

Say we want to add some footprint data and we want to be able to store based on the project it's associated with.
We'll update the TOML file and add a couple of new lines

.. code-block:: toml

    [footprints.required.site]
    type = [ "str",]

    [footprints.required.model]
    type = [ "str",]

    [footprints.required.inlet]
    type = [ "height",]

    [footprints.required.domain]
    type = [ "str",]

    [footprints.required.time_resolved]
    type = [ "bool",]

    [footprints.required.high_spatial_resolution]
    type = [ "bool",]

    [footprints.required.short_lifetime]
    type = [ "bool",]

    [footprints.required.species]
    type = [ "str", "species",]

    [footprints.required.met_model]
    type = [ "str",]

    [footprints.required.project]
    type = ["str",]

This key must now be provided when standardising data. At the moment this key must be passed in using the ``optional_metadata``
argument to the standardisation functions. To standardise a footprint we now do

.. code-block:: python

    .. code-block:: python

    
    result = standardise_footprint(filepath=fp_filepath,
                                    site="TAC",
                                    domain="EUROPE",
                                    model="NAME",
                                    height="100m",
                                    met_model"UKV",
                                    optional_metadata={"project": "project_a"})

    result = standardise_footprint(filepath=fp_filepath_b,
                                    site="TAC",
                                    domain="EUROPE",
                                    model="NAME",
                                    height="100m",
                                    met_model"UKV",
                                    optional_metadata={"project": "project_b"})

These files will now be stored separately within OpenGHG.