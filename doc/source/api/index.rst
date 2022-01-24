========
User API
========

This section of the documentation gives an overview of the public facing functions used in the Jupyter notebooks available at the
OpenGHG hub. For developers documentation of the internal workings of the library are available in the developer API section.

client
======

Classes within the client module are used to interact with OpenGHG, whether it be a cloud or local instance.

:func:`~openghg.client.process_files`
    Upload data files for standardisation and storage in the object store

:func:`~openghg.client.search`
    Search and retrieve data from the object store

:func:`~openghg.client.rank_sources`
    Rank data sources by date range

:func:`~openghg.client.get_obs_surface`
    Retrieve data from the object store in a format expected by the ACRG

