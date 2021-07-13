=============
Documentation
=============

This section of the documentation gives an overview of the public facing functions used in the Jupyter notebooks available at the
OpenGHG hub. For developers documentation of the internal workings of the library are available in the developer API section.

modules
=======

These modules are used to process observation data. Classes in this module should not be used directly as they
are used by functions in the ``Client`` or ``LocalClient`` modules when either uploading data to the OpenGHG cloud platform
or processing data for storage in a local object store.

:class:`~openghg.modules.ObsSurface`
    Process surface observation data

client
======

Classes within the client module are used to interact with the cloud based OpenGHG system.

:class:`~openghg.client.Process`
    Upload and process observation data files

:class:`~openghg.client.Search`
    Search for data in the object store

:class:`~openghg.client.RankSources`
    Rank data sources by date range

:class:`~openghg.client.JobRunner`
    Run jobs on a local or cloud HPC cluster

jobs
====

Classes within this module are used for running simulation jobs on high performace computing (HPC) clusters either locally
or within the cloud based on a cluster as a service (CaaS) offering (see CitC).

:class:`~openghg.jobs.SSHConnect`
    Connect via SSH to a HPC cluster

:class:`~openghg.jobs.JobDrive`
    Create a cloud storage drive for use by the HPC job

localclient
===========

For use with a local version of the object store. These functions make it easy to take advantage of the processing and export capabilities
of OpenGHG on your local filesystem. Use of this module results in the creation of a local object store in a location controlled by the
``OPENGHG_PATH`` environment variable.


:func:`~openghg.localclient.get_obs_surface`
    Search for observations in the object store using a site, species and date range.

:func:`~openghg.localclient.process_files`
    Process files for storage in the object store

:class:`~openghg.localclient.RankSources`
    Rank data sources by date range

:class:`~openghg.localclient.Search`
    Search for data within the object store (soon to be deprecated in favour of ``get_obs``


objectstore
===========

Many of the functions in this submodule are only for internal use and will be renamed. 

:func:`~openghg.objectstore.get_bucket`
    Get a bucket (data container) for storing of data within the object store


processing
==========

This submodule contains functions that are widely used in the processing functions found in ``modules``.

:func:`~openghg.processing.assign_attributes`
    Assign attributes to a dictionary of observation data in NetCDF format using ``get_attributes``

:func:`~openghg.processing.get_attributes`
    Write attributes to an in-memory NetCDF file to ensure it is `CF-compliant <https://cfconventions.org/>`_

:func:`~openghg.processing.get_ceda_file`
    Create a file that contains the correct attributes for uploading to the `CEDA archive <http://archive.ceda.ac.uk/>`_

:func:`~openghg.processing.recombine_datasets`
    Recombine separate NetCDF files into a single file sorted by date

:func:`~openghg.processing.search`
    The function that is used by ``openghg.client.Search`` and ``openghg.localclient.Search`` to search the object store

:func:`~openghg.processing.assign_data`
    Assings data to exisiting Datasources or creates new Datasources


.. .. toctree::
..    :maxdepth: 1

..    index_api_client
