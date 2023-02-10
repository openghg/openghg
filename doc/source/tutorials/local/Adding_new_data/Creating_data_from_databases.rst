Using databases to add data
===========================

Within OpenGHG so far, we have been using the provided standardisation
functions for known input file formats (source formats) to populate the
object store ([see here and here] - add links to previous tutorial
pages). OpenGHG also provides functionality to create data from
underlying databases and add this to the object store as well. At
present, this has been implemented for the `EDGAR global anthropogenic
database <https://edgar.jrc.ec.europa.eu/>`__ to allow emissions files
to be created on a regional domain.

Tutorial store
--------------

…

Creating emissions from the EDGAR database
------------------------------------------

To create emissions maps from an underlying database, we can use the
``transform_data(...)`` method for the ``Emissions`` object (from
``openghg.store``). The general form for this is as follows (same as
``Emissions.read_file()``):

.. code:: python

   from openghg.store import Emissions

   Emissions.transform_data(datapath, database, ...)  # Additional keywords

This function expects the path, the name of the database as well as
keyword inputs which will be determined by the underlying function being
accessed. To use this for the global EDGAR database this must be locally
available and a link provided to the folder path (``datapath`` input)
(**Add option to download this data e.g. via wget if possible** GJ -
could we use the ``retrieve_data`` function?). The ``database`` input
must also be specified as ``"edgar"``.

**Should we add ``transform_...`` functions as a shorthand (c.f.
standardise\_… functions)?**

We will need to provide inputs for species and for our domain (area) of
interest. A flux map of the correct format will then be created and
added to the openghg object store with the associated keywords.

In the background, this will call the
``openghg.transform.emissions.parse_edgar`` function in the background
and we can look at this function for details of what keywords we need to
provide:

-  date - the year to process from the database (as a string)
-  species - the species name
-  domain / lat_out / lon_out

   -  To describe the regional domain a keyword can be passed for
      pre-determined domains OR a new set of lat-lon arrays.

-  edgar_version - specify this if this cannot be extracted from the
   database provided

**How do we find out which domains are available and what they mean?
Need a checker function?**

*Note: for creating new domains this currently relies on the xesmf
package which is better installed in a conda environment than using pip.
Please see details in install instructions around this.*

**++Add paired down edgar database here which can be downloaded and
used.++**

For example, this would create a new emissions map for methane, “ch4”,
for the area over Europe, “EUROPE” domain for 2014. The edgar database
provided is v6.0, annual, global and for methane.

.. code:: python

   from openghg.store import Emissions

   datapath = "..."
   database = "edgar"

   Emissions.transform_data(datapath, database, date=2014, domain="EUROPE", species="ch4")

We can then check the data has been added to the object store.

.. code:: python

   from openghg.retrieve import search_emissions

   results = search_emissions(database="edgar")

   results.results

Adding new options
------------------

Transformation workflow
^^^^^^^^^^^^^^^^^^^^^^^

Within OpenGHG, there are multiple ways to add data to the object store.
The most direct way is to use a standardisation functions already
introduced ([see here] - add link to previous tutorial pages) which can
be used to convert from an understood format (``source_format``) into
the standardised openghg format and add this to the database. The other
way is to use the tranformation workflow which extracts a subset /
performs an operation to update the provided data and adds this to the
object store.

In this way, the standardisation functions can be considered as a
one-to-one mapping to be run only when the data is first added and when
the data has been updated, whereas the transformation functions allow a
one-to-many mapping where many different data products can be stored
within the openghg object store depending on the inputs.

The main implementation for this at present, is in creating flux /
emissions maps based on underlying inventories or databases but this can
be expanded for use with any data type as appropriate.
