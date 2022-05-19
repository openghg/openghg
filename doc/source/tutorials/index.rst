=========
Tutorials
=========

These tutorials will get you up and running with OpenGHG so you can process, analyse and query data.
The pages linked to below are created from Jupyter Notebooks that are available in the OpenGHG
repository under ``notebooks/beginner_workflow``.


Tutorial 1 - Adding observation data
------------------------------------

This tutorial covers adding data to the object store using the standardisation functions.

.. toctree::
   :maxdepth: 1

   beginner_workflow/1_Adding_observation_data

Tutorial 2 - Ranking observations
---------------------------------

This tutorial covers ranking the data available in the object store. This ensures users always find the correct data when using the search
functions.

.. toctree::
   :maxdepth: 1

   beginner_workflow/2_Ranking_for_observations

Tutorial 3 - Comparing observations to emissions
------------------------------------------------

This tutorial covers the workflow comparing observations data to emissions using the new ``ModelScenario`` class.
This replaces the ``footprint_data_merge`` function with an object that can be used to retrieve and manipulate observations,
emissions and footprint data (with boundary conditions in the pipeline).

.. toctree::
   :maxdepth: 1

   beginner_workflow/3_Comparing_with_emissions

Tutorial 4 - Working with high time resolution CO2
--------------------------------------------------

.. toctree::
   :maxdepth: 1

   beginner_workflow/4_Working_with_co2

Tutorial 5 - Exploring the NOAA ObsPack data
--------------------------------------------

.. toctree::
   :maxdepth: 1

   beginner_workflow/5_Searching_and_plotting


Tutorial 6 - Searching and plotting
-----------------------------------

.. toctree::
   :maxdepth: 1

   beginner_workflow/6_Explore_NOAA_ObsPack


Tutorial 7 - Retrieving data from ICOS and CEDA
-----------------------------------------------

.. toctree::
   :maxdepth: 1

   beginner_workflow/7_Retrieving_remote_data
