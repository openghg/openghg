# Changelog

All notable changes to OpenGHG will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/openghg/openghg/compare/0.5.0...HEAD)

## [0.5.0] - 2023-03-14

### Added

- New tutorial on changing object store path using command line
- New tutorial on adding data from EDGAR database
- Ability to use separate
- New keywords to allow metadata for emissions data to be more specific
- New command-line tool to setup user configuration file using `openghg --quickstart`
- New installation guide for users and developers
- Added check for 0-dimension time coordinates in some NetCDFs

### Fixed

- Fix for retrieval of different ICOS datasets
- Fix for user configuration file not being written out correctly

### Removed

- Supplementary site and species data moved to separate [supplementary_data](https://github.com/openghg/supplementary_data) repository
- Unused test data files

### Changed

- Print statements changed to logging
- Updated version of mypy used to 0.991
- Converted all tutorial notebooks to restructured text files

## [0.4.0] - 2022-11-12

### Added

- A new :ref:`DataHandler<DataHandler>` class for modification and deletion of metadata and added a matching tutorial.
- Move to a new user configuration file.
- A new `ObsColumn` class for handing satellite data

### Fixed

- Fixes for search parameters such as inlet height, sampling height
- Removed `date` parameter from Flux data to improve searchability
- Inlet and height are now aliases for each other for footprints and obs_surface
- Check for tuple of data and precisions file on processing of GCWERKS data

### Changed

- Documentation overhaul to ensure all functions are visible in either the user or developer documentation
- The `OPENGHG_PATH` environment variable check has been deprecated in favour of the user config file.

## [0.3.1] - 2022-07-19

### Fixed

- Removed old ``footprints_data_merge`` workflow which is superceded by ``ModelScenario``.
- Tidied and updated tutorial notebooks
- Updated the conda build recipe

### Removed

- Removed unused ``jobs`` submodule

## [0.3.0] - 2022-08-31

### Added

- Full ICOS and CEDA archive pulling capabilities in the cloud and locally.
- Adds logging to logfile, stored at ``~/openghg_log`` when run locally.
- Improved schema checking for different file formats, including footprint files.

### Fixed

- Fixes the ``clean_string`` function of ``openghg.util`` to allow dashes as these are commonly used in species names.
- Improves local routing functionality within cloud functions

### Changed

- OpenGHG now only supports Python >= 3.8

## [0.2.1] - 2022-07-27

### Added

- Adds improved ``get_obs_surface`` behaviour for cloud usage.
- Added shortcut routing for serverless functions, see ``openghg.cloud.call_function``. This differentiates between running on the hub or the cloud, where cloud is classed as running within a serverless function environment.
- Added new ``running_locally`` function in addition to the ``running_in_cloud`` or ``running_on_hub`` to allower easier checks with the ``openghg.client`` functions.

### Fixed

- Fix to metadata storage for different sampling period formats
- Fix to ``SearchResults`` where environment checks were resulting in attemping to access a local object store even though we were in a Hub environment.

## [0.2.0] - 2022-07-19

### Added

- New ``openghg.client`` functions for our cloud platform. Standardisation, searching and retrieval of data is passed to either the local or cloud function call depending on platform setup.

### Fixed

- Extra checks added to reading of emissions NetCDF datasets that have a single time value. Previously an error occurred due to performing a ``len`` on an unsized object.

### Removed

- The ``openghg.client.process`` functions have been removed. These have been replaced with ``openghg.client.standardise`` functions.

### Changed

- A split in the tutorials for cloud and local platforms. We've updated all the tutorials to better cover the differences in running OpenGHG with different setups.

## [0.1.0] - 2022-05-18

### Added

- Standardise measuremnts taken from many different sources - Tutorial 1 - Adding observation data
- Rank observations to ensure use of the best measurements - Tutorial 2 - Ranking observations
- Compare observations with emissions - Tutorial 3 - Comparing observations to emissions
- Create workflows using high time resolution CO2 data - Tutorial 4 - Working with high time resolution CO2
- Search for data in the OpenGHG data store and create plots to compare measurements - Tutorial 5 - Searching and plotting
- Retrieve and explore NOAA ObsPack data - Tutorial 6 - Exploring the NOAA ObsPack data
- Pull data from the ICOS Carbon Portal and the CEDA archive - Tutorial 7 - Retrieving data from ICOS and CEDA
