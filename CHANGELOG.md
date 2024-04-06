# Changelog

All notable changes to OpenGHG will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/openghg/openghg/compare/0.8.0...HEAD)

### Added

- Ability to convert from an old style NetCDF object store to the new Zarr based store format - [PR #967](https://github.com/openghg/openghg/pull/967)
- Updated `parse_edgar` function to handle processing of v8.0 Edgar datasets. [PR #965](https://github.com/openghg/openghg/pull/965)

### Fixed

- Added check to verify if `obs` and `footprint` have overlapping time coordinates when creating a `ModelScenario` object, if not then raise `ValueError` - [PR #954](https://github.com/openghg/openghg/pull/954)

## [0.8.0] - 2024-03-19

This version brings a breaking change with the move to use the [Zarr](https://zarr.dev/) file format for data storage. This means that object stores created with previous versions of OpenGHG will need to be repopulated to use this version. You should notice improvements in time taken for standardisation, memory consumption and disk usage. With the use of Zarr comes the ability for the user to control the way which data is processed and stored. Please see [the documentation](https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking) for more on this.

### Added

- Added option to pass `store` argument to `ModelScenario` init method. [PR #928](https://github.com/openghg/openghg/pull/928)

### Fixed

- Issue caused when passing a list of files to be processed. If OpenGHG had seen some of the files before it would refuse to process any of them - [PR #890](https://github.com/openghg/openghg/pull/890)

### Changed

- Moved to store data in [Zarr](https://github.com/zarr-developers/zarr-python) stores, this should reduce both the size of the object store and memory consumption whilst processing and retrieving data - [PR #803](https://github.com/openghg/openghg/pull/803)
- standardise_footprint was updated to allow a source_format input to be specified. This currently only supports "acrg_org" type but can be expanded upon [PR #914](https://github.com/openghg/openghg/pull/914).
- Internal format for "footprint" data type was updated to rename meteorological variable names to standard names [PR #918](https://github.com/openghg/openghg/pull/918).
- Standardise_footprint now uses the meterological model input as a distinguishing keyword when adding data. [PR #955](https://github.com/openghg/openghg/pull/955).
- Meterological model input renamed from `metmodel` to `met_model` [PR #957](https://github.com/openghg/openghg/pull/957).
- Updated internal naming and input data_type to use "flux" rather than "emissions" consistently. - [PR #827](https://github.com/openghg/openghg/pull/827)

### Added

- More more explanation regarding use of `search_*` and `get_*` function in tutorial 1 [PR #952](https://github.com/openghg/openghg/pull/952)

## [0.7.1] - 2024-03-01

### Fixed

- Bug fix for conversion of species parameter with its synonym value inside get_obs_surface_local. [PR #871](https://github.com/openghg/openghg/pull/871)
- Missing requirement for filelock package added to conda environment file [PR #857](https://github.com/openghg/openghg/pull/857)
- Missing store argument adding to search function allow searching within specific object stores [PR #859](https://github.com/openghg/openghg/pull/859)
- Bug fix for allowing a period to be specified when this cannot be inferred from the input data [PR #899](https://github.com/openghg/openghg/pull/899)


## [0.7.0] - 2023-12-15

### Added

- Added `DeprecationWarning` to the functions `parse_cranfield` and  `parse_btt`. - [PR #792](https://github.com/openghg/openghg/pull/792)
- Added `environment-dev.yaml` file for developer conda environment - [PR #769](https://github.com/openghg/openghg/pull/769)
- Added generic `standardise` function that accepts a bucket as an argument, and used this to refactor `standardise_surface` etc, and tests that standardise data - [PR #760](https://github.com/openghg/openghg/pull/760)
- Added `MetaStore` abstract base class as interface for metastore classes, and a `ClassicMetaStore` subclass implements the same bucket/key structure as the previous metastore.
  All references to TinyDB are now in the `objectstore` module, meaning that there is only one place where code needs to change to use a different backend with the metastore - [PR #771](https://github.com/openghg/openghg/pull/771)
- Added compression to `Datasource.save` and modified `Datasource.load` to take advantage of lazy loading via `xarray.open_dataset` - [PR #755](https://github.com/openghg/openghg/pull/755)
- Added progress bars using `rich` package - [PR #718](https://github.com/openghg/openghg/pull/718)
- Added config for Black to `pyproject.toml` - [PR #822](https://github.com/openghg/openghg/pull/822)
- Added `force` option to `retrieve_atmospheric` and `ObsSurface.store_data` so that retrieved hashes can be ignored - [PR #819](https://github.com/openghg/openghg/pull/819)
- Added `SafetyCachingMiddleware` to metastore, which caches writes and only saves them to disk if the underlying file
has not changed. This is to prevent errors when concurrent writes are made to the metastore. [PR #836](https://github.com/openghg/openghg/pull/836)

### Fixed

- Bug fix for sampling period attribute having a value of "NOT_SET" and combining the observation and footprint data. Previously this was raising a ValueError. [PR #808](https://github.com/openghg/openghg/pull/808)
- Bug where `radon` was not fetched using `retrieve_atmospheric` from icos data. - [PR #794](https://github.com/openghg/openghg/pull/794)
- Bug with CRDS parse function where data for all species was being dropped if only one species was missing - [PR #829](https://github.com/openghg/openghg/pull/829)
- Datetime processing has been updated to be compatible with Pandas 2.0: the `date_parser` argument of `read_csv` was deprecated in favour of `date_format`. [PR #816](https://github.com/openghg/openghg/pull/816)
- Updated ICOS retrieval functionality to match new metadata retrieved from ICOS Carbon Portal - [PR #806](https://github.com/openghg/openghg/pull/806)
- Added "parse_intem" function to parse intem emissions files - [PR #804](https://github.com/openghg/openghg/pull/804)

### Changed

- Datasource UUIDs are no longer stored in the storage class and are now only stored in the metadata store - [PR #752](https://github.com/openghg/openghg/pull/752)
- Support dropped for Python 3.8 - [PR #818](https://github.com/openghg/openghg/pull/818). OpenGHG now supports Python >= 3.9.


## [0.6.2] - 2023-08-07

### Fixed

- Bug where the object store path being written to JSON led to an invalid path being given to some users - [PR #741](https://github.com/openghg/openghg/pull/741)

### Changed

- Added read-only opening of the metadata store of each storage class when searching. This is done using a `mode` argument pased to the `load_metastore` function - [PR #763](https://github.com/openghg/openghg/pull/763)

## [0.6.1] - 2023-08-04

### Added

- Added `rich` package to printing out SearchResults object in a table format. If using an editable install please update your environment to match requirements.txt / environment.yml - [PR #696](https://github.com/openghg/openghg/pull/696)

### Fixed

- Bug in `get_readable_buckets`: missing check for tutorial store - [PR #729](https://github.com/openghg/openghg/pull/729)
- Bug when adding high time resolution footprints to object store: they were not being distinguished from low resolution footprints - [PR #720](https://github.com/openghg/openghg/pull/720)
- Bug due to `object_store` key not being present in `Datasource` metadata - [PR #725](https://github.com/openghg/openghg/pull/725)
- Bug in `DataManager` where a string was interpreted as a list when processing metadata keys to be deleted - [PR #713](https://github.com/openghg/openghg/pull/713)

## [0.6.0] - 2023-07-18

### Added

- Multiple object stores are now supported. Any number of stores may be accessed and read from and written to - [PR #664](https://github.com/openghg/openghg/pull/664)
- Added `standardise_column()` wrapper function - [PR #569](https://github.com/openghg/openghg/pull/643)
- The 'height_name' definition from the [openghg/supplementary_data repository](https://github.com/openghg/supplementary_data) for each site can now be accessed, used and interpreted - [PR #648](https://github.com/openghg/openghg/pull/648)
- Allow metadata within metastore to be updated for existing data sources for the latest version, start and end dates of the data - [PR #652](https://github.com/openghg/openghg/pull/652)
- Allow mismatches between values in metadata and data attributes to be updated to either match to the metadata or attribute collated details - [PR #682](https://github.com/openghg/openghg/pull/682)

### Changed

- Configuration file format has been updated to support multiple object stores, note directing users to upgrade has been added
- The `openghg --quickstart` functionality has been updated to allow multiple objects stores to be added and migrate users from the previous version of the configuration file
- Synchronise metadata within metastore to align with data sources when updating (including latest version, start and end dates of the data) - [PR #652](https://github.com/openghg/openghg/pull/652) and [PR #664](https://github.com/openghg/openghg/pull/664)
- The name of the `DataHandler` class has been changed to `DataManager` to better reflect its function.

### Removed

- Reading multi-site AQMesh data is no longer possible. This may be reintroduced if required.

## [0.5.1] - 2023-05-10

### Fixed

- Fix for the sampling period of data files being read incorrectly - [PR #584](https://github.com/openghg/openghg/pull/584)
- Fix for overlapping dateranges being created when adding new data to a Datasource. This introduced errors when keys were being removed and updated - [PR #570](https://github.com/openghg/openghg/pull/570)
- Incorrect data being retrieved by the ICOS retrieval function - [PR #611](https://github.com/openghg/openghg/pull/611)
- Error raised on attempt to delete object store after it wasn't created by some tests - [PR #626](https://github.com/openghg/openghg/pull/626)
- Very small (nanosecond) changes in period between measurements resulting in error due to `pandas.infer_period` not being able to return a period - [PR #634](https://github.com/openghg/openghg/pull/634)
- Processing of ObsPack resulted in errors due to limited metadata read and data overwrite, temporary fix in place for now - [PR #642](https://github.com/openghg/openghg/pull/642)

### Changed

- Mock for `openghg_defs` data to remove external dependency for tests - [PR #582](https://github.com/openghg/openghg/pull/582)
- Removed use of environment variable for test store, moved to mock - [PR # 580](https://github.com/openghg/openghg/pull/580)
- Temporary pinning of pandas < 2.0 due to changes that introduced errors - [PR #619](https://github.com/openghg/openghg/pull/619)
- `ObsData.plot_timeseries` now uses `openghg.plotting.plot_timeseries` to avoid duplication in efforts/code - [PR #624](https://github.com/openghg/openghg/pull/624)
- `openghg.util._user.get_user_config_path` now creates `openghg.conf` in `~/.openghg` - [PR #690](https://github.com/openghg/openghg/pull/690)

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

- A new `DataHandler` class for modification and deletion of metadata and added a matching tutorial.
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

- Removed old `footprints_data_merge` workflow which is superceded by `ModelScenario`.
- Tidied and updated tutorial notebooks
- Updated the conda build recipe

### Removed

- Removed unused `jobs` submodule

## [0.3.0] - 2022-08-31

### Added

- Full ICOS and CEDA archive pulling capabilities in the cloud and locally.
- Adds logging to logfile, stored at `~/openghg_log` when run locally.
- Improved schema checking for different file formats, including footprint files.

### Fixed

- Fixes the `clean_string` function of `openghg.util` to allow dashes as these are commonly used in species names.
- Improves local routing functionality within cloud functions

### Changed

- OpenGHG now only supports Python >= 3.8

## [0.2.1] - 2022-07-27

### Added

- Adds improved `get_obs_surface` behaviour for cloud usage.
- Added shortcut routing for serverless functions, see `openghg.cloud.call_function`. This differentiates between running on the hub or the cloud, where cloud is classed as running within a serverless function environment.
- Added new `running_locally` function in addition to the `running_in_cloud` or `running_on_hub` to allower easier checks with the `openghg.client` functions.

### Fixed

- Fix to metadata storage for different sampling period formats
- Fix to `SearchResults` where environment checks were resulting in attemping to access a local object store even though we were in a Hub environment.

## [0.2.0] - 2022-07-19

### Added

- New `openghg.client` functions for our cloud platform. Standardisation, searching and retrieval of data is passed to either the local or cloud function call depending on platform setup.

### Fixed

- Extra checks added to reading of emissions NetCDF datasets that have a single time value. Previously an error occurred due to performing a `len` on an unsized object.

### Removed

- The `openghg.client.process` functions have been removed. These have been replaced with `openghg.client.standardise` functions.

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
