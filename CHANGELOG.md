# Changelog

All notable changes to OpenGHG will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/openghg/openghg/compare/0.10.1...HEAD)

### Added

- Removed parsers that are unused. [PR #1129](https://github.com/openghg/openghg/pull/1129)

### Updated

- Updated ICOS standardise function to reflect changes in ASCII file format. [PR #1140](https://github.com/openghg/openghg/pull/1140)


## [0.10.1] - 2024-09-27

### Fixed

- Bug where `search_surface` couldn't accept a dictionary argument for `data_level`. [PR #1133](https://github.com/openghg/openghg/pull/1133)
- GIT_TAG variable passed to build step of release_conda and also environment activation is executed for publishing to conda step. [PR #1135](https://github.com/openghg/openghg/pull/1135) 


### Updated

- Added option to `get_obs_column` to return the column data directly rather than converting to mole fractions. [PR #1131](https://github.com/openghg/openghg/pull/1131)


## [0.10.0] - 2024-09-24

### Updated

- Updated parse_* functions for surface data type to accept `filepath` rather than `data_filepath`. This maps better to the `standardise_surface` input and makes this consistent with the other data types. [PR #1101](https://github.com/openghg/openghg/pull/1101)
- Separated data variable formatting from assign attributes function into dataset_formatter function.- [PR #1102](https://github.com/openghg/openghg/pull/1102)
- Required and optional keys for "column" data type were updated to reflect the two sources of this data (site, satellite) [PR #1104](https://github.com/openghg/openghg/pull/1104)
- Ensure metadata keywords for NOAA obspack are consistent with wider definitions including renaming data_source to dataset_source (data_source would be "internal"). [PR #1110](https://github.com/openghg/openghg/pull/1110)
- Updated data type classes to dynamically select inputs to pass to parse function and to include any required/optional keys not passed to the parse function within the metadata. [PR #1111](https://github.com/openghg/openghg/pull/1111)
- When adding new data sources, updated how lookup keys add optional keys. This used to only extract these from the optional_metadata input but this now allows keys to be added through any metadata. [PR #1112](https://github.com/openghg/openghg/pull/1112)
- Formalising metadata data merging logic within new util.metadata_util functions. [PR #1113](https://github.com/openghg/openghg/pull/1113)
- Splited build and publish steps in the workflow and check for `-` and `.` in the tags for build and publish. [PR #759](https://github.com/openghg/openghg/pull/759)

### Fixed

- Bug where a datasource's folder in the `data` directory was not deleted by `Datasource.delete_all_data()`. This was causing `check_zarr_store` in `util/_user.py` to give a false negative. [PR #1126](https://github.com/openghg/openghg/pull/1126) 
- Bug where an input filepath list to standardise_surface was only storing the last file hash. This allowed for some files to bypass the check for the same files depending on where they were in the original filepath list. [PR #1100](https://github.com/openghg/openghg/pull/1100)
- Bug where filepath needed to be a Path object when storing the file hash values. [PR #1108](https://github.com/openghg/openghg/pull/1108)
- Catch an `AttributeError` when trying synchronise attributes and metadata and a user passes a `bool` - [PR #1029](https://github.com/openghg/openghg/pull/1029)
- Mypy issue fixed for `util.download_data()` function based on updates described [requests Issue 465](https://github.com/psf/requests/issues/465) and included in [urllib3 PR 159](https://github.com/urllib3/urllib3/pull/159/files). This allowed the `decode_content` flag to be set directly rather than needing to patch the method. [PR #1118](https://github.com/openghg/openghg/pull/1118)


## [0.9.0] - 2024-08-14

### Added

- In `get_obs_surface`, if `inlet` is passed a slice and multiple search results are found, they will be combined into a single `ObsData` object with a "inlet" data variable. [PR #1066](https://github.com/openghg/openghg/pull/1066)
- Packaging and release documentation. [PR #961](https://github.com/openghg/openghg/pull/961)
- Options to search metastore by "negative lookup" and by "test functions"; the latter is used to implement 
  searching by a `slice` object to find a range of values - [PR #1064](https://github.com/openghg/openghg/pull/1064)
- Code to combine multiple data objects - [PR #1063](https://github.com/openghg/openghg/pull/1063)
- A new object store config file to allow customisation of metadata keys used to store data in unique Datasources - [PR #1041](https://github.com/openghg/openghg/pull/1041)
- Adds `data_level` and `data_sublevel` as additional keys which can be used to distinguish observation surface data - [PR #1051](https://github.com/openghg/openghg/pull/1051)
- New keywords `data_level` and `data_sublevel` as additional keys which can be used to distinguish observation surface data - [PR #1051](https://github.com/openghg/openghg/pull/1051)
- The `dataset_source` keyword previously only used for retrieved data is now available when using `standardise_surface` as well. This allows an origin key for the dataset to be included e.g. "InGOS", "European ObsPack". [PR #1083](https://github.com/openghg/openghg/pull/1083)
- Footprint parser for "paris" footprint format which includes the new format used for both the NAME and FLEXPART LPDM models - [PR #1070](https://github.com/openghg/openghg/pull/1070)
- Added `AGAGE` as a source format in the `standardise_surface` function, and associated parser functions and tests. Reads in output files from Matt Rigby's agage-archive - [PR #912](https://github.com/openghg/openghg/pull/912)
- Added feature of file sorting at standardise level before processing and remove `filepaths` input option - [PR #1074](https://github.com/openghg/openghg/pull/1074)
- Utility functions to combine multiple "data objects" (e.g. `ObsData`, or anything with `.data` and `.metadata` attributes) - [PR #1063](https://github.com/openghg/openghg/pull/1063) 

### Updated

- Updated `base` to `offset` in `resample` due to xarray deprecation. [PR #1073](https://github.com/openghg/openghg/pull/1073)
- Updated `get_obs_column` to output mole fraction details. This involves using the apriori level data above a maximum level and applying a correction to the column data (aligned with this process within [acrg code](https://github.com/ACRG-Bristol/acrg)). [PR #1050](https://github.com/openghg/openghg/pull/1050)
- The `data_source` keyword is now included as "internal" when using `standardise_surface` to distinguish this from data retrieved from external sources (e.g. "icos", "noaa_obspack"). [PR #1083](https://github.com/openghg/openghg/pull/1083)
- Added interactive timeseries plots in Search and Plotting tutorial. [PR #953](https://github.com/openghg/openghg/pull/953) 
- Pinned the `icoscp` version within requirements to 0.1.17 based on new authentication requirements. [PR #1084](https://github.com/openghg/openghg/pull/1084)
- The `icos_data_level` metadata keyword is now retired and replaced with `data_level` when using the `retrieve.icos.retrieve_atmopsheric` workflow to access data from the ICOS Carbon Portal. [PR #1087](https://github.com/openghg/openghg/pull/1087)
- Removing 'station_long_name' and 'data_type' as required keys from the metadata config file as these do not need to be used as keys to distinguish datasources when adding new data. [PR #1088](https://github.com/openghg/openghg/pull/1088)
- The sort flag can now be passed via the SearchResults.retrieve interfaces to choose whether the data is returned sorted along the time axis. [PR #1090](https://github.com/openghg/openghg/pull/1090)

### Fixed

- Bug in test when checking customised chunks were stored correctly in the zarr store. dask v2024.8 now changed the chunk shape after this was sorted was test was updated to ensure this didn't sort when retrieving the data. [PR #1090](https://github.com/openghg/openghg/pull/1090)
- Error reporting for `BaseStore` context manager. [PR #1059](https://github.com/openghg/openghg/pull/1059)
- Formatting of `inlet` (and related keys) in search, so that float values of inlet can be retrieved - [PR #1057](https://github.com/openghg/openghg/pull/1057)
- Test for zarr compression `test_bytes_stored_compression` that was failing due to a slight mismatch between actual and expected values. The test now uses a bound on relative error - [PR #1065](https://github.com/openghg/openghg/pull/1065)
- Typo and possible performance issue in `analysis._scenario.combine_datasets` - [PR #1047](https://github.com/openghg/openghg/pull/1047)
- Pinned numpy to < 2.0 and netcdf4 to <= 1.6.5. Numpy 2.0 release caused some minor bugs in OpenGHG, and netCDF4's updates to numpy 2.0 were also causing tests to fail - [PR #1043](https://github.com/openghg/openghg/pull/1043)
- Fixed bug where slightly different latitude and longitude values were being standardised and not aligned later down the line. These are now all fixed to the openghg_defs domain definitions where applicable upon standardisation. [PR #1049](https://github.com/openghg/openghg/pull/1049) 

## [0.8.2] - 2024-06-06

### Fixed

- Updated incorrect import for data_manager within tutorial. This now shows the import from `openghg.dataobjects` not `openghg.store` - [PR #1007](https://github.com/openghg/openghg/pull/1007)
- Issue causing missing data when standardising multiple files in a loop - [PR #1032](https://github.com/openghg/openghg/pull/1032)

### Added

- Added ability to process CRF data as `flux_timeseries` datatype (one dimensional data) - [PR #870](https://github.com/openghg/openghg/pull/870)

## [0.8.1] - 2024-05-17

### Added

- Ability to convert from an old style NetCDF object store to the new Zarr based store format - [PR #967](https://github.com/openghg/openghg/pull/967)
- Updated `parse_edgar` function to handle processing of v8.0 Edgar datasets. [PR #965](https://github.com/openghg/openghg/pull/965)
- Argument `time_resolved` is added as phase 1 change for `high_time_resolution`, also metadata is updated and added deprecation warning. - [PR #968](https://github.com/openghg/openghg/pull/968)
- Added ability to pass additional tags as optional metadata through `standardising_*` and `transform_flux_data functions`. [PR #981](https://github.com/openghg/openghg/pull/981)
- Renaming `high_time_resolution` argument to `time_resolved` in metadata as more appropriate description for footprints going forward and added deprecation warning. - [PR #968](https://github.com/openghg/openghg/pull/968)
- Added explicit backwards compatability when searching previous object stores containing the `high_time_resolution` keyword rather than `time_resolved` - [PR #990](https://github.com/openghg/openghg/pull/990)
- Added ability to pass additional tags as optional metadata through `standardise_*` and `transform_flux_data functions`. [PR #981](https://github.com/openghg/openghg/pull/981)
- Check added object stores listed in the configuration which are in the previous format with a warning raised for this [PR #962](https://github.com/openghg/openghg/pull/962)

### Fixed

- `source` can be passed to `transform_flux_data` with the EDGAR parser; `date` isn't stored with the transformed EDGAR data, since this is used for choosing what data to add, but doesn't describe all of the data in the object store. Fixed bug due to string split over two lines in logging message - [#PR 1010](https://github.com/openghg/openghg/pull/1010)
- Fixed problem where the zarr store check raised an error for empty stores, preventing new zarr stores from being created - [#PR 993](https://github.com/openghg/openghg/pull/993)
- Retrieval of level 1 data from the ICOS Carbon Portal now no longer tries to retrieve a large number of CSV files - [#PR 868](https://github.com/openghg/openghg/pull/868)
- Added check for duplicate object store path being added under different store name, if detected raises `ValueError`. - [PR #904](https://github.com/openghg/openghg/pull/904)
- Added check to verify if `obs` and `footprint` have overlapping time coordinates when creating a `ModelScenario` object, if not then raise `ValueError` - [PR #954](https://github.com/openghg/openghg/pull/954)
- Added fix to make sure data could be returned within a date range when the data had been added non-sequentially to an object store - [PR #997](https://github.com/openghg/openghg/pull/997)
- Replace references to old `supplementary_data` repository with `openghg_defs` - [PR #999](https://github.com/openghg/openghg/pull/999)
- Added call to synonyms for species while standardising - [PR #984](https://github.com/openghg/openghg/pull/984)

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

### Fixed

- Bug fix for passing calibration_scale as optional parameter to the parser function. [PR #872](https://github.com/openghg/openghg/pull/872)

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
