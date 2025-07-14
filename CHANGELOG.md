# Changelog

All notable changes to OpenGHG will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/openghg/openghg/compare/0.15.0...HEAD)

### Updated
- Updated temporary path creation to have user specific folder.[PR #]()
## [0.15.0] - 2025-07-02

### Added

- Allow to provide a kwargs dict to `resampler` function via `drop_na` that will be used by `xarray.Dataset.dropna` [PR #1314](https://github.com/openghg/openghg/pull/1314)
- Improved the check for nans in `surface_obs_resampler function`: drop data for times where any of `f"{species}"` or `"inlet"` variables are nan [PR #1314](https://github.com/openghg/openghg/pull/1314). This removed the improvements of `surface_obs_resampler` from [PR #1298](https://github.com/openghg/openghg/pull/1298).
- Fews fix for footprints standardisation : add `unify_chunks` in `openghg/store/storage/_localzarrstore.py` to prevent chunking errors while checking if chunks match; change name of function `check_function_open_nc` to `footprint_open_nc_fn` in `openghg/standardise/footprints/_acrg_org.py`; rewrite `footprint_open_nc_fn` to allow selection of the month (for badly formatted footprints) and footprints (same reason); and add a `"release_height"` variable to old format footprints (`format="acrg_org"`) for compatibility with new footprint format (`format="paris"`) [PR #1287](https://github.com/openghg/openghg/pull/1287)
- Option to compute modelled obs (and "fp x flux") by flux sector/source in `ModelScenario.footprints_data_merge`. [PR #1330](https://github.com/openghg/openghg/pull/1330)
- Option to return "fp x flux" from `ModelScenario.footprints_data_merge`. [PR #1328](https://github.com/openghg/openghg/pull/1328)
- Function to compute baseline sensitivities for NESW. This is used in `calc_modelled_baseline` and will be useful for OpenGHG inversions. [PR #1326](https://github.com/openghg/openghg/pull/1326)
- Added support for converting `calibration_scale` before plotting in the `plot_timeseries` function.[PR #1361](https://github.com/openghg/openghg/pull/1361)
- Method to update attributes of stored data. [PR #1375](https://github.com/openghg/openghg/pull/1375)
- Added "tag" keyword as an option when standardising data. This allows a list of user-specified tags to be included. This allows users to search and connect data which includes the chosen tags. [PR #1354](https://github.com/openghg/openghg/pull/1354)

### Updated

- Updated `ModelScenario` to work with the new PARIS footprint format for time-resolved footprints. [PR #1324](https://github.com/openghg/openghg/pull/1324)
- Updated the package release pyproject.toml and removed the setup.py to make sure PEP621 is followed. [PR #1345](https://github.com/openghg/openghg/pull/1345)
- Updated '_scale_convert' to 'convert' function from openghg_calscales package. [PR #1349](https://github.com/openghg/openghg/pull/1349)
- Renamed `optional_metadata` to `info_metadata` within `standardise_*` functions so this is more descriptive of how these keys are currently used [PR #1377](https://github.com/openghg/openghg/pull/1377)

### Fixed

- Added unit of `xch4` data var as units attribute to `mf` inside `get_obs_column`. [PR #1360](https://github.com/openghg/openghg/pull/1360)
- Added missing reference to mf_mod while plotting in the tutorial.[PR #1365](https://github.com/openghg/openghg/pull/1365)
- Made call to `.load` in `combine_datasets` optional. [PR #1371](https://github.com/openghg/openghg/pull/1371)
- Fixed bug where `force` keyword was not being used correctly for `standardise_surface` and wasn't allowing the same data to be added again. [PR #1374](https://github.com/openghg/openghg/pull/1374)

## [0.14.0] - 2025-04-16

### Added
- Added new tutorial for satellite ModelScenario processing.[PR #1304](https://github.com/openghg/openghg/pull/1304)
- Improved the check for nans in surface_obs_resampler function: drop data for times where any of `f"{species}"` or `"inlet"` variables are nan, or when both `f"{species}_variability"` and `f"{species}_repeatability"` are nans.[PR #1298](https://github.com/openghg/openghg/pull/1298)
- Added a `"keep_variables"` parameter in `get_obs_surface` to choose which variables we want to keep when retrieving data. This can be use to prevent resampling functions to try to resample unused variables filled with nans or string [PR #1283](https://github.com/openghg/openghg/pull/1283)
- Added a new resampling feature for obs where a f"{species}_variability" variable is present but not f"{species}_number_of_observation" [PR #1275](https://github.com/openghg/openghg/pull/1275)
- Added ability to retrieve ICOS combined Obspack .nc data. [PR #1212](https://github.com/openghg/openghg/pull/1212)
- Added ability to process ModelScenario for Observation and Footprint satellite data. Added `platform` keyword to split the process and added ability to pass `satellite` as argument.[#PR 1244](https://github.com/openghg/openghg/pull/1244)
- The `platform` keyword can now be used with surface data and can be passed to the standardise_surface function (e.g. "surface-insitu", "surface-flask"). This can be used to (a) separate data into different datasources based on platform when storing and (b) when deciding whether to resample data when aligning using ModelScenario methods. [PR #1278](https://github.com/openghg/openghg/pull/1278), [PR #1279](https://github.com/openghg/openghg/pull/1279) and [PR #1289](https://github.com/openghg/openghg/pull/1289).
- Added ability to reindex footprint data to obs data with tolerance of `1ms` with method="nearest".[#PR 1264](https://github.com/openghg/openghg/pull/1264)
- Added ability to standardise CORSO radiocarbon data, added new parser named `parse_icos_corso` to handle data modifications.[PR #1285](https://github.com/openghg/openghg/pull/1285)
- `tox` testing setup. [#PR 1268](https://github.com/openghg/openghg/pull/1268)
- Added abilty to parse reformatted NAME co2 footprints (PARIS format) to 'paris.py' [PR #1319](https://github.com/openghg/openghg/pull/1319)

### Updated

- For new object stores, a config file copied into this by default. If no config file is detected the internal defaults for the config are used instead. A custom config file can still be created as needed. [PR #1260](https://github.com/openghg/openghg/pull/1260)

### Fixed

- Possible circular import due to `get_metakeys`; the metakey config functionality was moved to the `store` module. [PR #1318](https://github.com/openghg/openghg/pull1318)
- Changed type definition from xr.Coordinates to xarray.core.coordinates.[PR #1316](https://github.com/openghg/openghg/pull/1316)
- Bugs of resampling functions : delete all variables in the obs data that are filled of nan, test the emptiness of the dataset, and delete "flag" variable (removed in [PR #1283] https://github.com/openghg/openghg/pull/1283), all that before resampling to prevent errors [PR #1275](https://github.com/openghg/openghg/pull/1275)
- Fixed bug where `period="varies"` could not be used or set when determining the time period associated with the input data. [#PR 1259](https://github.com/openghg/openghg/pull/1259) and [PR #1267](https://github.com/openghg/openghg/pull/1267)
- Dropped `exposure_id` variable for GOSAT data to avoid change in dimension size error raised from `to_zarr`. [PR #1243](https://github.com/openghg/openghg/pull/1243) [PR #1257](https://github.com/openghg/openghg/pull/1257)
- Drop `id` coordinate for GOSAT data to avoid merging errors [PR #1257](https://github.com/openghg/openghg/pull/1257)
- Fixed bugs in ModelScenario for satellite data e.g. requiring max_level as argument [#PR 1261](https://github.com/openghg/openghg/pull/1261)
- Fixed `get_*` functions if passed with `start_date` or `end_date` in format of ""dd:mm:yy T 00:00:0000" can still fetch the relevant data.[PR #1273](https://github.com/openghg/openghg/issues/1273)
- Fixed `numcodecs` version to be less than 0.16 to avoid ci runner failing while importing zarr.[PR #1296](https://github.com/openghg/openghg/pull/1296)

## [0.13.0] - 2025-03-10

### Added

- New `datapack` submodule to allow output obspacks to be created. This includes the `create_obspack` function which takes an input search file and produces an obspack within a defined structure from this. [PR #1117](https://github.com/openghg/openghg/pull/1117)

### Updated

- Unpinned numpy so that we can now use numpy 2.0. [PR #1235](https://github.com/openghg/openghg/pull/1235)
- When combining obs and footprint data in ModelScenario, allow resample_to to be set to None so that the data is aligned but not resampled. This is also turned on by default by passing the `platform` keyword and setting to any name which contains "flask" to the relevant ModelScenario methods. [PR #1236](https://github.com/openghg/openghg/pull/1236)
- Extracted `align_obs_and_other` from `ModelScenario.align_obs_footprint` into `analyse._alignment`. [PR #1234](https://github.com/openghg/openghg/pull/1234)

### Fixed

- Bug where attributes were not preserved during some resampling operations. [PR #1233](https://github.com/openghg/openghg/pull/1233)

## [0.12.0] - 2025-02-27

### Updated

- Update `standardise_column` inputs to include more explicit keywords around selection of satellite points. This includes adding the `obs_region` keyword to describe an area selected for satellite points (not necessarily the same as `domain`) and updating the definition of `selection` to be linked to any additional selection filters included for the satellite data. [#PR 1217](https://github.com/openghg/openghg/pull/1217/)
- Update `standardise_footprint` inputs to include more explicit keywords around selection of satellite points. This includes adding the `obs_region` keyword to describe an area selected for satellite points (not necessarily the same as `domain`) and updating the definition of `selection` to be linked to any additional selection filters included for the satellite data. [#PR 1218](https://github.com/openghg/openghg/pull/1218/)
- Output of parsers changed from nested dictionary to list of `MetadataAndData` objects. [PR #1199](https://github.com/openghg/openghg/pull/1199)

### Added

- Added parser to process and add "NIWA" network data to the object store. [PR #1208](https://github.com/openghg/openghg/pull/1208)
- Improved resampling of variability when number of observations is present. Also added methods for customising resampling, and a `Registry` class to "register" functions. [PR #1156](https://github.com/openghg/openghg/pull/1156)
- Allow parsers to return a list of `MetadataAndData` directly. [PR #1222](https://github.com/openghg/openghg/pull/1222)

### Fixed

- Changed `icos_data_level` to `data_level` in `ObsSurface.store_data` to fix bug where ICOS data was not distinguished by data level. [PR #1211](https://github.com/openghg/openghg/pull/1211)
- Fixed permissions for file locks [PR #1221](https://github.com/openghg/openghg/pull/1221)

## [0.11.1] - 2025-01-10

### Fixed

- Added `align_metadata_attributes` to retrieve_remote and shifted function defination to standardise/meta. [PR #1197](https://github.com/openghg/openghg/pull/1197)
- Added `icos flags` to handle data that is flagged bad for remote icos data.[PR #1200](https://github.com/openghg/openghg/pull/1200)
- Pinned Zarr to `2.18.3` as github runners are picking `zarr 3.0` which is still in significant development state.[PR #1205](https://github.com/openghg/openghg/pull/1205)
- Added exist_ok = true argument to create_config_folder and removed ObjectStoreError call.[PR #1198](https://github.com/openghg/openghg/pull/1198)

## [0.11.0] - 2024-12-16

### Fixed

- Fixed options used with `xr.Dataset.to_zarr` in reponse to updates in xarray. [PR #1160](https://github.com/openghg/openghg/pull/1160)
- Added xfail for `cfchecker` tests due to broken link. [PR #1178](https://github.com/openghg/openghg/pull/1178)
- Removed duplicate code from `read_file` method in `BoundaryConditions` and `EulerianModel`. [PR #1192](https://github.com/openghg/openghg/pull/1192)

### Added

- Check for file lock permissions with helpful error message. File locks are now created with rw permissions for user and group. [PR #1168](https://github.com/openghg/openghg/pull/1168)
- Removed parsers that are unused. [PR #1129](https://github.com/openghg/openghg/pull/1129)
- Added `data_owner` and `inlet_height_magl` as attributes to parse_icos. [PR #1147](https://github.com/openghg/openghg/pull/1147)
- Align the dataset(s) when opening the data while standardising footprints data to prevent error due to misalign coordinates. [PR #1164](https://github.com/openghg/openghg/pull/1164)
- Moved `sync_surface_metadata` to ObsSurface.read_file function so this is applied for all input data regardless of source_format. [PR #1138](https://github.com/openghg/openghg/pull/1138)
- Added `parse_co2_games` parser splitting multiple model from one file into separate datasources. [PR #1170](https://github.com/openghg/openghg/pull/1170)
- Added `dobj_url` as attribute to icos `retrieve_atmospheric` function. [PR #1174](https://github.com/openghg/openghg/pull/1174)
- Added parser for BoundaryConditions class. [PR #1180](https://github.com/openghg/openghg/pull/1180)
- Added parser for Eulerian Model class. [PR #1181](https://github.com/openghg/openghg/pull/1181)

### Updated

- Removed serverless/cloud code since it is not being used. [PR #1177](https://github.com/openghg/openghg/pull/1177)
- Minimum version of python to 3.10. [PR #1175](https://github.com/openghg/openghg/pull/1175)
- Updated ICOS standardise function to reflect changes in ASCII file format. [PR #1140](https://github.com/openghg/openghg/pull/1140)
- Added `rename_vars` option to `get_obs_surface` to allow variable names based around species to be returned. [PR #1130](https://github.com/openghg/openghg/pull/1130)
- Added option to `get_obs_column` to return the column data directly rather than converting to mole fractions. [PR #1131](https://github.com/openghg/openghg/pull/1131)
- Made calculations in `ModelScenario._calc_modelled_obs_HiTRes` more efficient. [PR #1062](https://github.com/openghg/openghg/pull/1062)
- Updated type hints and `typing` imports for Python 3.10 [PR #1193](https://github.com/openghg/openghg/pull/1193)

## [0.10.1] - 2024-09-27

### Fixed

- Bug where `search_surface` couldn't accept a dictionary argument for `data_level`. [PR #1133](https://github.com/openghg/openghg/pull/1133)
- GIT_TAG variable passed to build step of release_conda and also environment activation is executed for publishing to conda step. [PR #1135](https://github.com/openghg/openghg/pull/1135)

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
