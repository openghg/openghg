from __future__ import annotations
import logging
from pathlib import Path
from typing import Any, cast
from collections.abc import Sequence

import numpy as np
from pandas import Timedelta
from xarray import Dataset
from openghg.standardise.meta import align_metadata_attributes
from openghg.store import DataSchema
from openghg.store.base import BaseStore
from openghg.types import multiPathType, pathType, resultsType, optionalPathType
from collections import defaultdict

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class ObsSurface(BaseStore):
    """This class is used to process surface observation data"""

    _data_type = "surface"
    _root = "ObsSurface"
    _uuid = "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def read_data(
        self,
        binary_data: bytes,
        metadata: dict,
        file_metadata: dict,
        precision_data: bytes | None = None,
        site_filepath: optionalPathType = None,
    ) -> dict:
        """Reads binary data passed in by serverless function.
        The data dictionary should contain sub-dictionaries that contain
        data and metadata keys.

        This is clunky and the ObsSurface.read_file function could
        be tidied up quite a lot to be more flexible.

        Args:
            binary_data: Binary measurement data
            metadata: Metadata
            file_metadata: File metadata such as original filename
            precision_data: GCWERKS precision data
            site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
                Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
        Returns:
            dict: Dictionary of result
        """
        from tempfile import TemporaryDirectory

        possible_kwargs = {
            "source_format",
            "network",
            "site",
            "inlet",
            "instrument",
            "sampling_period",
            "measurement_type",
            "if_exists",
            "save_current",
            "overwrite",
            "force",
            "source_format",
            "data_type",
        }

        # We've got a lot of functions that expect a file and read
        # metadata from its filename. As Acquire handled all of this behind the scenes
        # we'll create a temporary directory for now
        # TODO - add in just passing a filename to prevent all this read / write
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            try:
                filename = file_metadata["filename"]
            except KeyError:
                raise KeyError("We require a filename key for metadata read.")

            filepath = tmpdir_path.joinpath(filename)
            filepath.write_bytes(binary_data)

            meta_kwargs = {k: v for k, v in metadata.items() if k in possible_kwargs}

            if not meta_kwargs:
                raise ValueError("No valid metadata arguments passed, please check documentation.")

            if precision_data is None:
                result = self.read_file(filepath=filepath, **meta_kwargs)
            else:
                # We'll assume that if we have precision data it's GCWERKS
                # We don't read anything from the precision filepath so it's name doesn't matter
                precision_filepath = tmpdir_path.joinpath("precision_data.C")
                precision_filepath.write_bytes(precision_data)
                # Create the expected GCWERKS tuple
                result = self.read_file(
                    filepath=(filepath, precision_filepath),
                    site_filepath=site_filepath,
                    **meta_kwargs,
                )

        return result

    def read_file(
        self,
        filepath: multiPathType,
        source_format: str,
        site: str,
        network: str,
        inlet: str | None = None,
        height: str | None = None,
        instrument: str | None = None,
        data_level: str | int | float | None = None,
        data_sublevel: str | float | None = None,
        dataset_source: str | None = None,
        sampling_period: Timedelta | str | None = None,
        calibration_scale: str | None = None,
        measurement_type: str = "insitu",
        verify_site_code: bool = True,
        site_filepath: optionalPathType = None,
        update_mismatch: str = "never",
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Any | None = None,
        filters: Any | None = None,
        chunks: dict | None = None,
        optional_metadata: dict | None = None,
    ) -> dict:
        """Process files and store in the object store. This function
            utilises the process functions of the other classes in this submodule
            to handle each data type.

        Args:
            filepath: Filepath(s)
            source_format: Data format, for example CRDS, GCWERKS
            site: Site code/name
            network: Network name

            inlet: Inlet height. Format 'NUMUNIT' e.g. "10m".
                If retrieve multiple files pass None, OpenGHG will attempt to
                extract this from the file.
            height: Alias for inlet.
            read inlets from data.
            instrument: Instrument name
            data_level: The level of quality control which has been applied to the data.
                This should follow the convention of:
                    - "0": raw sensor output
                    - "1": automated quality assurance (QA) performed
                    - "2": final data set
                    - "3": elaborated data products using the data
            data_sublevel: Can be used to sub-categorise data (typically "L1") depending on different QA performed
                before data is finalised.
            dataset_source: Dataset source name, for example "ICOS", "InGOS", "European ObsPack", "CEDA 2023.06"
            sampling_period: Sampling period in pandas style (e.g. 2H for 2 hour period, 2m for 2 minute period).
            measurement_type: Type of measurement e.g. insitu, flask
            verify_site_code: Verify the site code
            site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
                Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
                        update_mismatch: This determines whether mismatches between the internal data
                attributes and the supplied / derived metadata can be updated or whether
                this should raise an AttrMismatchError.
                If True, currently updates metadata with attribute value.
            update_mismatch: This determines how mismatches between the internal data
                "attributes" and the supplied / derived "metadata" are handled.
                This includes the options:
                    - "never" - don't update mismatches and raise an AttrMismatchError
                    - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
                    - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
            if_exists: What to do if existing data is present.
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - just include new data and ignore previous
                - "combine" - replace and insert new data into current timeseries
            save_current: Whether to save data in current form and create a new version.
                - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
                - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
                - "n" / "no" - Allow current data to updated / deleted
            overwrite: Deprecated. This will use options for if_exists="new".
            force: Force adding of data even if this is identical to data stored.
            compressor: A custom compressor to use. If None, this will default to
                `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
            See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
            filters: Filters to apply to the data on storage, this defaults to no filtering. See
                https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters
            chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
                for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
                See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
                To disable chunking pass in an empty dictionary.
            optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
        Returns:
            dict: Dictionary of Datasource UUIDs

        TODO: Should "measurement_type" be changed to "platform" to align
        with ModelScenario and ObsColumn?
        """
        # Get initial values which exist within this function scope using locals
        # MUST be at the top of the function
        fn_input_parameters = locals().copy()

        from collections import defaultdict
        from openghg.store.spec import define_standardise_parsers
        from openghg.util import (
            clean_string,
            format_inlet,
            format_data_level,
            evaluate_sampling_period,
            check_and_set_null_variable,
            hash_file,
            load_standardise_parser,
            verify_site,
            check_if_need_new_version,
            split_function_inputs,
            synonyms,
        )

        standardise_parsers = define_standardise_parsers()[self._data_type]

        try:
            source_format = standardise_parsers[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Test that the passed values are valid
        # Check validity of site, instrument, inlet etc in 'site_info.json'
        # Clean the strings
        if verify_site_code:
            verified_site = verify_site(site=site)
            if verified_site is None:
                raise ValueError("Unable to validate site")
            else:
                site = verified_site
        else:
            site = clean_string(site)

        network = clean_string(network)
        instrument = clean_string(instrument)

        sampling_period = evaluate_sampling_period(sampling_period)

        # Ensure we have a clear missing value for data_level, data_sublevel
        data_level = format_data_level(data_level)
        if data_sublevel is not None:
            data_sublevel = str(data_sublevel)

        data_level = check_and_set_null_variable(data_level)
        data_sublevel = check_and_set_null_variable(data_sublevel)
        dataset_source = check_and_set_null_variable(dataset_source)

        data_level = clean_string(data_level)
        data_sublevel = clean_string(data_sublevel)
        dataset_source = clean_string(dataset_source)

        # Check if alias `height` is included instead of `inlet`
        if inlet is None and height is not None:
            inlet = height

        # Try to ensure inlet is 'NUM''UNIT' e.g. "10m"
        inlet = clean_string(inlet)
        inlet = format_inlet(inlet)

        # Would like to rename `data_source` to `retrieved_from` but
        # currently trying to match with keys added from retrieve_atmospheric (ICOS) - Issue #654
        data_source = "internal"

        # Define additional metadata which is not being passed to the parse functions
        additional_metadata = {
            "data_source": data_source,
        }

        if overwrite and if_exists == "auto":
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                "See documentation for details of these inputs and options."
            )
            if_exists = "new"

        # Making sure new version will be created by default if force keyword is included.
        if force and if_exists == "auto":
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

        # Load the data retrieve object
        parser_fn = load_standardise_parser(data_type=self._data_type, source_format=source_format)

        results: resultsType = defaultdict(dict)

        if chunks is None:
            chunks = {}

        if not isinstance(filepath, list):
            filepaths = [filepath]
        else:
            filepaths = filepath

        # Get current parameter values and filter to only include function inputs
        current_parameters = locals().copy()
        fn_input_parameters = {key: current_parameters[key] for key in fn_input_parameters}

        # Create a progress bar object using the filepaths, iterate over this below
        for fp in filepaths:
            if source_format == "GCWERKS":
                if isinstance(fp, tuple):
                    filepath = Path(fp[0])
                    precision_filepath = Path(fp[1])
                else:
                    raise TypeError("For GCWERKS data we expect a tuple of (data file, precision file).")
            else:
                filepath = fp

            # Cast so it's clear we no longer expect a tuple
            filepath = cast(str | Path, filepath)
            filepath = Path(filepath)

            fn_input_parameters["filepath"] = filepath

            # Define parameters to pass to the parser function and remaining keys
            parser_input_parameters, additional_input_parameters = split_function_inputs(
                fn_input_parameters, parser_fn
            )

            # This hasn't been updated to use the new check_hashes function due to
            # the added complication of the GCWERKS precision file handling,
            # so we'll just use the old method for now.
            file_hash = hash_file(filepath=filepath)
            if file_hash in self._file_hashes and overwrite is False:
                logger.warning(
                    "This file has been uploaded previously with the filename : "
                    f"{self._file_hashes[file_hash]} - skipping."
                )
                continue

            if source_format == "GCWERKS":
                parser_input_parameters["precision_filepath"] = precision_filepath

            # Call appropriate standardisation function with input parameters
            data = parser_fn(**parser_input_parameters)

            # Current workflow: if any species fails, whole filepath fails
            for key, value in data.items():
                species = key.split("_")[0]
                species = synonyms(species)
                try:
                    ObsSurface.validate_data(value["data"], species=species)
                except ValueError:
                    logger.error(
                        f"Unable to validate and store data from file: {filepath.name}.",
                        f" Problem with species: {species}\n",
                    )
                    validated = False
                    break
            else:
                validated = True

            if not validated:
                continue

            # Ensure the data is chunked
            if chunks:
                for key, value in data.items():
                    data[key]["data"] = value["data"].chunk(chunks)

            align_metadata_attributes(data=data, update_mismatch=update_mismatch)

            # Check to ensure no required keys are being passed through optional_metadata dict
            # before adding details
            self.check_info_keys(optional_metadata)
            if optional_metadata is not None:
                additional_metadata.update(optional_metadata)

            # Mop up and add additional keys to metadata which weren't passed to the parser
            data = self.update_metadata(data, additional_input_parameters, additional_metadata)

            # Create Datasources, save them to the object store and get their UUIDs
            data_type = "surface"
            datasource_uuids = self.assign_data(
                data=data,
                if_exists=if_exists,
                new_version=new_version,
                data_type=data_type,
                compressor=compressor,
                filters=filters,
            )

            results["processed"][filepath.name] = datasource_uuids
            logger.info(f"Completed processing: {filepath.name}.")

            self._file_hashes[file_hash] = filepath.name

        return dict(results)

    def read_multisite_aqmesh(
        self,
        filepath: pathType,
        metadata_filepath: pathType,
        network: str = "aqmesh_glasgow",
        instrument: str = "aqmesh",
        sampling_period: int = 60,
        measurement_type: str = "insitu",
        if_exists: str = "auto",
        overwrite: bool = False,
    ) -> defaultdict:
        """Read AQMesh data for the Glasgow network

        NOTE - temporary function until we know what kind of AQMesh data
        we'll be retrieve in the future.

        This data is different in that it contains multiple sites in the same file.
        """
        raise NotImplementedError(
            "This needs reworking for the new data storage method or removing as unused."
        )
        # from collections import defaultdict
        # from openghg.standardise.surface import parse_aqmesh
        # from openghg.store import assign_data
        # from openghg.util import hash_file

        # filepath = Path(filepath)
        # metadata_filepath = Path(metadata_filepath)

        # if overwrite and if_exists == "auto":
        #     logger.warning(
        #         "Overwrite flag is deprecated in preference to `if_exists` input."
        #         "See documentation for details of this input and options."
        #     )
        #     if_exists = "new"

        # # Get a dict of data and metadata
        # processed_data = parse_aqmesh(filepath=filepath, metadata_filepath=metadata_filepath)

        # results: resultsType = defaultdict(dict)
        # for site, site_data in processed_data.items():
        #     metadata = site_data["metadata"]
        #     measurement_data = site_data["data"]

        #     file_hash = hash_file(filepath=filepath)

        #     if self.seen_hash(file_hash=file_hash) and not force:
        #         raise ValueError(
        #             f"This file has been uploaded previously with the filename : {self._file_hashes[file_hash]}.\n"
        #              "If necessary, use force=True to bypass this to add this data."
        #         )
        #         break

        #     combined = {site: {"data": measurement_data, "metadata": metadata}}

        #     required_keys = (
        #         "site",
        #         "species",
        #         "inlet",
        #         "network",
        #         "instrument",
        #         "sampling_period",
        #         "measurement_type",
        #     )

        #     uuid = lookup_results[site]

        #     # Jump through these hoops until we can rework the data assignment functionality to split it out
        #     # into more sensible functions
        #     # TODO - fix the assign data function to avoid this kind of hoop jumping
        #     lookup_result = {site: uuid}

        #     # Create Datasources, save them to the object store and get their UUIDs
        #     data_type = "surface"
        #     datasource_uuids = assign_data(
        #         data_dict=combined,
        #         lookup_results=lookup_result,
        #         overwrite=overwrite,
        #         data_type=data_type,
        #     )

        #     results[site] = datasource_uuids

        #     # Record the Datasources we've created / appended to
        #     self.add_datasources(uuids=datasource_uuids, data=combined, metastore=self._metastore)

        #     # Store the hash as the key for easy searching, store the filename as well for
        #     # ease of checking by user
        #     self.set_hash(file_hash=file_hash, filename=filepath.name)

        # return results

    @staticmethod
    def schema(species: str) -> DataSchema:
        """
        Define schema for surface observations Dataset.

        Only includes mandatory variables
            - standardised species name (e.g. "ch4")
            - expected dimensions: ("time")

        Expected data types for variables and coordinates also included.

        Returns:
            DataSchema : Contains basic schema for ObsSurface.

        # TODO: Decide how to best incorporate optional variables
        # e.g. "ch4_variability", "ch4_number_of_observations"
        """
        from openghg.standardise.meta import define_species_label

        name = define_species_label(species)[0]

        data_vars: dict[str, tuple[str, ...]] = {name: ("time",)}
        dtypes = {name: np.floating, "time": np.datetime64}

        source_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return source_format

    @staticmethod
    def validate_data(data: Dataset, species: str) -> None:
        """
        Validate input data against ObsSurface schema - definition from
        ObsSurface.schema() method.

        Args:
            data : xarray Dataset in expected format
            species: Species name

        Returns:
            None

            Raises a ValueError with details if the input data does not adhere
            to the ObsSurface schema.
        """
        data_schema = ObsSurface.schema(species)
        data_schema.validate_data(data)

    def store_data(
        self,
        data: dict,
        if_exists: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        required_metakeys: Sequence | None = None,
        compressor: Any | None = None,
        filters: Any | None = None,
    ) -> dict | None:
        """This expects already standardised data such as ICOS / CEDA

        Args:
            data: Dictionary of data in standard format, see the data spec under
            Development -> Data specifications in the documentation
            if_exists: What to do if existing data is present.
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - creates new version with just new data
                - "combine" - replace and insert new data into current timeseries
            overwrite: Deprecated. This will use options for if_exists="new".
            force: Force adding of data even if this is identical to data stored (checked based on previously retrieved file hashes).
            required_metakeys: Keys in the metadata we should use to store this metadata in the object store
                if None it defaults to:
                    {"species", "site", "station_long_name", "inlet", "instrument",
                    "network", "source_format", "data_source", "icos_data_level"}
            compressor: A custom compressor to use. If None, this will default to
                `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
                See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
            filters: Filters to apply to the data on storage, this defaults to no filtering. See
                https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
        Returns:
            Dict or None:
        """
        from openghg.util import hash_retrieved_data

        if overwrite and if_exists == "auto":
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` input."
                "See documentation for details of this input and options."
            )
            if_exists = "new"

        # TODO: May need to delete
        # obs = ObsSurface.load()
        # metastore = load_metastore(key=obs._metakey)

        # Very rudimentary hash of the data and associated metadata
        hashes = hash_retrieved_data(to_hash=data)
        # Find the keys in data we've seen before
        if force:
            file_hashes_to_compare = set()
        else:
            file_hashes_to_compare = {next(iter(v)) for k, v in hashes.items() if k in self._retrieved_hashes}

        # Making sure data can be force overwritten if force keyword is included.
        if force and if_exists == "auto":
            if_exists = "new"

        if len(file_hashes_to_compare) == len(data):
            logger.warning("Note: There is no new data to process.")
            return None

        keys_to_process = set(data.keys())
        if file_hashes_to_compare:
            # TODO - add this to log
            logger.warning(f"Note: We've seen {file_hashes_to_compare} before. Processing new data only.")
            keys_to_process -= file_hashes_to_compare

        to_process = {k: v for k, v in data.items() if k in keys_to_process}

        if required_metakeys is None:
            required_metakeys = (
                "species",
                "site",
                "station_long_name",
                "inlet",
                "instrument",
                "network",
                "source_format",
                "data_source",
                "icos_data_level",
            )

        # Create Datasources, save them to the object store and get their UUIDs
        data_type = "surface"
        # This adds the parsed data to new or existing Datasources by performing a lookup
        # in the metastore
        datasource_uuids = self.assign_data(
            data=to_process,
            if_exists=if_exists,
            data_type=data_type,
            required_keys=required_metakeys,
            min_keys=5,
            compressor=compressor,
            filters=filters,
        )

        self.store_hashes(hashes=hashes)

        return datasource_uuids

    def store_hashes(self, hashes: dict) -> None:
        """Store hashes of data retrieved from a remote data source such as
        ICOS or CEDA. This takes the full dictionary of hashes, removes the ones we've
        seen before and adds the new.

        Args:
            hashes: Dictionary of hashes provided by the hash_retrieved_data function
        Returns:
            None
        """
        new = {k: v for k, v in hashes.items() if k not in self._retrieved_hashes}
        self._retrieved_hashes.update(new)

    def delete(self, uuid: str) -> None:
        """Delete a Datasource with the given UUID

        This function deletes both the record of the object store in he

        Args:
            uuid (str): UUID of Datasource
        Returns:
            None
        """
        from openghg.objectstore import delete_object
        from openghg.store.base import Datasource

        # Load the Datasource and get all its keys
        # iterate over these keys and delete them
        datasource = Datasource(bucket=self._bucket, uuid=uuid)

        data_keys = datasource.raw_keys()

        for version in data_keys:
            key_data = data_keys[version]

            for daterange in key_data:
                key = key_data[daterange]
                delete_object(bucket=self._bucket, key=key)

        # Then delete the Datasource itself
        key = f"{Datasource._datasource_root}/uuid/{uuid}"
        delete_object(bucket=self._bucket, key=key)

        # Delete the UUID from the metastore
        self._metastore.delete({"uuid": uuid})

    def seen_hash(self, file_hash: str) -> bool:
        return file_hash in self._file_hashes

    def set_hash(self, file_hash: str, filename: str) -> None:
        self._file_hashes[file_hash] = filename
