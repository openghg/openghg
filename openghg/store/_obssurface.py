from __future__ import annotations
import logging
from pathlib import Path
from typing import Any, MutableSequence
from collections.abc import Sequence
import numpy as np
import xarray as xr

from openghg.standardise.meta import align_metadata_attributes, dataset_formatter, assign_attributes
from openghg.store import DataSchema
from openghg.store.base import BaseStore
from openghg.types import pathType, MetadataAndData, DataOverlapError
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
        site_filepath: pathType | None = None,
    ) -> list[dict] | None:
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
                    filepath=filepath,
                    precision_filepath=[precision_filepath],
                    site_filepath=site_filepath,
                    **meta_kwargs,
                )

        return result

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

    def format_inputs(self, **kwargs: Any) -> dict:
        """
        Apply appropriate formatting for expected inputs for ObsColumn. Expected
        inputs will typically be defined within the openghg.standardse.standardise_surface()
        function.

        Args:
            kwargs: Set of keyword arguments. Selected keywords will be
                appropriately formatted.
        Returns:
            dict: Formatted parameters for this data type.

        TODO: Decide if we can phase out additional_metadata or if this could be
            added to params.
        """
        from openghg.util import (
            verify_site,
            clean_string,
            evaluate_sampling_period,
            format_platform,
            format_data_level,
            format_inlet,
            check_and_set_null_variable,
        )

        params = kwargs.copy()

        # Apply pre-checks and formatting
        # - check and verify site code based on flag
        verify_site_code = params.get("verify_site_code")
        if verify_site_code:
            params.pop("verify_site_code")
        site_filepath = params.get("site_filepath")
        site_filepath_str = "default" if site_filepath is None else site_filepath

        if verify_site_code is True:
            site = params.get("site")
            if site is not None:
                verified_site = verify_site(site, site_filepath=site_filepath)
                if verified_site is not None:
                    params["site"] = verified_site
                else:
                    msg = (
                        f"Unable to verify site from site code: {site}. (Site filepath: {site_filepath_str})"
                    )
                    logger.exception(msg)
                    raise ValueError(msg)
            else:
                msg = "Unable to verify site: site details not specified"
                logger.exception(msg)
                raise ValueError(msg)
        else:
            params["site"] = clean_string(params["site"])

        # - make sure `inlet` OR the alias `height` is included
        #    - note: from this point only `inlet` variable should be used.        params["inlet"] = params.get("inlet")
        if params.get("inlet") is None and params.get("height") is not None:
            params["inlet"] = params["height"]
            params.pop("height")

        # - make sure data_sublevel is cast as a string
        params["data_sublevel"] = params.get("data_sublevel")
        if params["data_sublevel"] is not None:
            params["data_sublevel"] = str(params["data_sublevel"])

        # Apply clean string formatting
        params["inlet"] = clean_string(params.get("inlet"))
        params["network"] = clean_string(params.get("network"))
        params["instrument"] = clean_string(params.get("instrument"))
        params["data_level"] = clean_string(params.get("data_level"))
        params["data_sublevel"] = clean_string(params.get("data_sublevel"))
        params["platform"] = clean_string(params.get("platform"))
        params["dataset_source"] = clean_string(params.get("dataset_source"))

        # Apply individual formatting as appropriate

        # - check and evaluate sampling_period is in correct format
        params["sampling_period"] = evaluate_sampling_period(params.get("sampling_period"))

        # - format platform and populate measurement_type
        params["platform"] = format_platform(params.get("platform"), data_type=self._data_type)
        if params.get("measurement_type") is None and params.get("platform") is not None:
            params["measurement_type"] = params["platform"]

        # - try to ensure inlet is 'NUM''UNIT' e.g. "10m"
        params["inlet"] = format_inlet(params.get("inlet"))

        # - check data level
        params["data_level"] = format_data_level(params.get("data_level"))

        # Ensure we have a clear missing value (not_set) where needed (required keys)
        params["data_level"] = check_and_set_null_variable(params.get("data_level"))
        params["data_sublevel"] = check_and_set_null_variable(params.get("data_sublevel"))
        params["platform"] = check_and_set_null_variable(params.get("platform"))
        params["dataset_source"] = check_and_set_null_variable(params.get("dataset_source"))

        if params.get("precision_filepath") is not None:
            if not isinstance(params["precision_filepath"], list):
                params["precision_filepath"] = [Path(params["precision_filepath"])]
            else:
                params["precision_filepath"] = [Path(pfp) for pfp in params["precision_filepath"]]

        # Include additional, internally-defined keywords
        # Would like to rename `data_source` to `retrieved_from` but
        # currently trying to match with keys added from retrieve_atmospheric (ICOS) - Issue #654
        data_source = "internal"
        params["data_source"] = data_source

        return params

    def align_metadata_attributes(self, data: list[MetadataAndData], update_mismatch: str) -> None:
        """
        Check values within metadata and attributes are consistent and update (in place).
        This is a wrapper for separate openghg.util.align_metadata_attributes() function.

        Args:
            data: sequence of MetadataAndData objects
            update_mismatch: This determines how mismatches between the internal data
                "attributes" and the supplied / derived "metadata" are handled.
                This includes the options:
                    - "never" - don't update mismatches and raise an AttrMismatchError
                    - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
                    - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
        Returns:
            None

        TODO: At the moment the align_metadata_attributes() function is only applicable
            to surface data but this should be generalised to all data types.
        """
        return align_metadata_attributes(data, update_mismatch)

    def define_loop_params(self) -> dict:
        """
        If filepath is supplied as a list, depending on the data type this will be
        looped over to extract each file. If there are additional parameters which need to
        be looped over as well (when defined) these are defined here.

        Returns:
            dict: Dictionary of name of loop parameters within inputs and to pass
                to the relevant parse functions.
        """
        loop_params = {  # "filepath": "filepaths",
            "precision_filepath": "precision_filepath",
        }
        return loop_params

    @staticmethod
    def schema(species: str) -> DataSchema:  # type: ignore[override]
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

    def store_data(
        self,
        data: MutableSequence[MetadataAndData],
        if_exists: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        required_metakeys: Sequence | None = None,
        compressor: Any | None = None,
        filters: Any | None = None,
    ) -> list[dict] | None:
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
            list of dicts containing details of stored data, or None
        """
        if overwrite and if_exists == "auto":
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` input."
                "See documentation for details of this input and options."
            )
            if_exists = "new"

        # TODO: May need to delete
        # obs = ObsSurface.load()
        # metastore = load_metastore(key=obs._metakey)

        # Making sure data can be force overwritten if force keyword is included.
        if force and if_exists == "auto":
            if_exists = "new"

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
                "data_level",
            )

        # Create Datasources, save them to the object store and get their UUIDs
        # This adds the parsed data to new or existing Datasources by performing a lookup
        # in the metastore

        # Workaround to maintain old behavior without using hashes
        # TODO: when zarr store updates are made, make default to combine any
        # new data with the existing, ignoring new data that overlaps
        datasource_uuids = []

        for mdd in data:
            try:
                datasource_uuid = self.assign_data(
                    data=[mdd],
                    if_exists=if_exists,
                    required_keys=required_metakeys,
                    min_keys=5,
                    compressor=compressor,
                    filters=filters,
                )
            except DataOverlapError:
                data_info = ", ".join(f"{key}={mdd.metadata.get(key)}" for key in required_metakeys)
                logger.info(f"Skipping data that overlaps existing data:\n\t{data_info}.")
            else:
                datasource_uuids.extend(datasource_uuid)

        return datasource_uuids

    def seen_hash(self, file_hash: str) -> bool:
        return file_hash in self._file_hashes

    def set_hash(self, file_hash: str, filename: str) -> None:
        self._file_hashes[file_hash] = filename


    def prepare_surface_gas_data(self,
            dataset: xr.Dataset,
            metadata: dict,
            update_mismatch: str = "never",
            site_filepath: str | None = None,
            species_filepath: str | None = None,
        ) -> dict:
            """
            Wraps a dataset into the gas_data format with metadata, attributes,
            and CF-compliant attributes applied (like in parser workflow).
            """
            # TODO: Replace this logic in surface parsers
            species = metadata["species"]

            gas_data = {
                species: {
                    "metadata": metadata,
                    "data": dataset,
                    "attributes": dataset.attrs,
                }
            }

            # Format dataset variables
            gas_data = dataset_formatter(data=gas_data)

            # Attach CF-compliant attributes
            gas_data = assign_attributes(
                data=gas_data,
                site=metadata.get("site"),
                network=metadata.get("network"),
                sampling_period=metadata.get("sampling_period"),
                update_mismatch=update_mismatch,
                site_filepath=site_filepath,
                species_filepath=species_filepath,
            )

            return gas_data