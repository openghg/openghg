from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np
from xarray import Dataset
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from openghg.store import DataSchema

from openghg.store.base import BaseStore

__all__ = ["FluxTimeseries"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class FluxTimeseries(BaseStore):
    """This class is used to process ond dimension timeseries data"""

    _data_type = "flux_timeseries"
    """ _root = "FluxTimeseries"
    _uuid = "099b597b-0598-4efa-87dd-472dfe027f5d8"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"""

    def read_data(self, binary_data: bytes, metadata: dict, file_metadata: dict) -> dict | None:
        """Ready a footprint from binary data

        Args:
            binary_data: Footprint data
            metadata: Dictionary of metadata
            file_metadat: File metadata
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            try:
                filename = file_metadata["filename"]
            except KeyError:
                raise KeyError("We require a filename key for metadata read.")

            filepath = tmpdir_path.joinpath(filename)
            filepath.write_bytes(binary_data)

            return self.read_file(filepath=filepath, **metadata)

    def read_file(
        self,
        filepath: str | Path,
        species: str,
        source: str,
        region: str,
        domain: str | None = None,
        database: str | None = None,
        database_version: str | None = None,
        model: str | None = None,
        source_format: str = "crf",
        period: str | tuple | None = None,
        continuous: bool = True,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Any | None = None,
        filters: Any | None = None,
        optional_metadata: dict | None = None,
    ) -> dict:
        """Read one dimension timeseries file

        Args:
            filepath: Path of flux timeseries / emissions timeseries file
            species: Species name
            domain: Region for Flux timeseries
            source: Source of the emissions data, e.g. "energy", "anthro", default is 'anthro'.
            region: Region/Country of the CRF data
            domain: Geographic domain, default is 'None'. Instead region is used to identify area
            database: Name of database source for this input (if relevant)
            database_version: Name of database version (if relevant)
            model: Model name (if relevant)
            source_format : Type of data being input e.g. openghg (internal format)
            period: Period of measurements. Only needed if this can not be inferred from the time coords
            If specified, should be one of:
                - "yearly", "monthly"
                - suitable pandas Offset Alias
                - tuple of (value, unit) as would be passed to pandas.Timedelta function
            continuous: Whether time stamps have to be continuous.
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
                https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
            optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
        Returns:
            dict: Dictionary of datasource UUIDs data assigned to
        """
        # Get initial values which exist within this function scope using locals
        # MUST be at the top of the function
        fn_input_parameters = locals().copy()

        from openghg.store.spec import define_standardise_parsers

        from openghg.util import (
            clean_string,
            load_standardise_parser,
            check_if_need_new_version,
            split_function_inputs,
        )

        species = clean_string(species)
        source = clean_string(source)
        region = clean_string(region)
        if domain:
            domain = clean_string(domain)

        # Specify any additional metadata to be added
        additional_metadata = {}

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

        filepath = Path(filepath)

        standardise_parsers = define_standardise_parsers()[self._data_type]

        try:
            source_format = standardise_parsers[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Load the data retrieve object
        parser_fn = load_standardise_parser(data_type=self._data_type, source_format=source_format)

        # Get current parameter values and filter to only include function inputs
        fn_current_parameters = locals().copy()  # Make a copy of parameters passed to function
        fn_input_parameters = {key: fn_current_parameters[key] for key in fn_input_parameters}

        _, unseen_hashes = self.check_hashes(filepaths=filepath, force=force)

        if not unseen_hashes:
            return {}

        filepath = next(iter(unseen_hashes.values()))

        # Define parameters to pass to the parser function and remaining keys
        parser_input_parameters, additional_input_parameters = split_function_inputs(
            fn_input_parameters, parser_fn
        )

        flux_timeseries_data = parser_fn(**parser_input_parameters)

        # Checking against expected format for Flux
        for split_data in flux_timeseries_data.values():
            em_data = split_data["data"]
            FluxTimeseries.validate_data(em_data)

        # Check to ensure no required keys are being passed through optional_metadata dict
        self.check_info_keys(optional_metadata)
        if optional_metadata is not None:
            additional_metadata.update(optional_metadata)

        # Mop up and add additional keys to metadata which weren't passed to the parser
        flux_timeseries_data = self.update_metadata(
            flux_timeseries_data, additional_input_parameters, additional_metadata
        )

        data_type = "flux_timeseries"
        datasource_uuids = self.assign_data(
            data=flux_timeseries_data,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
            compressor=compressor,
            filters=filters,
        )

        # Record the file hash in case we see this file again
        self.store_hashes(unseen_hashes)

        return datasource_uuids

    @staticmethod
    def validate_data(data: Dataset) -> None:
        """
            Validate input data against FluxTimeseries schema - definition from
            FluxTimeseries.schema() method.

            Args:
                data : xarray Dataset in expected format

            Returns:
                None

        Raises: ValueError if the input data does not match the schema
                to the FluxTimeseries schema.
        """
        data_schema = FluxTimeseries.schema()
        data_schema.validate_data(data)

    @staticmethod
    def schema() -> DataSchema:
        """
        Define schema for one dimensional timeseries(FluxTimeseries) Dataset.

        Includes observation for each time of the defined domain:
            - "Obs"
                - expected dimensions: ("time")

        Expected data types for all variables and coordinates also included.

        Returns:
            DataSchema : Contains schema for FluxTimeseries.
        """
        from openghg.store import DataSchema

        data_vars: dict[str, tuple[str, ...]] = {"flux_timeseries": ("time",)}
        dtypes = {
            "time": np.datetime64,
            "flux_timeseries": np.floating,
        }

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format
