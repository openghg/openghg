from __future__ import annotations

import logging
import inspect
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np
from xarray import Dataset
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

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

    def read_data(self, binary_data: bytes, metadata: Dict, file_metadata: Dict) -> Optional[Dict]:
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
        filepath: Union[str, Path],
        species: str,
        source: str,
        region: str,
        domain: Optional[str] = None,
        database: Optional[str] = None,
        database_version: Optional[str] = None,
        model: Optional[str] = None,
        source_format: str = "crf",
        period: Optional[Union[str, tuple]] = None,
        continuous: bool = True,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
        optional_metadata: Optional[Dict] = None,
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
        from openghg.types import FluxTimeseriesTypes

        from openghg.util import (
            clean_string,
            load_flux_timeseries_parser,
            check_if_need_new_version,
        )

        species = clean_string(species)
        source = clean_string(source)
        region = clean_string(region)
        if domain:
            domain = clean_string(domain)

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

        try:
            source_format = FluxTimeseriesTypes[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Load the data retrieve object
        parser_fn = load_flux_timeseries_parser(source_format=source_format)

        _, unseen_hashes = self.check_hashes(filepaths=filepath, force=force)

        if not unseen_hashes:
            return {}

        filepath = next(iter(unseen_hashes.values()))

        # Define parameters to pass to the parser function
        # TODO: Update this to match against inputs for parser function.
        param = {
            "filepath": filepath,
            "species": species,
            "region": region,
            "source": source,
            "data_type": "flux_timeseries",
            "period": period,
            "continuous": continuous,
        }

        optional_keywords: dict[Any, Any] = {
            "database": database,
            "database_version": database_version,
            "model": model,
        }

        signature = inspect.signature(parser_fn)
        fn_accepted_parameters = [param.name for param in signature.parameters.values()]

        input_parameters: dict[Any, Any] = param.copy()

        # Checks if optional parameters are present in function call and includes them else ignores its inclusion in input_parameters.
        for param, param_value in optional_keywords.items():
            if param in fn_accepted_parameters:
                input_parameters[param] = param_value
            else:
                logger.warning(
                    f"Input: '{param}' (value: {param_value}) is not being used as part of the standardisation process."
                    f"This is not accepted by the current standardisation function: {parser_fn}"
                )

        flux_timeseries_data = parser_fn(**input_parameters)

        # Checking against expected format for Flux
        for split_data in flux_timeseries_data.values():
            em_data = split_data["data"]
            FluxTimeseries.validate_data(em_data)

            # combine metadata and get look-up keys
        if optional_metadata is None:
            optional_metadata = {}

        # Make sure none of these are Nones
        to_add = {k: v for k, v in optional_keywords.items() if v is not None}

        # warn if `optional_metadata` overlaps with keyword arguments
        overlap = [k for k in optional_metadata if k in to_add]
        if overlap:
            msg = (
                f"Values for {', '.join(overlap)} in `optional_metadata` are "
                "being overwritten by values passed as keyword arguments."
            )
            logger.warning(msg)

        # update `optional_metadata` dict with any "optional" arguments passed to the parser
        optional_metadata.update(to_add)

        lookup_keys = self.get_lookup_keys(optional_metadata=optional_metadata)

        # add optional metdata to parsed metadata
        for parsed_data in flux_timeseries_data.values():
            parsed_data["metadata"].update(optional_metadata)

        data_type = "flux_timeseries"
        datasource_uuids = self.assign_data(
            data=flux_timeseries_data,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
            required_keys=lookup_keys,
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

        data_vars: Dict[str, Tuple[str, ...]] = {"flux_timeseries": ("time",)}
        dtypes = {
            "time": np.datetime64,
            "flux_timeseries": np.floating,
        }

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format
