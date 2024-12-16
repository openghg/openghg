from __future__ import annotations
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Optional
import warnings
import numpy as np
from numpy import ndarray
from openghg.store import DataSchema
from openghg.store.base import BaseStore
from openghg.util import synonyms, align_lat_lon

from xarray import DataArray, Dataset

__all__ = ["Flux"]


logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


ArrayType = Optional[ndarray | DataArray]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class Flux(BaseStore):
    """This class is used to process flux / emissions flux data"""

    _data_type = "flux"
    _root = "Flux"
    _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

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
        domain: str,
        database: str | None = None,
        database_version: str | None = None,
        model: str | None = None,
        source_format: str = "openghg",
        time_resolved: bool = False,
        high_time_resolution: bool = False,
        period: str | tuple | None = None,
        chunks: dict | None = None,
        continuous: bool = True,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Any | None = None,
        filters: Any | None = None,
        optional_metadata: dict | None = None,
    ) -> dict:
        """Read flux / emissions file

        Args:
            filepath: Path of flux / emissions file
            species: Species name
            domain: Flux / Emissions domain
            source: Flux / Emissions source
            database: Name of database source for this input (if relevant)
            database_version: Name of database version (if relevant)
            model: Model name (if relevant)
            source_format : Type of data being input e.g. openghg (internal format)
            time_resolved: If this is a high resolution file
            high_time_resolution: This argument is deprecated and will be replaced in future versions with time_resolved.
            period: Period of measurements. Only needed if this can not be inferred from the time coords
            If specified, should be one of:
                - "yearly", "monthly"
                - suitable pandas Offset Alias
                - tuple of (value, unit) as would be passed to pandas.Timedelta function
            chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
                for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
                See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
                To disable chunking pass in an empty dictionary.
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
        species = synonyms(species)
        source = clean_string(source)
        domain = clean_string(domain)

        if high_time_resolution:
            warnings.warn(
                "This argument is deprecated and will be replaced in future versions with time_resolved.",
                DeprecationWarning,
            )
            time_resolved = high_time_resolution

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

        _, unseen_hashes = self.check_hashes(filepaths=filepath, force=force)

        if not unseen_hashes:
            return {}

        filepath = next(iter(unseen_hashes.values()))

        if chunks is None:
            chunks = {}

        # Get current parameter values and filter to only include function inputs
        fn_current_parameters = locals().copy()  # Make a copy of parameters passed to function
        fn_input_parameters = {key: fn_current_parameters[key] for key in fn_input_parameters}

        # Define parameters to pass to the parser function and remaining keys
        parser_input_parameters, additional_input_parameters = split_function_inputs(
            fn_input_parameters, parser_fn
        )

        parser_input_parameters["data_type"] = self._data_type

        flux_data = parser_fn(**parser_input_parameters)

        # Checking against expected format for Flux, and align to expected lat/lons if necessary.
        for split_data in flux_data.values():

            split_data["data"] = align_lat_lon(data=split_data["data"], domain=domain)

            em_data = split_data["data"]
            Flux.validate_data(em_data)

        # Check to ensure no required keys are being passed through optional_metadata dict
        self.check_info_keys(optional_metadata)
        if optional_metadata is not None:
            additional_metadata.update(optional_metadata)

        # Mop up and add additional keys to metadata which weren't passed to the parser
        flux_data = self.update_metadata(flux_data, additional_input_parameters, additional_metadata)

        data_type = "flux"
        datasource_uuids = self.assign_data(
            data=flux_data,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
            compressor=compressor,
            filters=filters,
        )

        # Record the file hash in case we see this file again
        self.store_hashes(unseen_hashes)

        return datasource_uuids

    def transform_data(
        self,
        datapath: str | Path,
        database: str,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        compressor: Any | None = None,
        filters: Any | None = None,
        optional_metadata: dict | None = None,
        **kwargs: dict,
    ) -> dict:
        """
        Read and transform a flux / emissions database. This will find the appropriate
        parser function to use for the database specified. The necessary inputs
        are determined by which database is being used.

        The underlying parser functions will be of the form:
            - openghg.transform.flux.parse_{database.lower()}
                - e.g. openghg.transform.flux.parse_edgar()

        Args:
            datapath: Path to local copy of database archive (for now)
            database: Name of database
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
            compressor: A custom compressor to use. If None, this will default to
                `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
                See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
            filters: Filters to apply to the data on storage, this defaults to no filtering. See
                https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
            **kwargs: Inputs for underlying parser function for the database.

                Necessary inputs will depend on the database being parsed.

        TODO: Could allow Callable[..., Dataset] type for a pre-defined function be passed
        """
        import inspect
        from openghg.store.spec import define_transform_parsers
        from openghg.util import load_transform_parser, check_if_need_new_version

        if overwrite and if_exists == "auto":
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                "See documentation for details of these inputs and options."
            )
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

        datapath = Path(datapath)

        transform_parsers = define_transform_parsers()[self._data_type]

        try:
            data_type = transform_parsers[database.upper()].value
        except KeyError:
            raise ValueError(f"Unable to transform '{database}' selected.")

        # Load the data retrieve object
        parser_fn = load_transform_parser(data_type=self._data_type, source_format=database)

        # Find all parameters that can be accepted by parse function
        all_param = list(inspect.signature(parser_fn).parameters.keys())

        # Define parameters to pass to the parser function from kwargs
        param: dict[Any, Any] = {key: value for key, value in kwargs.items() if key in all_param}
        param["datapath"] = datapath  # Add datapath explicitly (for now)

        flux_data = parser_fn(**param)

        # Checking against expected format for Flux
        for split_data in flux_data.values():
            em_data = split_data["data"]
            Flux.validate_data(em_data)

        required_keys = ("species", "source", "domain")

        if optional_metadata:
            common_keys = set(required_keys) & set(optional_metadata.keys())

            if common_keys:
                raise ValueError(
                    f"The following optional metadata keys are already present in required keys: {', '.join(common_keys)}"
                )
            else:
                for key, parsed_data in flux_data.items():
                    parsed_data["metadata"].update(optional_metadata)

        data_type = "flux"
        datasource_uuids = self.assign_data(
            data=flux_data,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
            required_keys=required_keys,
            compressor=compressor,
            filters=filters,
        )

        return datasource_uuids

    @staticmethod
    def schema() -> DataSchema:
        """
        Define schema for flux / emissions Dataset.

        Includes flux/emissions for each time and position:
            - "flux"
                - expected dimensions: ("time", "lat", "lon")

        Expected data types for all variables and coordinates also included.

        Returns:
            DataSchema : Contains schema for Flux.
        """
        data_vars: dict[str, tuple[str, ...]] = {"flux": ("time", "lat", "lon")}
        dtypes = {"lat": np.floating, "lon": np.floating, "time": np.datetime64, "flux": np.floating}

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format

    @staticmethod
    def validate_data(data: Dataset) -> None:
        """
        Validate input data against Flux schema - definition from
        Flux.schema() method.

        Args:
            data : xarray Dataset in expected format

        Returns:
            None

            Raises a ValueError with details if the input data does not adhere
            to the Flux schema.
        """
        data_schema = Flux.schema()
        data_schema.validate_data(data)
