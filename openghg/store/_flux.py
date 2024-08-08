from __future__ import annotations
import logging
import inspect
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Optional, Tuple, Union
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


ArrayType = Optional[Union[ndarray, DataArray]]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class Flux(BaseStore):
    """This class is used to process flux / emissions flux data"""

    _data_type = "flux"
    _root = "Flux"
    _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

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
        domain: str,
        database: Optional[str] = None,
        database_version: Optional[str] = None,
        model: Optional[str] = None,
        source_format: str = "openghg",
        time_resolved: bool = False,
        high_time_resolution: bool = False,
        period: Optional[Union[str, tuple]] = None,
        chunks: Optional[Dict] = None,
        continuous: bool = True,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
        optional_metadata: Optional[Dict] = None,
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
        from openghg.types import FluxTypes
        from openghg.util import (
            clean_string,
            load_flux_parser,
            check_if_need_new_version,
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
            source_format = FluxTypes[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Load the data retrieve object
        parser_fn = load_flux_parser(source_format=source_format)

        _, unseen_hashes = self.check_hashes(filepaths=filepath, force=force)

        if not unseen_hashes:
            return {}

        filepath = next(iter(unseen_hashes.values()))

        if chunks is None:
            chunks = {}

        # Define parameters to pass to the parser function
        # TODO: Update this to match against inputs for parser function.
        # TODO - better match the arguments to the parser functions
        param = {
            "filepath": filepath,
            "species": species,
            "domain": domain,
            "source": source,
            "time_resolved": time_resolved,
            "period": period,
            "continuous": continuous,
            "data_type": "flux",
            "chunks": chunks,
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

        flux_data = parser_fn(**input_parameters)

        # Checking against expected format for Flux, and align to expected lat/lons if necessary.
        for split_data in flux_data.values():

            split_data["data"] = align_lat_lon(data=split_data["data"], domain=domain)

            em_data = split_data["data"]
            Flux.validate_data(em_data)

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
        for parsed_data in flux_data.values():
            parsed_data["metadata"].update(optional_metadata)

        data_type = "flux"
        datasource_uuids = self.assign_data(
            data=flux_data,
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

    def transform_data(
        self,
        datapath: Union[str, Path],
        database: str,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
        optional_metadata: Optional[Dict] = None,
        **kwargs: Dict,
    ) -> Dict:
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
        from openghg.types import FluxDatabases
        from openghg.util import load_flux_database_parser, check_if_need_new_version

        if overwrite and if_exists == "auto":
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                "See documentation for details of these inputs and options."
            )
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

        datapath = Path(datapath)

        try:
            data_type = FluxDatabases[database.upper()].value
        except KeyError:
            raise ValueError(f"Unable to transform '{database}' selected.")

        # Load the data retrieve object
        parser_fn = load_flux_database_parser(database=database)

        # Find all parameters that can be accepted by parse function
        all_param = list(inspect.signature(parser_fn).parameters.keys())

        # Define parameters to pass to the parser function from kwargs
        param: Dict[Any, Any] = {key: value for key, value in kwargs.items() if key in all_param}
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
        data_vars: Dict[str, Tuple[str, ...]] = {"flux": ("time", "lat", "lon")}
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
