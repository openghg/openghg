from __future__ import annotations
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Optional
import warnings
import numpy as np
from numpy import ndarray
from xarray import DataArray

from openghg.store import DataSchema
from openghg.store.base import BaseStore
from openghg.types import pathType
from openghg.util import synonyms

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

    def read_data(
        self, binary_data: bytes, metadata: dict, file_metadata: dict, source_format: str = "openghg"
    ) -> list[dict] | None:
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

            return self.read_file(filepath=filepath, source_format=source_format, **metadata)

    def format_inputs(self, **kwargs: Any) -> tuple[dict, dict]:
        """
        Apply appropriate formatting for expected inputs for Flux. Expected
        inputs will typically be defined within the openghg.standardise.standardise_flux()
        function.

        Args:
            kwargs: Set of keyword arguments. Selected keywords will be
                appropriately formatted.
        Returns:
            (dict, dict): Formatted parameters and any additional parameters
                for this data type.

        TODO: Decide if we can phase out additional_metadata or if this could be
            added to params.
        """
        from openghg.util import (
            clean_string,
        )

        # Apply clean_string first and then any specifics?
        # How do we check the keys we're expecting for this? Rely on required keys?

        params = kwargs.copy()

        species = clean_string(params["species"])
        params["species"] = synonyms(species)
        params["source"] = clean_string(params["source"])
        params["domain"] = clean_string(params["domain"])

        if params.get("high_time_resolution"):
            warnings.warn(
                "This argument is deprecated and will be replaced in future versions with time_resolved.",
                DeprecationWarning,
            )
            params["time_resolved"] = params["high_time_resolution"]

        # Specify any additional metadata to be added
        additional_metadata: dict = {}

        return params, additional_metadata

    def transform_data(
        self,
        datapath: pathType,
        database: str,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        compressor: Any | None = None,
        filters: Any | None = None,
        info_metadata: dict | None = None,
        **kwargs: dict,
    ) -> list[dict]:
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
        for mdd in flux_data:
            Flux.validate_data(mdd.data)

        required_keys = ("species", "source", "domain")

        if info_metadata:
            common_keys = set(required_keys) & set(info_metadata.keys())

            if common_keys:
                raise ValueError(
                    f"The following optional metadata keys are already present in required keys: {', '.join(common_keys)}"
                )
            else:
                for parsed_data in flux_data:
                    parsed_data.metadata.update(info_metadata)

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

        # "date" used to be part of the "keys" in the old datasource_uuids format
        if "date" in param:
            for du in datasource_uuids:
                du["date"] = param["date"]

        return datasource_uuids

    @staticmethod
    def schema() -> DataSchema:  # type: ignore[override]
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
