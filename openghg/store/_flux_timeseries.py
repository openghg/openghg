from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np
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

    def read_data(self, binary_data: bytes, metadata: dict, file_metadata: dict) -> list[dict] | None:
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

    def format_inputs(self, **kwargs: Any) -> dict:
        """
        Apply appropriate formatting for expected inputs for FluxTimeseries. Expected
        inputs will typically be defined within the openghg.standardise.standardise_flux_timeseries()
        function.

        Args:
            kwargs: Set of keyword arguments. Selected keywords will be
                appropriately formatted.
        Returns:
            dict: Formatted parameters for this data type.
        """
        from openghg.util import clean_string, synonyms

        params = kwargs.copy()

        # Apply clean string formatting
        params["species"] = clean_string(params.get("species"))
        params["source"] = clean_string(params.get("source"))
        params["region"] = clean_string(params.get("region"))
        params["domain"] = clean_string(params.get("domain"))

        # Apply individual formatting as appropriate
        # - apply synonyms substitution for species
        species = params.get("species")
        if species is not None:
            params["species"] = synonyms(species)

        return params

    @staticmethod
    def schema() -> DataSchema:  # type: ignore[override]
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
