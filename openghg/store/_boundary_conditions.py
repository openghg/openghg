from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, TYPE_CHECKING

import numpy as np
from xarray import Dataset

from openghg.util import align_lat_lon, load_standardise_parser, split_function_inputs, synonyms

if TYPE_CHECKING:
    from openghg.store import DataSchema

from openghg.store.base import BaseStore
from openghg.store.spec import define_standardise_parsers

__all__ = ["BoundaryConditions"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class BoundaryConditions(BaseStore):
    """This class is used to process boundary condition data"""

    _data_type = "boundary_conditions"
    _root = "BoundaryConditions"
    _uuid = "4e787366-be91-4fc5-ad1b-4adcb213d478"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def read_data(
        self,
        binary_data: bytes,
        metadata: dict,
        file_metadata: dict,
        source_format: str,
    ) -> list[dict] | None:
        """Ready a footprint from binary data

        Args:
            binary_data: Footprint data
            metadata: Dictionary of metadata
            file_metadat: File metadata
            source_format : Type of data being input e.g. openghg (internal format)

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

    def format_inputs(self, **kwargs) -> tuple[dict, dict]:
        """ """
        from openghg.util import clean_string

        params = kwargs.copy()

        species = clean_string(params["species"])
        params["species"] = synonyms(species)
        params["bc_input"] = clean_string(params["bc_input"])
        params["domain"] = clean_string(params["domain"])

        # Specify any additional metadata to be added
        additional_metadata = {}

        return params, additional_metadata

    @staticmethod
    def schema() -> DataSchema:
        """
        Define schema for boundary conditions Dataset.

        Includes volume mole fractions for each time and ordinal, vertical boundary at the edge of the defined domain:
            - "vmr_n", "vmr_s"
                - expected dimensions: ("time", "height", "lon")
            - "vmr_e", "vmr_w"
                - expected dimensions: ("time", "height", "lat")

        Expected data types for all variables and coordinates also included.

        Returns:
            DataSchema : Contains schema for BoundaryConditions.
        """
        from openghg.store import DataSchema

        data_vars: dict[str, tuple[str, ...]] = {
            "vmr_n": ("time", "height", "lon"),
            "vmr_e": ("time", "height", "lat"),
            "vmr_s": ("time", "height", "lon"),
            "vmr_w": ("time", "height", "lat"),
        }
        dtypes = {
            "lat": np.floating,
            "lon": np.floating,
            "height": np.floating,
            "time": np.datetime64,
            "vmr_n": np.floating,
            "vmr_e": np.floating,
            "vmr_s": np.floating,
            "vmr_w": np.floating,
        }

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format
