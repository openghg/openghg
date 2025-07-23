from __future__ import annotations
from pathlib import Path
from typing import Any
import numpy as np
from xarray import Dataset
import logging

from openghg.store import DataSchema
from openghg.store.base import BaseStore
from openghg.util import load_standardise_parser, split_function_inputs

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

__all__ = ["EulerianModel"]


# TODO: Currently built around these keys but will probably need more unique distiguishers for different setups
# model name
# species
# date (start_date)
# ...
# setup (included as option for now)


class EulerianModel(BaseStore):
    """This class is used to process Eulerian model data"""

    _data_type = "eulerian_model"
    _root = "EulerianModel"
    _uuid = "63ff2365-3ba2-452a-a53d-110140805d06"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def format_inputs(self, **kwargs) -> tuple[dict, dict]:
        """ """
        from openghg.util import clean_string, synonyms

        params = kwargs.copy()

        species = clean_string(params["species"])
        params["species"] = synonyms(species)

        params["model"] = clean_string(params["model"])
        
        params["start_date"] = clean_string(params["start_date"])
        params["end_date"] = clean_string(params["end_date"])
        params["setup"] = clean_string(params["setup"])

        # Specify any additional metadata to be added
        additional_metadata = {}

        return params, additional_metadata

    @staticmethod
    def schema() -> DataSchema:
        """
        Define schema for Eulerian model Dataset.

        At present, this doesn't check the variables but does check that
        "lat", "lon", "time" are included as appropriate types.

        Returns:
            DataSchema : Contains dummy schema for EulerianModel.

        TODO: Decide on data_vars checks as we build up the use of this data_type
        """
        data_vars: dict[str, tuple[str, ...]] = {}
        dtypes = {"lat": np.floating, "lon": np.floating, "time": np.datetime64}

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format

    @staticmethod
    def validate_data(data: Dataset) -> None:
        """
        Validate input data against EulerianModel schema - definition from
        EulerianModel.schema() method.

        Args:
            data : xarray Dataset in expected format

        Returns:
            None

            Raises a ValueError with details if the input data does not adhere
            to the EulerianModel schema.
        """
        data_schema = EulerianModel.schema()
        data_schema.validate_data(data)

    def validate_data_internal(self, data: Dataset) -> None:
        """ """
        EulerianModel.validate_data(data=data)
