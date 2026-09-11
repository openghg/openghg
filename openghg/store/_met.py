import logging
from typing import Any

from openghg.store import DataSchema
from openghg.store.base import BaseStore

__all__ = ["SiteMet"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class SiteMet(BaseStore):
    """ """

    _data_type = "site_met"
    _root = "SiteMet"
    _uuid = "dbb725a1-4102-4804-b732-9e2159fe04f1"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def format_inputs(self, **kwargs: Any) -> dict:
        """
        Apply appropriate formatting for expected inputs for SiteMet data. Expected
        inputs will typically be defined within the openghg.standardise.standardise_site_met()
        function.

        Args:
            kwargs: Set of keyword arguments. Selected keywords will be
                appropriately formatted.
        Returns:
            dict: Formatted parameters for this data type.
        """
        from openghg.util import (
            clean_string,
        )

        params = kwargs.copy()

        # Apply clean string formatting
        params["site"] = clean_string(params.get("site"))
        params["network"] = clean_string(params.get("network"))
        params["met_source"] = clean_string(params.get("met_source"))

        return params

    @staticmethod
    def schema() -> DataSchema:  # type: ignore[override]
        """
        Define schema for met Dataset.

        Currently includes expected coordinates:
            - ("time", "lat", "lon")

        Expected data types for coordinates also included.

        Returns:
            DataSchema : Contains schema for Flux.

        TODO: Expand to include data variables as well e.g. "pressure_level"
        """
        return DataSchema.from_name("site_met")
