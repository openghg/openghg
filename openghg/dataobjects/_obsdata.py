from collections import abc
from json import dumps
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterator, Union, Optional
from openghg.plotting import plot_timeseries as general_plot_timeseries
import plotly.graph_objects as go
import xarray as xr

from typing import Dict

# from openghg.objectstore import
from openghg.store.base import LocalZarrStore
import zarr

# from ._basedata import _BaseData

__all__ = ["ObsData"]


class ObsData:
    """This class is used to return observations data from the get_observations function

    Args:
        uuid: UUID of Datasource
        version: Version of data requested from Datasrouce
        metadata: Dictionary of metadata
        compute: Open zarr store as dataset
    """

    def __init__(self, uuid: str, version: str, metadata: Dict) -> None:
        self._bucket = metadata["object_store"]
        self._uuid = uuid
        self._version = version
        self.metadata = metadata
        self._memory_store = {}
        self._data = None

        # Add the data to the memory store
        # TODO - make this a generic zarr store
        with LocalZarrStore(bucket=self._bucket, datasource_uuid=uuid, mode="r") as zarr_store:
            zarr.convenience.copy_store(
                source=zarr_store,
                dest=self._memory_store,
                source_path=version,
            )

    def __bool__(self) -> bool:
        return bool(self._memory_store)

    # Compatability layer for legacy format - mimicking the behaviour of a dictionary
    # Previous format expected a dictionary containing the site code and data
    # as key:value pairs.
    # TODO: May also want to check other expected keys within the legacy
    # dictionary format and add them below
    def __getitem__(self, key: str) -> Any:
        """
        Returns the data attribute (xarray Dataset) when the site name is
        specified.
        Included as a compatability layer for legacy format as a dictionary
        containing a Dataset for each site code.

        key (str): Site code
        """
        site = self.metadata["site"].lower()
        if key.lower() == site:
            return self.data
        else:
            raise KeyError(f"Site '{key}' does not match to expected site '{site}'")

    def __iter__(self) -> Iterator:
        """
        Returns site code as the key for the dictionary as would be expected.
        """
        site = self.metadata["site"]
        return iter([site])

    def __len__(self) -> int:
        """
        Returns number of key values (fixed at 1 at present)
        """
        # Fixed length as 1 at the moment but may need to update if other key
        # values are added.
        return 1

    def __eq__(self, other: object) -> bool:
        raise NotImplementedError
        if not isinstance(other, ObsData):
            return NotImplemented

        return self.data.equals(other.data) and self.metadata == other.metadata

    def data(self, start_date: Optional[str], end_date: Optional[str]) -> xr.Dataset:
        # get the date keys
        # TODO - implement time selection of data
        date_keys = self.metadata["versions"][self._version]["keys"]
        fileset = [xr.open_zarr(store=self._memory_store, group=key) for key in date_keys]
        ds = xr.open_mfdataset()
        # combine them into a single dataset

        return xr.open_zarr(store=self._memory_store, consolidated=False)

    def plot_timeseries(
        self,
        title: Optional[str] = None,
        xlabel: Optional[str] = None,
        ylabel: Optional[str] = None,
        units: Optional[str] = None,
        logo: Optional[bool] = True,
    ) -> go.Figure:
        """Plot a timeseries"""

        return general_plot_timeseries(
            data=self,
            title=title,
            xlabel=xlabel,
            ylabel=ylabel,
            units=units,
            logo=logo,
        )
