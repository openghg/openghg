from openghg.plotting import plot_timeseries as general_plot_timeseries
import plotly.graph_objects as go
from openghg.store.base import LocalZarrStore
import xarray as xr
from typing import Any, Dict, Iterator, Optional

__all__ = ["ObsData"]


class ObsData:
    """This class is used to return observations data. It be created with a preloaded xarray Dataset or
    with a UUID and version number to retrieve data from Datasource zarr store.

    Args:
        metadata: Dictionary of metadata
        data: Dataset if data is already loaded
        uuid: UUID of Datasource to retrieve data from
        version: Version of data requested from Datasrouce
    """

    def __init__(
        self,
        metadata: Dict,
        data: Optional[xr.Dataset] = None,
        uuid: Optional[str] = None,
        version: Optional[str] = None,
    ) -> None:
        if data is None and uuid is None and version is None:
            raise ValueError("Must supply either data or uuid and version")

        self.metadata = metadata
        self._bucket = metadata["object_store"]
        self._data = data
        self._version = version
        self._lazy = False
        self._uuid = uuid

        if uuid is not None and version is not None:
            self._lazy = True
            self._memory_stores = []
            self._zarrstore = LocalZarrStore(bucket=self._bucket, datasource_uuid=uuid, mode="r")

        # We'll use this to open the zarr store as a dataset
        # If the user wants to select data by a daterange then it's easy to just copy the daterange keys that match
        # the dates the user has requested. Nothing is copied from disk until the user requests it.

    def __bool__(self) -> bool:
        return bool(self._zarrstore)

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
        if not isinstance(other, ObsData):
            return NotImplemented

        return self.data.equals(other.data) and self.metadata == other.metadata

    @property
    def data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> xr.Dataset:
        if self._lazy and self._data is None:
            date_keys = self.metadata["versions"][self._version]["keys"]
            self._memory_stores = self._zarrstore.copy_to_stores(keys=date_keys, version=self._version)
            self._data = xr.open_mfdataset(paths=self._memory_stores, engine="zarr", combine="by_coords")

        return self._data

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
