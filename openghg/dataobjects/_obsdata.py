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
        data: Dictionary of xarray Dataframes
        metadata: Dictionary of metadata
    """

    def __init__(self, uuid: str, version: str, metadata: Dict, compute: bool = False) -> None:
        self._bucket = metadata["object_store"]
        self._uuid = uuid
        self._compute = compute
        self.version = version
        self.metadata = metadata
        self.data = False
        self._memory_store = {}

        # Retrieve the data from the memory store
        # lazy_zarr_store = open_zarr_store(bucket=self._bucket, datasource_uuid=self._uuid)
        self._lazy_zarr_store = LocalZarrStore(bucket=self._bucket, datasource_uuid=uuid, mode="r")
        # Copy the data we want to the memory store
        zarr.convenience.copy_store(
            source=self._lazy_zarr_store,
            dest=self._memory_store,
            source_path=version,
            dest_path=version,
            if_exists="replace",
        )

        if compute:
            self.compute()

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

    def to_data(self) -> Dict:
        """Creates a dictionary package of this ObsData's metadata and data.

        Returns:
            dict: Dictionary of metadata and bytes
        """
        raise NotImplementedError
        to_transfer: Dict[str, Union[str, bytes]] = {}
        to_transfer["metadata"] = dumps(self.metadata)

        # TODO - get better bytes
        with NamedTemporaryFile() as tmpfile:
            self.data.to_netcdf(tmpfile.name)
            to_transfer["data"] = Path(tmpfile.name).read_bytes()

        return to_transfer

    def compute(self) -> None:
        self.data = xr.open_zarr(store=self._memory_store, group=self.version)

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
