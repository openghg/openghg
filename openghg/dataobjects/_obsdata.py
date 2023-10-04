from collections import abc
from dataclasses import dataclass
from json import dumps
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterator, Union, Optional
from openghg.plotting import plot_timeseries as general_plot_timeseries
import plotly.graph_objects as go

from ._basedata import _BaseData

__all__ = ["ObsData"]


@dataclass(frozen=True)
class ObsData(_BaseData, abc.Mapping):
    """This class is used to return observations data from the get_observations function

    Args:
        data: Dictionary of xarray Dataframes
        metadata: Dictionary of metadata
    """

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

    def to_data(self) -> Dict:
        """Creates a dictionary package of this ObsData's metadata and data.

        Returns:
            dict: Dictionary of metadata and bytes
        """
        to_transfer: Dict[str, Union[str, bytes]] = {}
        to_transfer["metadata"] = dumps(self.metadata)

        # TODO - get better bytes
        with NamedTemporaryFile() as tmpfile:
            self.data.to_netcdf(tmpfile.name)
            to_transfer["data"] = Path(tmpfile.name).read_bytes()

        return to_transfer

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
