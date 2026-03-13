from ._basedata import _BaseData
from openghg.plotting import plot_timeseries as general_plot_timeseries
import plotly.graph_objects as go
from typing import Any
from collections.abc import Iterator


class ObsData(_BaseData):
    """This class is used to return observations data. It be created with a preloaded xarray Dataset or
    with a UUID and version number to retrieve data from Datasource zarr store.
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

        if self.data is None or other.data is None:
            raise ValueError("Cannot compare data if it is not loaded")

        return self.data.equals(other.data) and self.metadata == other.metadata

    def plot_timeseries(
        self,
        title: str | None = None,
        xlabel: str | None = None,
        ylabel: str | None = None,
        units: str | None = None,
        logo: bool | None = True,
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
