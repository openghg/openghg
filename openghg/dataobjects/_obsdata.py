from dataclasses import dataclass
from collections import abc
from typing import Any, Iterator
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

    def plot_timeseries(
        self, title: str = None, xlabel: str = None, ylabel: str = None, units: str = None
    ) -> go.Figure:
        """Plot a timeseries"""

        species = self.metadata["species"]
        site = self.metadata["site"]
        inlet = self.metadata["inlet"]

        if title is None:
            title = f"{species.upper()} at {site.upper()} - {inlet}"

        if xlabel is None:
            xlabel = "Date"

        if ylabel is None:
            ylabel = "Concentration"

        if units is not None:
            ylabel += f"  ({units})"

        data = self.data
        x_data = data.time

        try:
            y_data = data[species]
        except KeyError:
            y_data = data["mf"]

        font = {"size": 14}

        title_layout = {"text": title, "y": 0.9, "x": 0.5, "xanchor": "center", "yanchor": "top"}

        layout = go.Layout(
            title=title_layout,
            xaxis=dict(title=xlabel),
            yaxis=dict(title=ylabel),
            font=font,
        )

        # Create traces
        fig = go.Figure(layout=layout)
        fig.add_trace(go.Scatter(x=x_data, y=y_data, mode="lines", name=species.upper()))

        return fig
