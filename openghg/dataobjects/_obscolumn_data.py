from ._basedata import _BaseData
from openghg.plotting import plot_timeseries as general_plot_timeseries
import plotly.graph_objects as go

__all__ = ["ObsColumnData"]


class ObsColumnData(_BaseData):
    """This class is used to return observations data from the get_obs_column function

    Args:
        data: xarray Dataset
        metadata: Dictionary of metadata including model run parameters
    """

    def __str__(self) -> str:
        return f"Data: {self.data}\nMetadata : {self.metadata}"

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
