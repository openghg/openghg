from typing import Optional, List

import plotly.graph_objects as go
from openghg.dataobjects import METData
from plotly.subplots import make_subplots
from pandas import Timestamp
import numpy as np

from openghg.util import get_site_location


# from openghg.retrieve.met import get_site_pressure


def plot_met_timeseries(
    data: METData,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    title: Optional[str] = None,
    variables: Optional[List[str]] = None,
) -> go.Figure:
    """Plot a timeseries

    Args:
        data: ObsData object or list of objects
        title: Title for figure
        xvar: x axis variable, defaults to time
        yvar: y axis variable, defaults to species
        xlabel: Label for x axis
        ylabel: Label for y axis
        units: Units for y axis
    Returns:
        go.Figure: Plotly Graph Object Figure
    """

    # if not isinstance(data, list):
    #    data = [data]

    if title is None:
        title = "Met Variables"

    if variables is None:
        variables = list(data.data.keys())
    else:
        for v in variables:
            if v not in list(data.data.keys()):
                raise ValueError(
                    f"{v} is not one of the variables in the data, please select one of {list(data.data.keys())} or fetch more data"
                )
    site = data.metadata["site"]
    site_info = get_site_location(site=site, network=data.metadata["network"])
    # measure_pressure = get_site_pressure(inlet_heights=inlet_heights, site_height=site_height)

    if start_date is not None or end_date is not None:
        start_date = Timestamp(data.data.time.values[0]) if start_date is None else start_date
        end_date = Timestamp(data.data.time.values[-1]) if start_date is None else end_date
        dataset = data.data.sel(time=slice(Timestamp(start_date), Timestamp(end_date)))
        if len(dataset.time) == 0:
            raise ValueError("The dates that you passed are not contained in this met data!")
    else:
        dataset = data.data

    layouts = {}

    fig = make_subplots(rows=len(variables), cols=1, shared_xaxes=True, subplot_titles=variables)

    for nvar, v in enumerate(variables):
        units = data.data[v].attrs["units"]

        layouts[f"yaxis{nvar+1}"] = {"title": units}

        fig.add_trace(
            go.Scatter(
                name=data.data[v].attrs["standard_name"],
                x=dataset.time.values,
                y=np.squeeze(
                    dataset[v]
                    .interp(
                        latitude=site_info["latitude"],
                        longitude=site_info["longitude"],
                        level=dataset[v].level.values[0],
                    )
                    .values
                ),
                mode="lines",
            ),
            row=nvar + 1,
            col=1,
        )

    layouts[f"xaxis{nvar+1}"] = {"title": "time"}
    layouts["title"] = {"text": f"Met variables for {site}", "font": {"size": 24}}
    fig.update_layout(layouts)

    return fig
