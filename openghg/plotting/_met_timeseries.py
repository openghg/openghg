from typing import List, Optional, Union

import plotly.graph_objects as go
from openghg.dataobjects import METData
from plotly.subplots import make_subplots
from openghg.retrieve.met import _get_site_data, _get_site_pressure
from pandas import Timestamp
import numpy as np


def plot_met_timeseries(
    data: METData,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    title: Optional[str] = None,
    variables: Optional[str] = None,
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
    from openghg.util import load_json

    #if not isinstance(data, list):
    #    data = [data]

    font = {"size": 14}
    title_layout = {"text": title, "y": 0.9, "x": 0.5, "xanchor": "center", "yanchor": "top"}

    if title is None:
        title = "Met Variables"

    if variables is None:
        variables = list(data.data.keys())
    
    site= data.metadata["site"]
    latitude, longitude, site_height, inlet_heights = _get_site_data(site=site, network=data.metadata["network"])
    measure_pressure = _get_site_pressure(inlet_heights=inlet_heights, site_height=site_height)
    
    if start_date is not None or end_date is not None:
        start_date = Timestamp(data.data.time.values[0]) if start_date is None else start_date
        end_date = Timestamp(data.data.time.values[-1]) if start_date is None else end_date
        dataset = data.data.sel(time=slice(Timestamp(start_date), Timestamp(end_date)))
        print(len(dataset.time.values))
    else:
        dataset=data.data
        print(len(dataset.time.values))

    layouts ={}

    fig = make_subplots(rows=len(variables), cols=1, shared_xaxes=True, subplot_titles=variables)

    for nvar, v in enumerate(variables):
        units=data.data[v].attrs["units"]


        layouts[f"yaxis{nvar+1}"] = {"title": units}

        fig.add_trace(go.Scatter(name=data.data[v].attrs["standard_name"], x=dataset.time.values, y=np.squeeze(dataset[v].interp(latitude=latitude, longitude=longitude, level=measure_pressure).values), mode="lines"), row=nvar+1, col=1)
        
    layouts[f"xaxis{nvar+1}"] = {"title": "time"}
    layouts["title"] = f"Met variables for {site}"
    fig.update_layout(layouts)


    return fig


# def plot_timeseries(
#     data: Union[ObsData, List[ObsData]], xvar: str, yvar: str, xlabel: str, ylabel: str
# ) -> None:
#     """Plot timeseries data using Plotly

#     Args:
#         data: One or more ObsData objects
#         xvar: Variable to plot on x axis
#         yvar: Variable to plot on y axis
#         xlabel: Label for x axis
#         ylabel: Label for y axis
#     Returns:
#         plot type
#     """


#     for dat in data:
