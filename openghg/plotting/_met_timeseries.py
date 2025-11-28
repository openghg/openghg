import plotly.graph_objects as go
from openghg.dataobjects import METData
from plotly.subplots import make_subplots
from pandas import Timestamp
import numpy as np
import plotly.express as px

from openghg.util import _get_site_data, format_inlet


# from openghg.retrieve.met import get_site_pressure


def plot_met_timeseries(
    data: METData,
    start_date: str | None = None,
    end_date: str | None = None,
    title: str | None = None,
    variables: list[str] | None = None,
    inlet_height: str | None = None,
    subplot_height: int = 100,
) -> go.Figure:
    """Plot timeseries of meteorology for a particular site. METData object must be produced using the corresponding search function search_site_met(site="TAC")

    Args:
        data: ObsData object or list of objects
        start_date and end_date: as strings
        title: Title for the plot
        variables: List of variable names to plot (if None all variables in data will be plotted)
        inlet_height: Inlet height to plot. If None, the extracted pressure levels are plotted. If "all", all inlets are plotted, interpolated linearly. If a specific height is given, the data is interpolated linearly to this height (eg. "10magl").
        subplot_height: height of each subplot. default is 100
    Returns:
        go.Figure: Plotly Graph Object Figure
    """

    # if not isinstance(data, list):
    #    data = [data]

    if title is None:
        title = "Met Variables"

    if variables is None:
        variables = list(data.data.data_vars)
    else:
        for v in variables:
            if v not in list(data.data.keys()):
                raise ValueError(
                    f"{v} is not one of the variables in the data, please select one of {list(data.data.keys())} or fetch more data"
                )
    site = data.metadata["site"]
    latitude, longitude, _, inlet_heights = _get_site_data(site=site, network=data.metadata["network"])

    # measure_pressure = get_site_pressure(inlet_heights=inlet_heights, site_height=site_height)

    if start_date is not None or end_date is not None:
        start_date = Timestamp(data.data.time.values[0]) if start_date is None else start_date
        end_date = Timestamp(data.data.time.values[-1]) if start_date is None else end_date
        dataset = data.data.sel(time=slice(Timestamp(start_date), Timestamp(end_date)))
        if len(dataset.time) == 0:
            raise ValueError("The dates that you passed are not contained in this met data!")
    else:
        dataset = data.data

    layouts: dict[str, str | int | dict] = {}

    fig = make_subplots(rows=len(variables), cols=1, shared_xaxes=True, subplot_titles=variables)

    legend = True

    if inlet_height not in [None, "all"]:
        inlet_height = format_inlet(inlet_height)

    for nvar, v in enumerate(variables):
        units = data.data[v].attrs["units"]

        layouts[f"yaxis{nvar + 1}"] = {"title": units}

        # format the levels to plot (whether as extracted or interpolated) and their labels
        if inlet_height is None:
            # plot the met for all pressure levels, as present in the data
            pressure_level = sorted(dataset.pressure_level.values)

            names = [f"{pl} hPa" for pl in pressure_level]

        elif inlet_height == "all":
            # plot the met for all inlets, interpolated linearly to the corresponding pressure
            pressure_level = dataset.inlet_pressure.values
            names = [hg for hg in dataset.inlet_height.values]
            names = [x for _, x in sorted(zip(pressure_level, names), reverse=True)]
            pressure_level = sorted(pressure_level)

        elif inlet_height in dataset.inlet_height.values:
            # plot the met for the specific inlet height, interpolated linearly to the corresponding pressure
            pressure_level = [float(dataset.inlet_pressure.sel(inlet_height=inlet_height).values)]
            names = [inlet_height]

        else:
            raise ValueError(
                f"The inlet_height {inlet_height} is not valid. Please pass None, 'all' or one of {dataset.inlet_height.values}"
            )

        pressure_level = [round(x, 2) for x in pressure_level]
        data_to_plot = dataset[v].interp(lat=latitude, lon=longitude, pressure_level=pressure_level)

        colors = px.colors.qualitative.Plotly

        # plot each pressure line
        for i, pl in enumerate(pressure_level):
            fig.add_trace(
                go.Scatter(
                    name=names[i],
                    x=data_to_plot.time.values,
                    y=np.squeeze(data_to_plot.sel(pressure_level=pl).values),
                    mode="lines",
                    line=dict(color=colors[i]),
                    showlegend=legend,
                ),
                row=nvar + 1,
                col=1,
            )

        legend = False

    # format the subplot
    layouts[f"xaxis{nvar + 1}"] = {"title": "time"}
    layouts["title"] = {"text": f"Met variables for {site.upper()}", "font": {"size": 24}}
    total_height = subplot_height * max(1, len(variables))
    layouts["height"] = total_height
    fig.update_layout(layouts)

    return fig
