from typing import List, Optional, Union
from openghg.dataobjects import ObsData
import plotly.graph_objects as go


def plot_timeseries(
    data: Union[ObsData, List[ObsData]],
    title: Optional[str] = None,
    xvar: Optional[str] = None,
    yvar: Optional[str] = None,
    xlabel: Optional[str] = None,
    ylabel: Optional[str] = None,
    units: Optional[str] = None,
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

    if not isinstance(data, list):
        data = [data]

    font = {"size": 14}
    title_layout = {"text": title, "y": 0.9, "x": 0.5, "xanchor": "center", "yanchor": "top"}

    if title is None:
        title = ""

    if xlabel is None:
        xlabel = "Date"

    if ylabel is None:
        ylabel = "Concentration"

    if units is not None:
        ylabel += f"  ({units})"

    layout = go.Layout(
        title=title_layout,
        xaxis=dict(title=xlabel),
        yaxis=dict(title=ylabel),
        font=font,
    )

    # Create a single figure
    fig = go.Figure(layout=layout)

    for i, to_plot in enumerate(data):
        metadata = to_plot.metadata
        dataset = to_plot.data

        species = metadata["species"]
        site = metadata["site"]
        inlet = metadata["inlet"]

        legend = f"{site} - {species} - {inlet}"

        if xvar is not None:
            x_data = dataset[xvar]
        else:
            x_data = dataset.time

        if yvar is not None:
            y_data = dataset[yvar]
        else:
            try:
                y_data = dataset[species]
            except KeyError:
                y_data = dataset["mf"]

        if units is not None or len(data) > 1:

            data_attrs = y_data.attrs
            data_units = data_attrs.get("units", "1")

            if i == 0:
                if units:
                    attributes_data = load_json("attributes.json")
                    unit_interpret = attributes_data["unit_interpret"]
                    unit_value = unit_interpret.get(units, "1")
                else:
                    unit_value = data_units

            unit_conversion = float(data_units) / float(unit_value)
        else:
            unit_conversion = 1

        y_data *= unit_conversion

        fig.add_trace(go.Scatter(name=legend, x=x_data, y=y_data, mode="lines"))

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
