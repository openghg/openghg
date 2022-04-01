from typing import List, Optional, Union
from openghg.dataobjects import ObsData
import plotly.graph_objects as go


def plot_timeseries(
    data: Union[ObsData, List[ObsData]],
    title: Optional[str] = None,
    x_var: Optional[str] = None,
    y_var: Optional[str] = None,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    units: Optional[str] = None,
) -> go.Figure:
    """Plot a timeseries

    Args:
        data: ObsData object or list of objects
        title: Title for figure
        x_var: x axis variable, defaults to time
        y_var: y axis variable, defaults to species
        x_label: Label for x axis
        y_label: Label for y axis
        units: Units for y axis
    Returns:
        go.Figure: Plotly Graph Object Figure
    """
    if not isinstance(data, list):
        data = [data]

    font = {"size": 14}
    title_layout = {"text": title, "y": 0.9, "x": 0.5, "xanchor": "center", "yanchor": "top"}

    if title is None:
        title = ""

    if x_label is None:
        x_label = "Date"

    if y_label is None:
        y_label = "Concentration"

    if units is not None:
        y_label += f"  ({units})"

    layout = go.Layout(
        title=title_layout,
        xaxis=dict(title=x_label),
        yaxis=dict(title=y_label),
        font=font,
    )

    # Create a single figure
    fig = go.Figure(layout=layout)

    for to_plot in data:
        metadata = to_plot.metadata
        dataset = to_plot.data

        species = metadata["species"]
        site = metadata["site"]
        inlet = metadata["inlet"]

        legend = f"{site} - {species} - {inlet}"

        if x_var is not None:
            x_data = dataset[x_var]
        else:
            x_data = dataset.time

        if y_var is not None:
            y_data = dataset[y_var]
        else:
            try:
                y_data = dataset[species]
            except KeyError:
                y_data = dataset["mf"]

        fig.add_trace(go.Scatter(name=legend, x=x_data, y=y_data, mode="lines"))

    return fig


# def plot_timeseries(
#     data: Union[ObsData, List[ObsData]], x_var: str, y_var: str, x_label: str, y_label: str
# ) -> None:
#     """Plot timeseries data using Plotly

#     Args:
#         data: One or more ObsData objects
#         x_var: Variable to plot on x axis
#         y_var: Variable to plot on y axis
#         x_label: Label for x axis
#         y_label: Label for y axis
#     Returns:
#         plot type
#     """


#     for dat in data:
