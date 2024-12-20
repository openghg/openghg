from __future__ import annotations
import logging
import plotly.graph_objects as go
import numpy as np
import base64
from typing import TYPE_CHECKING

from openghg.util import get_species_info, synonyms, get_datapath

if TYPE_CHECKING:
    from openghg.dataobjects import ObsData


logger = logging.getLogger("openghg.plotting")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def _latex2html(latex_string: str) -> str:
    """Replace latex sub/superscript formatting with html.
    Written because the latex formatting in Plotly seems inconsistent
    (works in Notebooks, but not VSCode at the moment).

    Args:
        latex_string: String containing LaTeX math mode (including $$)
    Returns:
        str: string with matched sub-strings replaced with equivalent html.
    """

    replacements = {
        "$^2$": "<sup>2</sup>",
        "$^{-1}$": "<sup>-1</sup>",
        "$^{-2}$": "<sup>-2</sup>",
        "$_2$": "<sub>2</sub>",
        "$_3$": "<sub>3</sub>",
        "$_4$": "<sub>4</sub>",
        "$_5$": "<sub>5</sub>",
        "$_6$": "<sub>6</sub>",
    }

    html_string = latex_string
    for rep in replacements:
        html_string = html_string.replace(rep, replacements[rep])

    return html_string


def _plot_remove_gaps(
    x_data: np.ndarray, y_data: np.ndarray, gap: int | None = None
) -> tuple[np.ndarray, np.ndarray]:
    """Insert NaNs between big gaps in the data.
    Prevents connecting lines being drawn

    Args:
        x_data: plot timeseries (numpy timestamp)
        y_data: data array
        gap: gap beyond which a NaN is introducted (nanoseconds, defaults to 1 day)
    Returns:
        x, y: x and y arrays to plot
    """
    if gap is None:
        # ns in a day
        gap = 24 * 60 * 60 * 1000000000

    gap_idx = np.where(np.diff(x_data.astype(int)) > gap)[0]
    x_data_plot = np.insert(x_data, gap_idx + 1, values=x_data[0])
    y_data_plot = np.insert(y_data, gap_idx + 1, values=np.nan)

    return x_data_plot, y_data_plot


def _plot_legend_position(ascending: bool) -> tuple[dict, dict]:
    """Position of legend and logo,
    depending on whether data is ascending or descending

    Args:
        ascending: Is the data ascending
    Returns:
        Dict, Dict: Plotly legend and logo position parameters
    """
    if ascending:
        legend_pos = {"yanchor": "top", "xanchor": "left", "y": 0.99, "x": 0.01}
        logo_pos = {"yanchor": "bottom", "xanchor": "right", "y": 0.01, "x": 0.99}
    else:
        legend_pos = {"yanchor": "top", "xanchor": "right", "y": 0.99, "x": 0.99}
        logo_pos = {"yanchor": "bottom", "xanchor": "left", "y": 0.01, "x": 0.01}

    return legend_pos, logo_pos


def _plot_logo(
    logo_pos: dict,
) -> dict:
    """Create Plotly dictionary for logo

    Args:
        logo_pos: Dictionary containing the position of the logo
    Returns:
        dict: Dictionary containing logo + position parameters
    """
    logo_bytes = get_datapath("OpenGHG_Logo_NoText_transparent_200x200.png").read_bytes()
    logo = base64.b64encode(logo_bytes)

    logo_dict = dict(
        source=f"data:image/png;base64,{logo.decode()}",
        xref="x domain",
        yref="y domain",
        sizex=0.1,
        sizey=0.1,
    )
    logo_dict.update(logo_pos)

    return logo_dict


def plot_timeseries(
    data: ObsData | list[ObsData],
    xvar: str | None = None,
    yvar: str | None = None,
    title: str | None = None,
    xlabel: str | None = None,
    ylabel: str | None = None,
    units: str | None = None,
    logo: bool | None = True,
) -> go.Figure:
    """Plot a timeseries

    Args:
        data: ObsData object or list of objects
        xvar: x axis variable, defaults to time
        yvar: y axis variable, defaults to species
        title: Title for figure
        xlabel: Label for x axis
        ylabel: Label for y axis
        units: Units for y axis
        logo: Show the OpenGHG logo
    Returns:
        go.Figure: Plotly Graph Object Figure
    """
    from openghg.util import load_internal_json

    if not data:
        logger.warning("No data to plot, returning")
        return None

    if not isinstance(data, list):
        data = [data]

    # Get species info
    species_info = get_species_info()

    # Get some general attributes
    attributes_data = load_internal_json("attributes.json")

    font = {"size": 14}

    margin = {"l": 20, "r": 20, "t": 20, "b": 20}

    if title is not None:
        title_layout = {"text": title, "y": 0.9, "x": 0.5, "xanchor": "center", "yanchor": "top"}
        layout = go.Layout(
            title=title_layout, xaxis=dict(title=xlabel), yaxis=dict(title=ylabel), font=font, margin=margin
        )
    else:
        layout = go.Layout(font=font, margin=margin)

    # Create a single figure
    fig = go.Figure(layout=layout)

    species_strings = []
    unit_strings = []

    # Loop through inlets/species
    for i, to_plot in enumerate(data):
        metadata = to_plot.metadata
        dataset = to_plot.data

        species = metadata["species"]
        site = metadata["site"]
        inlet = metadata["inlet"]

        species_string = _latex2html(species_info[synonyms(species, lower=False)]["print_string"])

        legend_text = f"{species_string} - {site.upper()} ({inlet})"

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

        if units is not None or len(data) > 0:
            data_attrs = y_data.attrs
            data_units = data_attrs.get("units", "1")

            if i == 0:
                if units:
                    unit_interpret = attributes_data["unit_interpret"]
                    unit_value = unit_interpret.get(units, "1")
                else:
                    unit_value = data_units

            unit_conversion = float(data_units) / float(unit_value)
        else:
            unit_conversion = 1
            # TODO: Not sure what is expected for unit_value here
            unit_value = "1"

        y_data *= unit_conversion

        unit_string = attributes_data["unit_print"][unit_value]

        # Add NaNs where there are large data gaps
        x_data_plot, y_data_plot = _plot_remove_gaps(x_data.values, y_data.values)

        # Convert unit string to html
        unit_string_html = _latex2html(unit_string)

        # Create plot
        fig.add_trace(
            go.Scatter(
                name=legend_text,
                x=x_data_plot,
                y=y_data_plot,
                mode="lines",
                hovertemplate="%{x|%Y-%m-%d %H:%M}<br> %{y:.1f} " + unit_string_html,
            )
        )

        # Save units and species names for axis labels
        unit_strings.append(unit_string_html)
        species_strings.append(species_string)

    # Determine whether data is ascending or descending (positioning of legend)
    y_data_diff = y_data.diff(dim="time").mean()
    if y_data_diff >= 0:
        ascending = True
    else:
        ascending = False

    if len(set(unit_strings)) > 1:
        raise NotImplementedError("Can't plot two different units yet")

    # Write species and units on y-axis
    if ylabel is not None:
        fig.update_yaxes(title=ylabel)
    else:
        ytitle = ", ".join(set(species_strings)) + " (" + unit_strings[0] + ")"
        fig.update_yaxes(title=ytitle)

    if xlabel is None:
        xlabel = "Date"

    fig.update_xaxes(title=xlabel)

    # Position the legend
    legend_pos, logo_pos = _plot_legend_position(ascending)
    fig.update_layout(legend=legend_pos, template="seaborn")

    # Add OpenGHG logo
    if logo:
        logo_dict = _plot_logo(logo_pos)
        fig.add_layout_image(logo_dict)

    return fig
