from typing import List, Optional, Union
import plotly.graph_objects as go
import numpy as np
import base64

from openghg.dataobjects import ObsData
from openghg.util import get_species_info, synonyms, get_datapath


def _latex2html(
    latex_string: str
) -> str:
    """Replace latex sub/superscript formatting with html.
    Written because the latex formatting in Plotly seems inconsistent
    (works in Notebooks, but not VC Code at the moment).

    Args:
        latex_string: 
    """

    replacements = {"$^2$": "<sup>2</sup>",
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
        html_string = html_string.replace(rep,
                                        replacements[rep])

    return html_string


def plot_timeseries(
    data: Union[ObsData, List[ObsData]],
    xvar: Optional[str] = None,
    yvar: Optional[str] = None,
    xlabel: Optional[str] = None,
    ylabel: Optional[str] = None,
    units: Optional[str] = None,
    logo: Optional[bool] = True,
) -> go.Figure:
    """Plot a timeseries

    Args:
        data: ObsData object or list of objects
        xvar: x axis variable, defaults to time
        yvar: y axis variable, defaults to species
        xlabel: Label for x axis
        ylabel: Label for y axis
        units: Units for y axis
        logo: Show the OpenGHG logo
    Returns:
        go.Figure: Plotly Graph Object Figure
    """
    from openghg.util import load_internal_json

    if not data:
        print("No data to plot, returning")
        return None

    if not isinstance(data, list):
        data = [data]

    # Get species info
    species_info = get_species_info()

    # Get some general attributes
    attributes_data = load_internal_json("attributes.json")

    font = {"size": 14}

    margin = {
        "l": 20,
        "r": 20,
        "t": 20,
        "b": 20
    }

    layout = go.Layout(
        font=font,
        margin=margin
    )

    # Create a single figure
    fig = go.Figure(layout=layout)

    species_strings = []
    unit_strings = []
    ascending = []

    # Loop through inlets/species
    for i, to_plot in enumerate(data):
        metadata = to_plot.metadata
        dataset = to_plot.data

        species = metadata["species"]
        site = metadata["site"]
        inlet = metadata["inlet"]

        species_string = _latex2html(
            species_info[synonyms(species, lower=False)]["print_string"]
        )

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

        if units is not None or len(data) > 1:

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

        y_data *= unit_conversion

        unit_string = attributes_data["unit_print"][unit_value]

        # Determine whether data is ascending or descending (positioning of legend)
        ny = len(y_data)
        data_left = y_data.isel(time=slice(int(ny * 0.9), -1)).mean()
        data_right = y_data.isel(time=slice(0, int(ny * 0.1))).mean()
        if data_left > data_right:
            ascending.append(True)
        else:
            ascending.append(False)

        # Insert NaNs where there are large data gaps (removes connecting lines)
        gap_idx = np.where(np.diff(x_data.values.astype(int)) > 24 * 60 * 60 * 1e9)[0]
        x_data_plot = np.insert(x_data.values, gap_idx + 1, values=x_data.time[0])
        y_data_plot = np.insert(y_data.values, gap_idx + 1, values=np.nan)

        # Create plot
        fig.add_trace(
            go.Scatter(
                name=legend_text,
                x=x_data_plot,
                y=y_data_plot,
                mode="lines",
            )
        )

        # Save units and species names for axis labels
        unit_strings.append(_latex2html(unit_string))
        species_strings.append(species_string)

    if len(set(unit_strings)) > 1:
        raise NotImplementedError("Can't plot two different units yet")

    # Write species and units on y-axis
    if ylabel:
        fig.update_yaxes(title=ylabel)
    else:
        ytitle = ", ".join(set(species_strings)) + " (" + unit_strings[0] + ")"
        fig.update_yaxes(title=ytitle)

    if xlabel:
        fig.update_xaxes(title=xlabel)

    # If any timeseries is ascending, put the legend in the top-left.
    # Otherwise, put in the top-right
    if True in set(ascending):
        legend = {
            "yanchor": "top",
            "xanchor": "left",
            "y": 0.99,
            "x": 0.01
        }
        logo_pos = {
            "yanchor": "bottom",
            "xanchor": "right",
            "y": 0.01,
            "x": 0.99
        }
    else:
        legend = {
            "yanchor": "top",
            "xanchor": "right",
            "y": 0.99,
            "x": 0.99
        }
        logo_pos = {
            "yanchor": "bottom",
            "xanchor": "left",
            "y": 0.01,
            "x": 0.01
        }
    fig.update_layout(legend=legend,
                      template="seaborn")

    # Add OpenGHG logo
    logo = base64.b64encode(open(get_datapath("OpenGHG_Logo_NoText_transparent_200x200.png"), 'rb').read())
    logo_dict = dict(
        source='data:image/png;base64,{}'.format(logo.decode()),
        xref="x domain",
        yref="y domain",
        sizex=0.1,
        sizey=0.1)
    logo_dict.update(logo_pos)

    fig.add_layout_image(logo_dict
        # dict(
        #     source='data:image/png;base64,{}'.format(logo.decode()),
        #     xref="x domain",
        #     yref="y domain",
        #     sizex=0.1,
        #     sizey=0.1,
        #     x=0.99, y=0.01,
        #     xanchor="right", yanchor="bottom"
        # )  # .update(logo_pos)
                         )

    return fig
