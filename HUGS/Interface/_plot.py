from cartopy.feature import BORDERS
import cartopy.crs as ccrs
import matplotlib.cm as cm

import numpy as np
import xarray as xray
from bqplot import pyplot as plt
from bqplot import DateScale, LinearScale, LogScale, Axis, Lines, Figure, Scatter

__all__ = ["scatter_plot", "comparison_plot", "plot_emissions"]



def scatter_plot(data, title=None, x_title=None, y_title=None):
    x_scale = DateScale()
    y_scale = LinearScale()
    scales = {"x": x_scale, "y": y_scale}

    ax = Axis(label="Date", scale=x_scale)
    ay = Axis(label="Count", scale=y_scale, orientation="vertical")

    x_data = data.index
    y_data = data.iloc[:, 0]

    scatter = Scatter(x=x_data, y=y_data, scales=scales)
    return Figure(marks=[scatter], axes=[ax, ay], animation_duration=1000, default_size=24)

def comparison_plot(data, to_compare):
    import random
    import matplotlib.colors as mcolors

    x_scale = DateScale()
    y_scale = LinearScale()
    scales = {"x": x_scale, "y": y_scale}

    ax = Axis(label="Date", scale=x_scale)
    ay = Axis(label="Count", scale=y_scale, orientation="vertical")

    plots = []
    for d in to_compare:
        color = [random.choice(list(mcolors.TABLEAU_COLORS.values()))]
        species_data = data[d]
        x_data = species_data.index
        y_data = species_data.iloc[:,0]
        plot = Scatter(x=x_data, y=y_data, scales=scales,
                       colors=color,  default_size=24)
        plots.append(plot)

    return Figure(marks=plots, axes=[ax, ay], animation_duration=1000, display_legend=True)


"""
    Note this function is just a placeholder to be coupled to a function returning the NetCDF from the
    ObjectStore

    Currently it just reads the fixed filepath
"""

def plot_emissions(file):
    import matplotlib.pyplot as plt

    ds = xray.open_dataset(
        "/home/home/gar/Documents/Devel/RSE/hugs/emissions_data/WAO-20magl_EUROPE_201511.nc")

    domain = "EUROPE"

    fig = plt.figure()
    ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

    ax.coastlines(color="0.2")
    ax.add_feature(BORDERS, edgecolor="0.5")

    fp_name = "fp"
    lon_name = "lon"
    lat_name = "lat"

    cmap = cm.get_cmap("inferno")
    levels = np.linspace(np.percentile(ds[fp_name].values, 5), np.percentile(ds[fp_name].values, 95), 20)

    long_values = ds[fp_name][lon_name].values
    lat_values = ds[fp_name][lat_name].values
    zero_values = ds[fp_name][:, :, 0].values

    return ax.contourf(long_values, lat_values, zero_values,cmap=cm.get_cmap("inferno"), levels=levels)


    # co_x_data = co_data.index
    # co_y_data = co_data.iloc[:, 0]

    # co2_x_data = co2_data.index
    # co2_y_data = co2_data.iloc[:, 0]

    # co_scatter = Scatter(x=co_x_data, y=co_y_data, scales=scales, colors=["LightGreen"])
    # co2_scatter = Scatter(x=co2_x_data, y=co2_y_data, scales=scales, colors=["steelblue"])

    # figure

