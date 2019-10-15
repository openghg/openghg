import matplotlib.cm as cm
import numpy as np
import xarray as xray
from bqplot import pyplot as plt
from bqplot import DateScale, LinearScale, LogScale, Axis, Lines, Figure, Scatter

__all__ = ["scatter_plot", "comparison_plot", "plot_emissions", "get_map_locations"]


def get_map_locations(search_results):
    from ipyleaflet import (
        Map,
        Marker, MarkerCluster, TileLayer, ImageOverlay, GeoJSON,
        Polyline, Polygon, Rectangle, Circle, CircleMarker, Popup,
        SplitMapControl, WidgetControl,
        basemaps, basemap_to_tiles
    )
    from ipywidgets import HTML

    center = [54.2361, -4.548]
    zoom = 5
    m = Map(center=center, zoom=zoom)

    map_locations = get_locations(search_results)

    for l in map_locations:
        lat, long = map_locations[l]["location"]
        name = map_locations[l]["name"]
        species = ",".join(map_locations[l]["species"])
        mark = Marker(location=(lat, long))
        mark.popup = HTML(value="<br/>".join([("<b>"+ name + "</b>"), "Species: ", species.upper()]))

        m += mark

    return m


def get_locations(search_results):
    """ Returns the lat:long coordinates of the sites in the search results

        Returns:
            dict: Dictionary of site: lat,long
    """
    locations = {}
    locations["bsd"] = {"location": (54.942544, -1.369204), "name": "Bilsdale"}
    locations["mhd"] = {"location": (53.20, -9.54), "name": "Macehead"}
    locations["tac"] = {"location": (52.511, 1.155003), "name": "Tacolneston"}
    locations["hfd"] = {"location": (50.967, 0.257), "name": "Heathfield"}
    locations["rpb"] = {"location": (50.967, 0.257), "name": "Barbados"}
    locations["tta"] = {"location": (50.967, 0.257), "name": "Angus"}
    locations["rgl"] = {"location": (50.967, 0.257), "name": "Ridgehill"}

    results = {}
    for res in search_results:
        loc_species = res.split("_")
        loc = loc_species[0]
        species = loc_species[1]

        if loc in results:
            results[loc]["species"].append(species)
        else:
            results[loc] = locations[loc]
            results[loc]["species"] = [species]

        # If we've already seen this location, append gas name ?

    return results

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
        plot = Scatter(x=x_data, y=y_data, scales=scales, colors=color,  default_size=24, labels=[str(d).upper()], display_legend=True)
        plots.append(plot)

    return Figure(marks=plots, axes=[ax, ay], animation_duration=1000)


"""
    Note this function is just a placeholder to be coupled to a function returning the NetCDF from the
    ObjectStore

    Currently it just reads the fixed filepath
"""

def plot_emissions(file):
    import matplotlib.pyplot as plt
    from cartopy.feature import BORDERS
    import cartopy.crs as ccrs
    
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

