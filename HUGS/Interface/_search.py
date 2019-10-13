__all__ = ["get_map_locations", "search_emissions", "get_emissions_data"]

def search_emissions(search_terms):
    return ["WAO-20magl_EUROPE_201501", "WAO-20magl_EUROPE_201502", "WAO-20magl_EUROPE_201503", "WAO-20magl_EUROPE_201504"]

def get_emissions_data(search_terms):
    return []

def get_locations(search_results):
    """ Returns the lat:long coordinates of the sites in the search results

        Returns:
            dict: Dictionary of site: lat,long
    """
    locations = {}
    locations["bsd"] = {"location":(54.942544, -1.369204), "name":"Bilsdale"}
    locations["mhd"] = {"location":(53.20, -9.54), "name":"Macehead"}
    locations["tac"] = {"location":(52.511, 1.155003), "name":"Tacolneston"}
    locations["hfd"] = {"location": (50.967, 0.257), "name": "Heathfield"}

    results = {}
    for res in search_results:
        loc_species = res.split("_")
        loc = loc_species[0]
        species = loc_species[1]
        # If we've already seen this location, append gas name ?
        results[loc] = locations[loc]

    return results

def get_map_locations(search_results):
    from ipyleaflet import (
        Map,
        Marker, MarkerCluster, TileLayer, ImageOverlay, GeoJSON,
        Polyline, Polygon, Rectangle, Circle, CircleMarker, Popup,
        SplitMapControl, WidgetControl,
        basemaps, basemap_to_tiles
    )
    from ipywidgets import HTML

    center=[54.2361, -4.548]
    zoom=5
    m = Map(center=center, zoom=zoom)

    map_locations = get_locations(search_results)

    for l in map_locations:
        lat,long = map_locations[l]["location"]
        name = map_locations[l]["name"]
        mark = Marker(location=(lat, long))
        mark.popup = HTML(value=name)
        
        m += mark
    
    return m








