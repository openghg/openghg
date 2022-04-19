# I'm creating this submodule as I'm not quite sure where else to put this for now
# we can always move it in the future
from typing import Dict, List, Optional, Union
from openghg.dataobjects import ObsData


def retrieve_icos(
    site: str,
    species: Union[str, List],
    sampling_height: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> ObsData:
    """Retrieve ICOS data from the ICOS Carbon Portal

    Args:
        site: ICOS site code, for site codes see
        https://www.icos-cp.eu/observations/atmosphere/stations
        sampling_height: Sampling height in metres
    Returns:
        ObsData: ObsData object
    """
    # Check if the site passed is valid?
    from icoscp.station import station
    from icoscp.cpb.dobj import Dobj
    import re

    # terms = ["CO", "CO2", "CH4"]

    if not isinstance(species, list):
        species = [species]

    # We should first check if it's stored in the object store
    # Will need to make sure ObsSurface can accept the datasets we
    # create from the ICOS data
    stat = station.get(stationId=site)
    data_pids = stat.data(level=2)

    # We want to get the PIDs of the data for each species here
    species_upper = [s.upper() for s in species]
    # For this see https://stackoverflow.com/a/55335207
    search_str = r"\b(?:{})\b".format("|".join(map(re.escape, species_upper)))
    # Now filter the dataframe so we can extraxt the PIDS
    filtered_sources = data_pids[data_pids["specLabel"].str.contains(search_str)]

    if sampling_height is not None:
        sampling_height = str(float(sampling_height.rstrip("m")))
        filtered_sources = filtered_sources[
            [sampling_height in x for x in filtered_sources["samplingheight"]]
        ]

    # Now extract the PIDs along with some data about them
    dobj_urls = filtered_sources["dobj"].tolist()

    dobjs = []
    for url in dobj_urls:
        dobj = Dobj(url)
        metadata = extract_metadata(meta=dobj.info)
        dataframe = dobj.data

        # Now we want to tidy this and convert it into a format we expect


def extract_metadata(meta: List) -> Dict:
    """Extract metadata from the list of pandas DataFrames that are
    returned by the ICOS-CP pylib Dobj method.

    Args:
        metadataframes: List of dataframes containing metadata
    Returns:
        dict: Dictionary of metadata
    """
    # From the ICOS documentation
    # https://icos-carbon-portal.github.io/pylib/modules/#dobjinfo
    # info[0] -> information about the dobj like, url, specification, number of rows, related file name.
    # info[1] -> information about the data like colName, value type, unit, kind
    # info[2] -> information about the station, where the data was obtained. Name, id, lat, lon etc..
    spec_data = meta[0]
    measurement_data = meta[1]
    site_data = meta[2]

    metadata = {}

    metadata["dobj_pid"] = spec_data["dobj"][0]

    metadata["filename"] = spec_data["fileName"][0]

    metadata["species"] = measurement_data["colName"][4]
    metadata["meas_type"] = measurement_data["valueType"][4]
    metadata["units"] = measurement_data["unit"][4]

    metadata["site"] = site_data["stationName"][0]
    metadata["stationId"] = site_data["stationId"][0]
    metadata["sampling_height"] = site_data["samplingHeight"][0]
    metadata["latitude"] = site_data["latitude"][0]
    metadata["longitude"] = site_data["longitude"][0]
    metadata["elevation"] = site_data["elevation"][0]

    return metadata
