# I'm creating this submodule as I'm not quite sure where else to put this for now
# we can always move it in the future
from pandas import DataFrame
from typing import Dict, List, Optional, Union

from openghg.dataobjects import ObsData


def retrieve(
    site: str, species: Union[str, List], start_date: str, end_date: str
) -> Union[ObsData, List[ObsData]]:
    """Retrieve ICOS data. If data is found in the object store it is returned. Otherwise
    data will be retrieved from the ICOS Carbon Portal. This may take more time.

    Args:
        site: Site code
        species: Species name
        start_date: Start date
        end_date: End date
    Returns:
        ObsData or list
    """
    from openghg.retrieve import search
    from openghg.store import ObsSurface
    from openghg.dataobjects import ObsData

    results = search(site=site, species=species, network="ICOS")

    if results is not None:
        # TODO - if date is later than the data we have force a retrieval
        raise NotImplementedError
        return results
    else:
        # We'll also need to check we have current data
        standardised_data = _retrieve_remote(site=site, species=species)

        # How to best handle this? Static method seems ?
        obs = ObsSurface.load()
        obs.store_data(data=standardised_data)
        obs.save()

        # Create the expected ObsData type
        obs_data = []
        for data in standardised_data.values():
            measurement_data = data["data"]
            metadata = data["metadata"]
            obs_data.append(ObsData(data=measurement_data, metadata=metadata))

        if len(obs_data) == 1:
            return obs_data[0]
        else:
            return obs_data


def _retrieve_remote(
    site: str,
    species: Union[str, List],
    sampling_height: Optional[str] = None,
) -> Dict:
    """Retrieve ICOS data from the ICOS Carbon Portal and standardise it into
    a format expected by OpenGHG. A dictionary of metadata and Datasets

    Args:
        site: ICOS site code, for site codes see
        https://www.icos-cp.eu/observations/atmosphere/stations
        sampling_height: Sampling height in metres
    Returns:
        dict: Dictionary of processed data and metadata
    """
    from icoscp.station import station
    from icoscp.cpb.dobj import Dobj
    from openghg.standardise.meta import assign_attributes
    import re

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

    standardised_data = {}

    for dobj_url in dobj_urls:
        dobj = Dobj(dobj_url)
        metadata = _extract_metadata(meta=dobj.info)
        dataframe = dobj.data

        dataframe.columns = [x.lower() for x in dataframe.columns]
        dataframe = dataframe.dropna(axis="index")

        if not dataframe.index.is_monotonic_increasing:
            dataframe = dataframe.sort_index()

        spec = metadata["species"]
        rename_cols = {
            "stdev": spec + " variability",
            "nbpoints": spec + " number_of_observations",
        }

        dataset = dataframe.rename(columns=rename_cols).set_index("timestamp").to_xarray()
        dataset.attrs.update(metadata)

        # TODO - do we need both attributes and metadata here?
        standardised_data[spec] = {
            "metadata": metadata,
            "data": dataset,
            "attributes": metadata,
        }

    # Make sure everything is CF compliant
    standardised_data = assign_attributes(data=standardised_data)

    return standardised_data


def _extract_metadata(meta: List) -> Dict:
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

    metadata["dobj_pid"] = _get_value(df=spec_data, col="dobj", index=0)
    metadata["species"] = _get_value(df=measurement_data, col="colName", index=4)

    metadata["meas_type"] = _get_value(df=measurement_data, col="valueType", index=4)
    metadata["units"] = _get_value(df=measurement_data, col="unit", index=4)

    metadata["site"] = _get_value(df=site_data, col="stationName", index=0)
    metadata["station_id"] = _get_value(df=site_data, col="stationId", index=0)
    metadata["sampling_height"] = _get_value(df=site_data, col="samplingHeight", index=0)
    metadata["latitude"] = _get_value(df=site_data, col="latitude", index=0)
    metadata["longitude"] = _get_value(df=site_data, col="longitude", index=0)
    metadata["elevation"] = _get_value(df=site_data, col="elevation", index=0)

    return metadata


def _get_value(df: DataFrame, col: str, index: int) -> str:
    """Wrap the retrieval of data from the metadata DataFrame in a try/except

    Args:
        df: Metadata DataFrame
        col: Column name
        index: Index number
    Returns:
        str: Metadata value
    """
    try:
        return str(df[col][index]).lower()
    except KeyError:
        return "NA"
