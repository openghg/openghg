# I'm creating this submodule as I'm not quite sure where else to put this for now
# we can always move it in the future
from pandas import DataFrame
from typing import Dict, List, Optional, Union

from openghg.dataobjects import ObsData


def retrieve(
    site: str,
    species: Optional[Union[str, List]] = None,
    sampling_height: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force_retrieval: bool = False,
) -> Union[ObsData, List[ObsData], None]:
    """Retrieve ICOS data. If data is found in the object store it is returned. Otherwise
    data will be retrieved from the ICOS Carbon Portal. This may take more time.

    Args:
        site: Site code
        species: Species name
        start_date: Start date
        end_date: End date
        force_retrieval: Force the retrieval of data from the ICOS Carbon Portal
    Returns:
        ObsData or list
    """
    from openghg.retrieve import search
    from openghg.store import ObsSurface
    from openghg.dataobjects import ObsData
    from openghg.util import to_lowercase

    # NOTE - we skip ranking here, will we be ranking ICOS data?
    results = search(
        site=site,
        species=species,
        sampling_height=sampling_height,
        network="ICOS",
        data_source="icoscp",
        start_date=start_date,
        end_date=end_date,
        skip_ranking=True,
    )

    if results and not force_retrieval:
        obs_data: Union[ObsData, List[ObsData]] = results.retrieve_all()
    else:
        # We'll also need to check we have current data
        standardised_data = _retrieve_remote(site=site, species=species)

        if standardised_data is None:
            return None

        ObsSurface.store_data(data=standardised_data)

        # Create the expected ObsData type
        obs_data = []
        for data in standardised_data.values():
            measurement_data = data["data"]
            metadata = to_lowercase(data["metadata"])
            obs_data.append(ObsData(data=measurement_data, metadata=metadata))

    if isinstance(obs_data, list) and len(obs_data) == 1:
        return obs_data[0]
    else:
        return obs_data


def _retrieve_remote(
    site: str,
    species: Optional[Union[str, List]] = None,
    sampling_height: Optional[str] = None,
) -> Optional[Dict]:
    """Retrieve ICOS data from the ICOS Carbon Portal and standardise it into
    a format expected by OpenGHG. A dictionary of metadata and Datasets

    Args:
        site: ICOS site code, for site codes see
        https://www.icos-cp.eu/observations/atmosphere/stations
        sampling_height: Sampling height in metres
    Returns:
        dict or None: Dictionary of processed data and metadata if found
    """
    from icoscp.station import station  # type: ignore
    from icoscp.cpb.dobj import Dobj  # type: ignore
    from openghg.standardise.meta import assign_attributes
    from openghg.util import load_json
    from pandas import to_datetime
    import re

    if species is None:
        species = ["CO", "CO2", "CH4"]

    if not isinstance(species, list):
        species = [species]

    # We should first check if it's stored in the object store
    # Will need to make sure ObsSurface can accept the datasets we
    # create from the ICOS data
    stat = station.get(stationId=site.upper())

    if not stat.valid:
        print("Please check you have passed a valid ICOS site.")
        return None

    # See https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone
    # - Data level 2: The final quality checked ICOS RI data set, published by the CFs,
    #  to be distributed through the Carbon Portal.
    data_level = 2
    data_pids = stat.data(level=data_level)

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

    if filtered_sources.empty:
        print(
            f"No sources found for {species} at {site}. Please check with the ICOS Carbon Portal that this data is available."
        )
        return None

    # Now extract the PIDs along with some data about them
    dobj_urls = filtered_sources["dobj"].tolist()

    site_metadata = load_json("icos_atmos_site_metadata.json")

    standardised_data: Dict[str, Dict] = {}

    for dobj_url in dobj_urls:
        dobj = Dobj(dobj_url)
        # We need to pull the data down as .info (metadata) is populated further on this step
        dataframe = dobj.get()
        # This is the metadata
        dobj_info = dobj.info

        metadata = _extract_metadata(meta=dobj_info, site_metadata=site_metadata)

        # Add ICOS in directly here for now
        metadata["network"] = "ICOS"
        # The instrument doesn't seem to be in the
        # metadata returned from the ICOS CP,
        # How should we handle this?
        metadata["instrument"] = "NA"
        metadata["data_type"] = "timeseries"
        metadata["data_source"] = "icoscp"

        dataframe.columns = [x.lower() for x in dataframe.columns]
        dataframe = dataframe.dropna(axis="index")

        if not dataframe.index.is_monotonic_increasing:
            dataframe = dataframe.sort_index()

        spec = metadata["species"]
        inlet = metadata["inlet"]

        rename_cols = {
            "stdev": spec + " variability",
            "nbpoints": spec + " number_of_observations",
        }

        dataframe = dataframe.rename(columns=rename_cols).set_index("timestamp").astype({"flag": str})
        dataframe.index.name = "time"
        dataframe.index = to_datetime(dataframe.index, format="%Y-%m-%d %H:%M:%S")

        dataset = dataframe.to_xarray()
        dataset.attrs.update(metadata)

        # So there isn't an easy way of getting a hash of a Dataset, can we do something
        # simple here we can compare data that's being added? Then we'll be able to make sure
        # ObsSurface.store_data won't accept data it's already seen
        data_key = f"{spec}_{inlet}"
        # TODO - do we need both attributes and metadata here?
        standardised_data[data_key] = {
            "metadata": metadata,
            "data": dataset,
            "attributes": metadata,
        }

    # Make sure everything is CF compliant
    standardised_data = assign_attributes(data=standardised_data)

    return standardised_data


def _extract_metadata(meta: List, site_metadata: Dict) -> Dict:
    """Extract metadata from the list of pandas DataFrames that are
    returned by the ICOS-CP pylib Dobj method.

    Args:
        meta: List of dataframes containing metadata
        site_metadata: Dictionary of site metadata
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

    site = _get_value(df=site_data, col="stationId", index=0)
    sampling_height = _get_value(df=site_data, col="samplingHeight", index=0)

    metadata["site"] = site
    metadata["station_long_name"] = _get_value(df=site_data, col="stationName", index=0)
    # ICOS have sampling height as a float, we usually work with ints and an m on the end
    # should we have a separate sampling_height_units record
    metadata["sampling_height"] = f"{str(int(sampling_height))}m"
    metadata["sampling_height_units"] = "metres"
    metadata["inlet"] = f"{str(int(sampling_height))}m"
    metadata["station_latitude"] = _get_value(df=site_data, col="latitude", index=0)
    metadata["station_longitude"] = _get_value(df=site_data, col="longitude", index=0)
    metadata["elevation"] = _get_value(df=site_data, col="elevation", index=0)

    site_specific = site_metadata[site.upper()]
    metadata["data_owner"] = f"{site_specific['firstName']} {site_specific['lastName']}"
    metadata["data_owner_email"] = site_specific["email"]
    metadata["station_height_masl"] = site_specific["eas"]

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
    except (KeyError, TypeError):
        print(f"DEBUG: line 190 _retrieve.py: Can't find {col} {index}")
        return "NA"
