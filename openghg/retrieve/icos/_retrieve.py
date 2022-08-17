from typing import Any, Dict, List, Optional, Union

from openghg.dataobjects import ObsData
from openghg.util import running_on_hub


def retrieve_atmospheric(
    site: str,
    species: Optional[Union[str, List]] = None,
    sampling_height: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force_retrieval: bool = False,
    data_level: int = 2,
) -> Union[ObsData, List[ObsData], None]:
    """Retrieve ICOS atmospheric measurement data. If data is found in the object store it is returned. Otherwise
    data will be retrieved from the ICOS Carbon Portal. Data retrieval from the Carbon Portal may take a short time.
    If only a single data source is found an ObsData object is returned, if multiple a list of ObsData objects
    if returned, if nothing then None.

    Args:
        site: Site code
        species: Species name
        start_date: Start date
        end_date: End date
        force_retrieval: Force the retrieval of data from the ICOS Carbon Portal
        data_level: ICOS data level (1, 2)
        - Data level 1: Near Real Time Data (NRT) or Internal Work data (IW).
        - Data level 2: The final quality checked ICOS RI data set, published by the CFs,
                        to be distributed through the Carbon Portal.
                        This level is the ICOS-data product and free available for users.
        See https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone
        bypass_call: Bypass the remote function call, used to shortcut calls within a the serverless
        function call environment.
    Returns:
        ObsData, list[ObsData] or None
    """
    return retrieve(
        site=site,
        species=species,
        sampling_height=sampling_height,
        start_date=start_date,
        end_date=end_date,
        force_retrieval=force_retrieval,
        data_level=data_level,
    )


def retrieve(**kwargs: Any) -> Union[ObsData, List[ObsData], None]:
    """Retrieve data from the ICOS Carbon Portal. If data is found in the local object store
    it will be retrieved from there first.

    This function detects the running environment and routes the call
    to either the cloud or local search function.

    Example / commonly used arguments are given below.

    Args:
        site: Site code
        species: Species name
        start_date: Start date
        end_date: End date
        force_retrieval: Force the retrieval of data from the ICOS Carbon Portal
        data_level: ICOS data level (1, 2)
        - Data level 1: Near Real Time Data (NRT) or Internal Work data (IW).
        - Data level 2: The final quality checked ICOS RI data set, published by the CFs,
                        to be distributed through the Carbon Portal.
                        This level is the ICOS-data product and free available for users.
        See https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone
        bypass_call: Bypass the remote function call, used to shortcut calls within a the serverless
        function call environment.
    Returns:
        ObsData, list[ObsData] or None
    """
    from io import BytesIO

    from openghg.cloud import call_function, unpackage
    from xarray import load_dataset

    # The hub is the only place we want to make remote calls
    if running_on_hub():
        post_data: Dict[str, Union[str, Dict]] = {}
        post_data["function"] = "retrieve_icos"
        post_data["search_terms"] = kwargs

        call_result = call_function(data=post_data)

        content = call_result["content"]
        found = content["found"]

        if not found:
            return None

        observations = content["data"]

        obs_data = []
        for package in observations.values():
            unpackaged = unpackage(data=package)
            buf = BytesIO(unpackaged["data"])
            ds = load_dataset(buf)
            obs = ObsData(data=ds, metadata=unpackaged["metadata"])

            obs_data.append(obs)

        if len(obs_data) == 1:
            return obs_data[0]
        else:
            return obs_data
    else:
        return local_retrieve(**kwargs)


def local_retrieve(
    site: str,
    species: Optional[Union[str, List]] = None,
    sampling_height: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force_retrieval: bool = False,
    data_level: int = 2,
    **kwargs: Any,
) -> Union[ObsData, List[ObsData], None]:
    """Retrieve ICOS atmospheric measurement data. If data is found in the object store it is returned. Otherwise
    data will be retrieved from the ICOS Carbon Portal. Data retrieval from the Carbon Portal may take a short time.
    If only a single data source is found an ObsData object is returned, if multiple a list of ObsData objects
    if returned, if nothing then None.

    Args:
        site: Site code
        species: Species name
        start_date: Start date
        end_date: End date
        force_retrieval: Force the retrieval of data from the ICOS Carbon Portal
        data_level: ICOS data level (1, 2)
        - Data level 1: Near Real Time Data (NRT) or Internal Work data (IW).
        - Data level 2: The final quality checked ICOS RI data set, published by the CFs,
                        to be distributed through the Carbon Portal.
                        This level is the ICOS-data product and free available for users.
        See https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone
    Returns:
        ObsData, list[ObsData] or None
    """
    from openghg.dataobjects import ObsData
    from openghg.retrieve import search_surface
    from openghg.store import ObsSurface
    from openghg.util import to_lowercase

    if not 1 <= data_level <= 2:
        print("Error: data level must be 1 or 2.")

    # NOTE - we skip ranking here, will we be ranking ICOS data?
    results = search_surface(
        site=site,
        species=species,
        sampling_height=sampling_height,
        network="ICOS",
        data_source="icoscp",
        start_date=start_date,
        end_date=end_date,
        icos_data_level=data_level,
        skip_ranking=True,
    )

    if results and not force_retrieval:
        obs_data = results.retrieve_all()
    else:
        # We'll also need to check we have current data
        standardised_data = _retrieve_remote(site=site, species=species, data_level=data_level)

        if standardised_data is None:
            return None

        ObsSurface.store_data(data=standardised_data)

        # Create the expected ObsData type
        obs_data = []
        for data in standardised_data.values():
            measurement_data = data["data"]
            # These contain URLs that are case sensitive so skip lowercasing these
            skip_keys = ["citation_string", "instrument_data", "dobj_pid"]
            metadata = to_lowercase(data["metadata"], skip_keys=skip_keys)
            obs_data.append(ObsData(data=measurement_data, metadata=metadata))

    if isinstance(obs_data, list) and len(obs_data) == 1:
        return obs_data[0]
    else:
        return obs_data


def _retrieve_remote(
    site: str,
    data_level: int,
    species: Optional[Union[str, List]] = None,
    sampling_height: Optional[str] = None,
) -> Optional[Dict]:
    """Retrieve ICOS data from the ICOS Carbon Portal and standardise it into
    a format expected by OpenGHG. A dictionary of metadata and Datasets

    Args:
        site: ICOS site code, for site codes see
        https://www.icos-cp.eu/observations/atmosphere/stations
        data_level: ICOS data level (1, 2)
        - Data level 1: Near Real Time Data (NRT) or Internal Work data (IW).
        - Data level 2: The final quality checked ICOS RI data set, published by the CFs,
                        to be distributed through the Carbon Portal.
                        This level is the ICOS-data product and free available for users.
        See https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone
        species: Species name
        sampling_height: Sampling height in metres
    Returns:
        dict or None: Dictionary of processed data and metadata if found
    """
    # icoscp isn't available to conda so we've got to resort to this for now
    try:
        from icoscp.cpb.dobj import Dobj  # type: ignore
        from icoscp.station import station  # type: ignore
    except ImportError:
        raise ImportError(
            "Cannot import icoscp, if you've installed OpenGHG using conda please run: pip install icoscp"
        )

    import re

    from openghg.standardise.meta import assign_attributes
    from openghg.util import load_json
    from pandas import to_datetime

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
        dobj_info = dobj.meta

        metadata = {}

        specific_info = dobj_info["specificInfo"]
        col_data = specific_info["columns"]

        for col in col_data:
            # Find the species
            for s in species:
                if col["label"] == s.lower():
                    measurement_type = col["valueType"]["self"]["label"].lower()
                    units = col["valueType"]["unit"].lower()
                    this_species = str(s)
                    break

        metadata["species"] = this_species
        acq_data = specific_info["acquisition"]
        station_data = acq_data["station"]

        to_store: Dict[str, Any] = {}
        try:
            instrument_metadata = acq_data["instrument"]
        except KeyError:
            to_store["instrument"] = "NA"
            to_store["instrument_data"] = "NA"
        else:
            # Do some tidying of the instrument metadata
            instruments = set()
            cleaned_instrument_metadata = []

            if not isinstance(instrument_metadata, list):
                instrument_metadata = [instrument_metadata]

            for inst in instrument_metadata:
                instrument_name = inst["label"]
                instruments.add(instrument_name)
                uri = inst["uri"]

                cleaned_instrument_metadata.extend([instrument_name, uri])

            if len(instruments) == 1:
                instrument = instruments.pop()
            else:
                instrument = "multiple"

            to_store["instrument"] = instrument
            to_store["instrument_data"] = cleaned_instrument_metadata

        metadata.update(to_store)

        metadata["site"] = station_data["id"]
        metadata["measurement_type"] = measurement_type
        metadata["units"] = units

        _sampling_height = acq_data["samplingHeight"]
        metadata["sampling_height"] = f"{int(float(_sampling_height))}m"
        metadata["inlet_height_magl"] = f"{int(float(_sampling_height))}m"
        metadata["sampling_height_units"] = "metres"
        metadata["inlet"] = f"{int(float(_sampling_height))}m"

        loc_data = station_data["location"]
        metadata["station_long_name"] = loc_data["label"]
        metadata["station_latitude"] = str(loc_data["lat"])
        metadata["station_longitude"] = str(loc_data["lon"])
        metadata["station_altitude"] = f"{int(float(loc_data['alt']))}m"

        site_specific = site_metadata[site.upper()]
        metadata["data_owner"] = f"{site_specific['firstName']} {site_specific['lastName']}"
        metadata["data_owner_email"] = site_specific["email"]
        metadata["station_height_masl"] = f"{int(float(site_specific['eas']))}m"

        metadata["citation_string"] = dobj_info["references"]["citationString"]
        metadata["licence_name"] = dobj_info["references"]["licence"]["name"]
        metadata["licence_info"] = dobj_info["references"]["licence"]["url"]

        # Add ICOS in directly here for now
        metadata["network"] = "ICOS"
        metadata["data_type"] = "timeseries"
        metadata["data_source"] = "icoscp"
        metadata["icos_data_level"] = str(data_level)

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

        # TODO - add this back in once we've merged the fixes in
        # Try and conver the flag / userflag column to str
        # possible_flag_cols = ("flag", "userflag")
        # flag_col = [x for x in dataframe.columns if x in possible_flag_cols]

        # PR328
        # if flag_col:
        #     flag_str = flag_col[0]
        #     dataframe = dataframe.astype({flag_str: str})

        dataframe = dataframe.rename(columns=rename_cols).set_index("timestamp")

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


# def _read_site_metadata():
#     """ Read site metadata from object store, if it doesn't exist we'll
#     retrieve it from the ICOS CP and store it.

#     Returns:
#         dict: Dictionary of site data
#     """
#     from openghg.objectstore import get_bucket, get_object_from_json
#     from openghg.types import ObjectStoreError
#     from openghg.util import timestamp_now
# raise NotImplementedError
#     key = "metadata/icos_atmos_site_metadata"
#     bucket = get_bucket()

#     try:
#         data = get_object_from_json(bucket=bucket, key=key)
#     except ObjectStoreError:
#         # Retrieve and store
#         from icoscp import station
#         station_data = station.getIdList()
#         metadata = {d.id: dict(d) for _, d in df.iterrows()}
