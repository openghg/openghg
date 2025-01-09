from typing import Any
from openghg.dataobjects import ObsData
from openghg.objectstore import get_writable_bucket
from openghg.standardise.meta import dataset_formatter, align_metadata_attributes
from openghg.util import load_json
from openghg.types import MetadataFormatError
import openghg_defs
import logging

logger = logging.getLogger("openghg.retrieve")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def retrieve_atmospheric(
    site: str,
    species: str | list | None = None,
    inlet: str | None = None,
    sampling_height: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    force_retrieval: bool = False,
    data_level: int = 2,
    dataset_source: str | None = None,
    store: str | None = None,
    update_mismatch: str = "never",
    force: bool = False,
) -> ObsData | list[ObsData] | None:
    """Retrieve ICOS atmospheric measurement data. If data is found in the object store it is returned. Otherwise
    data will be retrieved from the ICOS Carbon Portal. Data retrieval from the Carbon Portal may take a short time.
    If only a single data source is found an ObsData object is returned, if multiple a list of ObsData objects
    if returned, if nothing then None.

    Args:
        site: Site code
        species: Species name
        inlet: Height of the inlet for sampling in metres.
        sampling_height: Alias for inlet
        start_date: Start date
        end_date: End date
        force_retrieval: Force the retrieval of data from the ICOS Carbon Portal
        data_level: ICOS data level (1, 2)
        - Data level 1: Near Real Time Data (NRT) or Internal Work data (IW).
        - Data level 2: The final quality checked ICOS RI data set, published by the CFs,
                        to be distributed through the Carbon Portal.
                        This level is the ICOS-data product and free available for users.
        See https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone
        dataset_source: Dataset source name, for example ICOS, InGOS, European ObsPack
        store: Name of object to search/store data to
        update_mismatch: This determines how mismatches between the "metadata" derived from
            stored data and "attributes" derived from ICOS Header are handled.
            This includes the options:
                - "never" - don't update mismatches and raise an AttrMismatchError
                - "from_source" / "attributes" - update mismatches based on attributes from ICOS Header
                - "from_definition" / "metadata" - update mismatches based on input metadata
        force: Force adding of data even if this is identical to data stored (checked based on previously retrieved file hashes).
    Returns:
        ObsData, list[ObsData] or None
    """
    return retrieve(
        site=site,
        species=species,
        inlet=inlet,
        sampling_height=sampling_height,
        start_date=start_date,
        end_date=end_date,
        force_retrieval=force_retrieval,
        data_level=data_level,
        dataset_source=dataset_source,
        update_mismatch=update_mismatch,
        store=store,
        force=force,
    )


def retrieve(
    site: str,
    species: str | list[str] | None = None,
    inlet: str | None = None,
    sampling_height: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    force_retrieval: bool = False,
    data_level: int = 2,
    dataset_source: str | None = None,
    store: str | None = None,
    update_mismatch: str = "never",
    force: bool = False,
    **kwargs: Any,
) -> ObsData | list[ObsData] | None:
    """Retrieve ICOS atmospheric measurement data. If data is found in the object store it is returned. Otherwise
    data will be retrieved from the ICOS Carbon Portal. Data retrieval from the Carbon Portal may take a short time.
    If only a single data source is found an ObsData object is returned, if multiple a list of ObsData objects
    if returned, if nothing then None.

    Args:
        site: Site code
        species: Species name
        inlet: Height of the inlet for sampling in metres.
        sampling_height: Alias for inlet
        start_date: Start date
        end_date: End date
        force_retrieval: Force the retrieval of data from the ICOS Carbon Portal
        data_level: ICOS data level (1, 2)
        - Data level 1: Near Real Time Data (NRT) or Internal Work data (IW).
        - Data level 2: The final quality checked ICOS RI data set, published by the CFs,
                        to be distributed through the Carbon Portal.
                        This level is the ICOS-data product and free available for users.
        See https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone
        dataset_source: Dataset source name, for example ICOS, InGOS, European ObsPack
        store: Name of object to search/store data to
        update_mismatch: This determines how mismatches between the "metadata" derived from
            stored data and "attributes" derived from ICOS Header are handled.
            This includes the options:
                - "never" - don't update mismatches and raise an AttrMismatchError
                - "from_source" / "attributes" - update mismatches based on attributes from ICOS Header
                - "from_definition" / "metadata" - update mismatches based on input metadata
        force: Force adding of data even if this is identical to data stored (checked based on previously retrieved file hashes).
    Returns:
        ObsData, list[ObsData] or None
    """
    from openghg.retrieve import search_surface
    from openghg.store import ObsSurface
    from openghg.util import to_lowercase, format_data_level

    # ICOS: Potentially a different constraint for data_level to general constraint ([1, 2], rather than [0, 1, 2, 3])
    if not 1 <= int(data_level) <= 2:
        msg = "Error: for ICOS data the data level must be 1 or 2."
        logger.exception(msg)
        raise MetadataFormatError(msg)

    if sampling_height and inlet is None:
        inlet = sampling_height
    elif sampling_height and inlet:
        logger.warning(f"Both sampling height and inlet specified. Using inlet value of {inlet}")

    # Search for data_level OR icos_data_level keyword within current data.
    # - icos_data_level is no longer added but this is included for backwards compatability.
    data_level_keywords = {
        "data_level": format_data_level(data_level),
        "icos_data_level": format_data_level(data_level),
    }

    search_keywords: dict[str, Any] = {
        "site": site,
        "species": species,
        "inlet": inlet,
        "network": "ICOS",
        "data_source": "icoscp",
        "start_date": start_date,
        "end_date": end_date,
        "dataset_source": dataset_source,
        "store": store,
        "data_level": data_level_keywords,
    }

    results = search_surface(**search_keywords)

    if results and not force_retrieval:
        obs_data = results.retrieve_all()
        # break
    else:
        # We'll also need to check we have current data
        standardised_data = _retrieve_remote(
            site=site,
            species=species,
            data_level=data_level,
            dataset_source=dataset_source,
            inlet=inlet,
            sampling_height=sampling_height,
            update_mismatch=update_mismatch,
        )
        if standardised_data is None:
            return None

        bucket = get_writable_bucket(name=store)
        with ObsSurface(bucket=bucket) as obs:
            obs.store_data(data=standardised_data, force=force)

        # Create the expected ObsData type
        obs_data = []
        for data in standardised_data.values():
            measurement_data = data["data"]
            # These contain URLs that are case sensitive so skip lowercasing these
            skip_keys = [
                "citation_string",
                "instrument_data",
                "dobj_pid",
                "dataset_source",
            ]
            metadata = to_lowercase(data["metadata"], skip_keys=skip_keys)
            obs_data.append(ObsData(data=measurement_data, metadata=metadata))

    if isinstance(obs_data, list) and len(obs_data) == 1:
        return obs_data[0]
    else:
        return obs_data


def _retrieve_remote(
    site: str,
    data_level: int,
    species: str | list | None = None,
    inlet: str | None = None,
    sampling_height: str | None = None,
    dataset_source: str | None = None,
    update_mismatch: str = "never",
) -> dict | None:
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
        inlet: Height of the inlet for sampling in metres.
        sampling_height: Alias for inlet
        dataset_source: Dataset source name, for example ICOS, InGOS, European ObsPack
        update_mismatch: This determines how mismatches between the "metadata" derived from
            stored data and "attributes" derived from ICOS Header are handled.
            This includes the options:
                - "never" - don't update mismatches and raise an AttrMismatchError
                - "from_source" / "attributes" - update mismatches based on attributes from ICOS Header
                - "from_definition" / "metadata" - update mismatches based on input metadata
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
    from openghg.util import format_inlet, format_data_level
    from pandas import to_datetime

    if species is None:
        species = ["CO", "CO2", "CH4"]

    if not isinstance(species, list):
        species = [species]

    if sampling_height and inlet is None:
        inlet = sampling_height
    elif sampling_height and inlet:
        logger.warning(f"Both sampling height and inlet specified. Using inlet value of {inlet}")

    # We should first check if it's stored in the object store
    # Will need to make sure ObsSurface can accept the datasets we
    # create from the ICOS data
    stat = station.get(stationId=site.upper())

    if not stat.valid:
        logger.error("Please check you have passed a valid ICOS site and have a working internet connection.")
        return None

    data_pids = stat.data(level=data_level)

    species_upper = [s.upper() for s in species]

    # We want to get the PIDs of the data for each species here
    # Annoyingly FastTrack and EYE-AVE-PAR data don't have the species anywhere in the data_pids dataframe
    # so we need to handle these cases separately
    if dataset_source in ["ICOS FastTrack", "EYE-AVE-PAR"]:
        search_str = "GHG"
    else:
        # For this see https://stackoverflow.com/a/55335207
        search_str = r"\b(?:{})\b".format("|".join(map(re.escape, species_upper)))

    # Now filter the dataframe so we can extract the PIDS
    # We filter out any data that contains "Obspack" or "csv" in the specLabel
    # Also filter out some drought files which cause trouble being read in
    # For some reason they have separate station record pages that contain "ATMO_"
    filtered_sources = data_pids[
        data_pids["specLabel"].str.contains(search_str)
        & ~data_pids["specLabel"].str.contains("Obspack")
        & ~data_pids["specLabel"].str.contains("csv")
        & ~data_pids["station"].str.contains("ATMO_")
    ]

    if inlet is not None:
        inlet = str(float(inlet.rstrip("m")))
        height_filter = [inlet in str(x) for x in filtered_sources["samplingheight"]]
        filtered_sources = filtered_sources[height_filter]

    if filtered_sources.empty:
        logger.error(
            f"No sources found for {species} at {site}. Please check with the ICOS Carbon Portal that this data is available."
        )
        return None

    # Now extract the PIDs along with some data about them
    dobj_urls = filtered_sources["dobj"].tolist()

    # Load our site metadata for a few things like the station's long_name that
    # isn't in the ICOS metadata in the way we want it at the momenet - 2023-03-20
    site_info_fpath = openghg_defs.site_info_file
    openghg_site_metadata = load_json(path=site_info_fpath)

    standardised_data: dict[str, dict] = {}

    for n, dobj_url in enumerate(dobj_urls):
        dobj = Dobj(dobj_url)
        logger.info(f"Retrieving {dobj_url}...")

        if dataset_source == "ICOS FastTrack":
            species_fname = re.split("[_.]", dobj.meta["fileName"])[-2]
            if "FAST_TRACK" in dobj.meta["fileName"] or species_fname in species_upper:
                dobj_dataset_source = "ICOS FastTrack"
            else:
                continue
        elif dataset_source == "EYE-AVE-PAR":
            species_fname = dobj.meta["fileName"].split(".")[-2]
            if "EYE-AVE-PAR" in dobj.meta["fileName"] or species_fname in species_upper:
                dobj_dataset_source = "EYE-AVE-PAR"
            else:
                continue
        else:
            try:
                dobj_dataset_source = dobj.meta["specification"]["project"]["self"]["label"]
            except KeyError:
                dobj_dataset_source = "NA"
                logger.warning("Unable to read project information from dobj.")

            if dataset_source is not None and dataset_source.lower() != dobj_dataset_source.lower():
                continue

        # We need to pull the data down as .info (metadata) is populated further on this step
        dataframe = dobj.get()
        # This is the metadata, dobj.info and dobj.meta are equal
        dobj_info = dobj.meta

        attributes = {}

        attributes["icoscp_url"] = str(dobj_url)
        specific_info = dobj_info["specificInfo"]
        col_data = specific_info["columns"]

        # Get the species this dobj holds information for
        not_the_species = {"TIMESTAMP", "Flag", "NbPoints", "Stdev"}
        species_info = next(i for i in col_data if i["label"] not in not_the_species)

        measurement_type = species_info["valueType"]["self"]["label"].lower()
        units = species_info["valueType"]["unit"].lower()
        the_species = species_info["label"]

        species_info = next(item for item in col_data if str(item["label"]).lower() == the_species.lower())

        attributes["species"] = the_species
        acq_data = specific_info["acquisition"]
        station_data = acq_data["station"]

        to_store: dict[str, Any] = {}
        try:
            instrument_attributes = acq_data["instrument"]
        except KeyError:
            to_store["instrument"] = "NA"
            to_store["instrument_data"] = "NA"
        else:
            # Do some tidying of the instrument attributes
            instruments = set()
            cleaned_instrument_attributes = []

            if not isinstance(instrument_attributes, list):
                instrument_attributes = [instrument_attributes]

            for inst in instrument_attributes:
                instrument_name = inst["label"]
                instruments.add(instrument_name)
                uri = inst["uri"]

                cleaned_instrument_attributes.extend([instrument_name, uri])

            if len(instruments) == 1:
                instrument = instruments.pop()
            else:
                instrument = "multiple"

            to_store["instrument"] = instrument
            to_store["instrument_data"] = cleaned_instrument_attributes

        attributes.update(to_store)

        attributes["site"] = station_data["id"]
        attributes["measurement_type"] = measurement_type
        # TODO: Remove this from general attributes but make sure this is
        # included as a specific value on the appropriate variable.
        attributes["units"] = units

        _sampling_height = acq_data["samplingHeight"]
        attributes["sampling_height"] = format_inlet(_sampling_height, key_name="sampling_height")
        attributes["sampling_height_units"] = "metres"
        attributes["inlet"] = format_inlet(_sampling_height, key_name="inlet")
        attributes["inlet_height_magl"] = format_inlet(_sampling_height, key_name="inlet_height_magl")

        loc_data = station_data["location"]

        attributes["station_long_name"] = loc_data["label"]
        attributes["station_latitude"] = str(loc_data["lat"])
        attributes["station_longitude"] = str(loc_data["lon"])

        # 03/05/2023: Updated attributes to include altitude for "station_height_masl" explicitly.
        # attributes["station_altitude"] = format_inlet(loc_data["alt"], key_name="station_altitude")
        # attributes["station_height_masl"] = format_inlet(str(stat.eas), key_name="station_height_masl")
        attributes["station_height_masl"] = format_inlet(loc_data["alt"], key_name="station_height_masl")

        attributes["data_owner"] = f"{stat.firstName} {stat.lastName}"
        attributes["data_owner_email"] = str(stat.email)

        attributes["citation_string"] = dobj_info["references"]["citationString"]
        attributes["licence_name"] = dobj_info["references"]["licence"]["name"]
        attributes["licence_info"] = dobj_info["references"]["licence"]["url"]

        metadata = {}

        network = "ICOS"

        try:
            site_info = openghg_site_metadata[site.upper()][network]
        except KeyError:
            pass
        else:
            metadata["station_long_name"] = site_info["long_name"]
            metadata["station_latitude"] = site_info["latitude"]
            metadata["station_longitude"] = site_info["longitude"]

        # Add some values directly for attributes (for now)
        metadata["species"] = attributes["species"]

        # Add ICOS in directly here for now
        additional_data = {}
        additional_data["network"] = network
        additional_data["data_type"] = "surface"
        additional_data["data_source"] = "icoscp"
        additional_data["source_format"] = "icos"
        # additional_data["icos_data_level"] = str(data_level)
        additional_data["data_level"] = format_data_level(data_level)
        additional_data["dataset_source"] = dobj_dataset_source
        additional_data["site"] = site

        attributes.update(additional_data)
        metadata.update(additional_data)

        spec = attributes["species"]

        dataframe.columns = [x.lower() for x in dataframe.columns]

        # Apply ICOS flags - O, U and R are all valid data, set mf to nan for everything else
        dataframe[spec] = dataframe[spec].where(dataframe["flag"].isin(["O", "U", "R"]))
        dataframe = dataframe.dropna(axis="index")

        if not dataframe.index.is_monotonic_increasing:
            dataframe = dataframe.sort_index()

        # If there is a stdev column, replace missing values with nans
        # Then rename columns
        try:
            dataframe["stdev"] = dataframe["stdev"].where(dataframe["stdev"] >= 0)
            rename_cols = {
                "stdev": spec + " variability",
                "nbpoints": spec + " number_of_observations",
            }
        except KeyError:
            rename_cols = {
                "nbpoints": spec + " number_of_observations",
            }

        dataframe = dataframe.rename(columns=rename_cols).set_index("timestamp")

        dataframe.index.name = "time"
        dataframe.index = to_datetime(dataframe.index, format="%Y-%m-%d %H:%M:%S")

        dataset = dataframe.to_xarray()
        dataset.attrs.update(attributes)

        # So there isn't an easy way of getting a hash of a Dataset, can we do something
        # simple here we can compare data that's being added? Then we'll be able to make sure
        # ObsSurface.store_data won't accept data it's already seen
        data_key = f"key-{n}"
        # TODO - do we need both attributes and metadata here?
        standardised_data[data_key] = {
            "metadata": metadata,
            "data": dataset,
            "attributes": attributes,
        }
    standardised_data = dataset_formatter(data=standardised_data)
    standardised_data = assign_attributes(data=standardised_data, update_mismatch=update_mismatch)
    align_metadata_attributes(data=standardised_data, update_mismatch=update_mismatch)

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
