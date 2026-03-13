from typing import Any
import logging
import io
import pandas as pd
import xarray as xr
import openghg_defs

from openghg.dataobjects import ObsData
from openghg.objectstore import get_writable_bucket
from openghg.standardise.meta import dataset_formatter, align_metadata_attributes
from openghg.util import load_json
from openghg.types import convert_to_list_of_metadata_and_data, MetadataAndData, MetadataFormatError

from openghg.retrieve.icos import _data_parsing

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
    retrieve_method: str = "icoscp_core",
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
        dataset_source: Dataset source name, for example ICOS, InGOS, European ObsPack. Specify "ICOS
            Combined" here in order to retrieve the combined timeseries including all Obspack and ICOS
            data (e.g. https://doi.org/10.18160/0HYS-FF7X)
        store: Name of object to search/store data to
        update_mismatch: This determines how mismatches between the "metadata" derived from
            stored data and "attributes" derived from ICOS Header are handled.
            This includes the options:
                - "never" - don't update mismatches and raise an AttrMismatchError
                - "from_source" / "attributes" - update mismatches based on attributes from ICOS Header
                - "from_definition" / "metadata" - update mismatches based on input metadata
        force: Force adding of data even if this is identical to data stored (checked based on previously retrieved file hashes).
        retrieve_method: Option of 'icoscp_core' or 'dobj' method but should always use 'icoscp_core' method
             unless testing against previous approach. The 'dobj' method is included for completeness but is deprecated.
    Returns:
        ObsData, list[ObsData] or None
    """
    from openghg.retrieve import search_surface
    from openghg.store import ObsSurface
    from openghg.util import to_lowercase, format_data_level

    retrieve_methods = ["icoscp_core", "dobj"]

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
        if retrieve_method == "icoscp_core":
            standardised_data = _retrieve_remote(
                site=site,
                species=species,
                data_level=data_level,
                dataset_source=dataset_source,
                inlet=inlet,
                sampling_height=sampling_height,
                update_mismatch=update_mismatch,
            )
        elif retrieve_method == "dobj":
            # Deprecated method but retained to alow for comparison with updated icoscp_core method
            standardised_data = _retrieve_remote_dobj(
                site=site,
                species=species,
                data_level=data_level,
                dataset_source=dataset_source,
                inlet=inlet,
                sampling_height=sampling_height,
                update_mismatch=update_mismatch,
            )
        else:
            raise ValueError(
                f"Did not recognise input for retrieve_method. Should be one of {retrieve_methods}"
            )

        if standardised_data is None:
            return None

        bucket = get_writable_bucket(name=store)
        with ObsSurface(bucket=bucket) as obs:
            obs.store_data(data=standardised_data, force=force)

        # Create the expected ObsData type
        obs_data = []
        for data in standardised_data:
            measurement_data = data.data
            # These contain URLs that are case sensitive so skip lowercasing these
            skip_keys = [
                "citation_string",
                "instrument_data",
                "dobj_pid",
                "dataset_source",
            ]
            metadata = to_lowercase(data.metadata, skip_keys=skip_keys)
            obs_data.append(ObsData(data=measurement_data, metadata=metadata))

    if isinstance(obs_data, list) and len(obs_data) == 1:
        return obs_data[0]
    else:
        return obs_data


# Parsing ICOS text file format is also
# equivalent to openghg.standardise.surface._icos.py
# def _read_data_large_header
# TODO: Should ensure this is put in a central location and not duplicated


def parse_icos_text_file(data_info: dict | pd.Series) -> tuple[xr.Dataset, dict]:
    """
    Updating to match previous Dobj format initially and then
    converting to Dataset
    """

    dobj_uri = data_info["dobj_uri"]
    species = data_info["species"]

    icos_text = _data_parsing.get_icos_text_file(dobj_uri)

    # What are steps that we need to do to the text data to parse into expected DataFrame

    # TODO: Check why we're not using attrs rather than header?
    header = _data_parsing.get_icos_text_header(icos_text)
    # attrs = _data_parsing.get_full_attrs(dobj_uri, species, header)

    skiprows = int(header["header_lines"]) - 1

    # # skip first column becaues it is just the site name, and skip DecimalDate
    # usecols = [col for col in header["columns"][1:] if col != "DecimalDate"]

    usecols = [species, "Stdev", "NbPoints", "Flag", "Year", "Month", "Day", "Hour", "Minute"]

    # na_values = ["-999.99", "-9.99"]  # these are used in some ICOS files

    dataframe = pd.read_csv(
        io.StringIO(icos_text), skiprows=skiprows, sep=";", usecols=usecols
    )  # , na_values=na_values)

    # make time index
    date_cols, times = _data_parsing.parse_date_columns(dataframe)
    # df["time"] = times
    dataframe["TIMESTAMP"] = times
    dataframe = dataframe.drop(columns=date_cols)
    # dataframe = dataframe.set_index("time")

    # # drop na
    # drop_na_cols = [col for col in df.columns if col in [species, "Stdev", "NbPoints"]]
    # df = df.dropna(subset=drop_na_cols)

    # # try to get dtypes
    # dtypes = dtypes_dict(df.columns.to_list(), attrs) if attrs is not None else None
    # if dtypes is not None:
    #     # if Stdev dtype not specified, make it the same as the mole fraction column
    #     if species in dtypes and "Stdev" not in dtypes:
    #         dtypes["Stdev"] = dtypes[species]

    #     df = df.astype(dtypes)

    dataframe.columns = [x.lower() for x in dataframe.columns]

    # If there is a stdev column, replace missing values with nans
    # Then rename columns
    try:
        dataframe["stdev"] = dataframe["stdev"].where(dataframe["stdev"] >= 0)
        rename_cols = {
            "timestamp": "time",
            "stdev": species + " variability",
            "nbpoints": species + " number_of_observations",
        }
    except KeyError:
        rename_cols = {
            "timestamp": "time",
            "nbpoints": species + " number_of_observations",
        }

    dataframe = dataframe.rename(columns=rename_cols)

    if not dataframe.index.is_monotonic_increasing:
        dataframe = dataframe.sort_index()

    dataframe = dataframe.set_index("time")

    dataset = dataframe.to_xarray()

    return dataset, header


def parse_icos_obspack_nc_file(data_info: dict | pd.Series) -> tuple[xr.Dataset, dict]:
    """
    Parsing expected obspack netcdf format from ICOS CP
    """
    from pandas import Timedelta
    from numpy.char import decode
    from openghg.util import load_internal_json

    dobj_uri = data_info["dobj_uri"]
    species = data_info["species"]

    dataset = _data_parsing.get_icos_nc_file(dobj_uri)

    if species == "co2":
        units = "ppm"
    else:
        units = "ppb"

    # For now: for ICOS Combined need to convert units manually
    attributes_data = load_internal_json("attributes.json")
    unit_interpret = attributes_data["unit_interpret"]
    unit_value = float(unit_interpret.get(units, "1"))

    dataset["value"] = dataset["value"] / unit_value
    dataset["value_std_dev"] = dataset["value_std_dev"] / unit_value
    dataset["icos_LTR"] = dataset["icos_LTR"] / unit_value
    dataset["icos_SMR"] = dataset["icos_SMR"] / unit_value
    dataset["icos_STTB"] = dataset["icos_STTB"] / unit_value

    # TODO: Lower case all the data variables in the dataset

    rename_cols_lower = {dv: dv.lower() for dv in dataset}  # type: ignore
    dataset = dataset.rename(rename_cols_lower)

    rename_col_names = {
        "value": species,
        "qc_flag": "flag",
        "value_std_dev": species + " variability",
        "icos_ltr": species + " repeatability",
    }
    dataset = dataset.rename(rename_col_names)

    dataset["time"] = dataset["time"] - Timedelta(minutes=30)

    dataset[f"{species} repeatability"].attrs[
        "comment"
    ] = "ICOS LTR as defined by Yver Kwok et al., 2015, doi:10.5194/amt-8-3867-2015"

    # Recast "flag" column to decode bytes and update to same dtype as other data
    dataset["flag"].data = decode(dataset["flag"].astype("bytes_"), "utf-8").astype(object)

    attrs = dataset.attrs

    attrs["instrument"] = "NA"
    attrs["instrument_data"] = "NA"

    attrs["measurement_unit"] = units

    return dataset, attrs


def retrieve_and_parse_icos_data(data_info: dict | pd.Series, dataset_source: str | None = None) -> tuple[xr.Dataset, dict]:
    """
    Based on known formats we want to be able to retrieve from ICOS, call
    the correct parsing function (mainly based on dataset_source and derived data_format)
    """
    data_format = _data_parsing._retrieve_dobj_format(data_info)

    if data_format in ("asciiAtcFlaskTimeSer", "asciiAtcProductTimeSer"):
        dataset, data_attributes = parse_icos_text_file(data_info)
    elif data_format in ("netcdfTimeSeries"):
        if dataset_source == "ICOS Combined":
            dataset, data_attributes = parse_icos_obspack_nc_file(data_info)
        else:
            raise NotImplementedError(
                f"Unable to parse other netcdf files than 'ICOS Combined'. Current dataset_source: {dataset_source}."
            )

    return dataset, data_attributes


def attributes_requiring_retrieval(
    species: str,
    dobj_uri: str,
    site: str | None = None,
    station_meta: _data_parsing.Station | None = None,
) -> dict:
    """
    Create attribute details which are based on retrieval of other URIs from
    ICOS CP. This includes:
     - Details from Dobj metadata - icoscp_core.meta.get_dobj_meta(dobj_uri)
     - Details from Station metadata - _data_parsing.Station / icoscp_core.meta.get_station_meta(station_uri)
    """
    retrieved_attributes_dict = {}

    dobj_meta = _data_parsing._check_and_get_dobj_meta(dobj_uri)
    station_meta = _data_parsing._check_and_get_station_meta(site, station_meta)

    # TODO: Decide on how to include data owner details
    # Currently searching for PI details and combining into one string separated by ";"
    # Could just pick first entry instead?
    pi_details = _data_parsing.retrieve_station_staff(station_meta=station_meta, role="PI")
    data_owners = [f'{PI["firstName"]} {PI["lastName"]}' for _, PI in pi_details.iterrows()]
    data_owner_emails = [PI["email"] for _, PI in pi_details.iterrows()]

    data_owners_dict = {}
    data_owners_dict["data_owner"] = "; ".join(data_owners)
    data_owners_dict["data_owner_email"] = "; ".join(data_owner_emails)

    # References dict includes 'citation_string', 'licence_name', 'licence_info'
    references_dict = _data_parsing.retrieve_dobj_references(dobj_meta=dobj_meta)

    # Instrument dict includes 'instrument' / 'instrument1' ...
    instrument_dict = _data_parsing.retrieve_dobj_instrument(dobj_meta=dobj_meta)
    # instrument_dict = {key: value.lower() for key, value in instrument_dict.items()}

    # Measurement type dict includes 'measurement_type'
    measurement_type_dict = _data_parsing.retrieve_dobj_measurement_type(species, dobj_meta=dobj_meta)
    measurement_type_dict["measurement_type"] = measurement_type_dict["measurement_type"].lower()

    # Collect together retrieved details
    retrieved_attributes_dict.update(data_owners_dict)
    retrieved_attributes_dict.update(references_dict)
    retrieved_attributes_dict.update(instrument_dict)
    retrieved_attributes_dict.update(measurement_type_dict)

    return retrieved_attributes_dict


def create_icos_attributes(
    dataset: xr.Dataset,
    data_info: dict | pd.Series,
    data_attributes: dict,
    species: str,
    additional_data: dict = {},
    station_meta: _data_parsing.Station | None = None,
    dataset_source: str | None = None,
    keep_dataset_source_names: list | None = None,
) -> dict:
    """
    Create attributes for ICOS dataset
    """

    from openghg.util import format_inlet

    if keep_dataset_source_names is None:
        keep_dataset_source_names = ["ICOS FastTrack", "EYE-AVE-PAR", "ICOS Combined"]

    # if user_input:
    #     site = user_input["site"]
    # else:
    #     site = data_info["site"]

    if dataset_source not in keep_dataset_source_names:
        try:
            dataset_source = data_info["project_name"]
        except KeyError:
            dataset_source = "NA"
            logger.warning("Unable to read project information from ICOS data object info.")

    dobj_uri = data_info["dobj_uri"]

    # data_attributes = data.attrs
    icos_site = data_info["site"]

    attributes: dict[str, Any] = {}
    attributes["icoscp_url"] = str(dobj_uri)
    attributes["species"] = data_info["species"]
    attributes["site"] = icos_site
    attributes["dataset_source"] = dataset_source

    # TODO: See if we can add this back in - where are these details in new setup?
    # attributes["measurement_type"] = data_attributes["long_name"].lower()

    # TODO: Remove this from general attributes but make sure this is
    # included as a specific value on the appropriate variable.
    attributes["units"] = data_attributes["measurement_unit"]

    if dataset_source == "ICOS Combined":
        attrs_mapping = {
            "sampling_height": "dataset_intake_ht",
            "inlet": "dataset_intake_ht",
            "inlet_height_magl": "dataset_intake_ht",
            "station_long_name": "site_name",
            "station_latitude": "site_latitude",
            "station_longitude": "site_longitude",
            "station_height_masl": "site_elevation",
        }
        remove_attrs = list(attrs_mapping.values()) + ["dataset_intake_ht_unit"]
    else:
        attrs_mapping = {
            "sampling_height": "sampling_heights",
            "inlet": "sampling_heights",
            "inlet_height_magl": "sampling_heights",
            "station_height_masl": "altitude",
            "station_long_name": "station_name",
            "station_latitude": "latitude",
            "station_longitude": "longitude",
        }
        remove_attrs = list(attrs_mapping.values())

    # Copy across equivalent attributes from downloaded data and apply formatting
    inlet_keys = ["sampling_height", "inlet", "inlet_height_magl", "station_height_masl"]

    for new_key, current_key in attrs_mapping.items():
        value = data_attributes[current_key]
        if new_key in inlet_keys:
            value = str(value).replace(" ", "")  # could use clean_string here
            value = format_inlet(value, key_name=new_key)
        else:
            value = str(value)

        attributes[new_key] = value

    attributes["sampling_height_units"] = "metres"

    if remove_attrs:
        for key in remove_attrs:
            if key in dataset.attrs:
                dataset.attrs.pop(key)

    add_attributes = attributes_requiring_retrieval(species, dobj_uri, station_meta=station_meta)
    attributes.update(add_attributes)

    attributes.update(additional_data)

    return attributes


def create_metadata(
    site: str,
    network: str,
    attributes: dict,
    additional_data: dict = {},
    openghg_site_metadata: dict | None = None,
) -> dict:
    """
    Create metadata to be stored in the metastore for ICOS data
    """

    # Load our site metadata for a few things like the station's long_name that
    # isn't in the ICOS metadata in the way we want it at the momenet - 2023-03-20
    if openghg_site_metadata is None:
        site_info_fpath = openghg_defs.site_info_file
        openghg_site_metadata = load_json(path=site_info_fpath)

    metadata = {}

    try:
        site_info = openghg_site_metadata[site.upper()][network]
    except KeyError:
        pass
    else:
        metadata["station_long_name"] = site_info["long_name"]
        metadata["station_latitude"] = site_info["latitude"]
        metadata["station_longitude"] = site_info["longitude"]

    # Add user defined values
    metadata["site"] = site

    # Add relevant values from created attributes
    metadata["dataset_source"] = attributes["dataset_source"]
    metadata["species"] = attributes["species"]

    metadata.update(additional_data)

    return metadata


def _retrieve_remote(
    site: str,
    data_level: int,
    species: str | list | None = None,
    inlet: str | None = None,
    sampling_height: str | None = None,
    dataset_source: str | None = None,
    update_mismatch: str = "never",
) -> list[MetadataAndData] | None:
    """Retrieve ICOS data from the ICOS Carbon Portal and standardise it into
    a format expected by OpenGHG. A dictionary of metadata and Datasets

    Args:
        site: Site code
        data_level: ICOS data level (1, 2)
        - Data level 1: Near Real Time Data (NRT) or Internal Work data (IW).
        - Data level 2: The final quality checked ICOS RI data set, published by the CFs,
                        to be distributed through the Carbon Portal.
                        This level is the ICOS-data product and free available for users.
        See https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone
        species: Species name
        inlet: Height of the inlet for sampling in metres.
        sampling_height: Alias for inlet
        dataset_source: Dataset source name, for example ICOS, InGOS, European ObsPack. Specify "ICOS
            Combined" here in order to retrieve the combined timeseries including all Obspack and ICOS
            data (e.g. https://doi.org/10.18160/0HYS-FF7X)
        update_mismatch: This determines how mismatches between the "metadata" derived from
            stored data and "attributes" derived from ICOS Header are handled.
            This includes the options:
                - "never" - don't update mismatches and raise an AttrMismatchError
                - "from_source" / "attributes" - update mismatches based on attributes from ICOS Header
                - "from_definition" / "metadata" - update mismatches based on input metadata
    Returns:
        dict or None: Dictionary of processed data and metadata if found
    """

    import re
    from openghg.standardise.meta import assign_attributes
    from openghg.util import format_data_level
    from openghg.retrieve.icos import _queries, _stations

    if species is None:
        species = ["CO", "CO2", "CH4"]

    if not isinstance(species, list):
        species = [species]

    if sampling_height and inlet is None:
        inlet = sampling_height
    elif sampling_height and inlet:
        logger.warning(f"Both sampling height and inlet specified. Using inlet value of {inlet}")

    # TODO: Check: does species need to go through synomyms and upper?
    species_upper = [s.upper() for s in species]

    # TODO: BIG Decide whether this spec_label search is needed in the same way with new interface
    # - For "ICOS FastTrack" - "FastTrack" is included in spec_label name and species in "species" column
    # - For "EYE-AVE-PAR" - "EYE-AVE-PAR" is not in spec_label but is in file_name and species in "species" column
    # - For "ICOS Combined" - is this always European ObsPack?
    #  - project_name includes "euroObspack" but does not always align with spec_label including "ObsPack"

    # Want to create `spec_label` regex based on all relevant species
    # Annoyingly FastTrack and EYE-AVE-PAR data don't have the species anywhere in the data_pids dataframe
    # so we need to handle these cases separately.
    if dataset_source == "ICOS FastTrack":
        search_str = "FastTrack"
    elif dataset_source == "EYE-AVE-PAR":
        search_str = "GHG"
    else:
        # For this see https://stackoverflow.com/a/55335207
        # Need to make sure this is a raw string and that all \ characters have been escaped.
        search_str = rf"\\b(?:{'|'.join(map(re.escape, species_upper))})\\b"

    # Additional filters to apply
    # - If we want the combined .nc file we search for "Obspack"
    # - Otherwise filter out any data that contains "Obspack" or "csv" in the specLabel
    # - Also filter out some drought files which cause trouble being read in
    if dataset_source == "ICOS Combined":
        spec_label = [search_str, "Obspack"]
        spec_label_include = [True, True]
    else:
        spec_label = [search_str, "Obspack", "csv"]
        spec_label_include = [True, False, False]
        # filtered_sources = data_pids[
        #     data_pids["specLabel"].str.contains(search_str)
        #     & ~data_pids["specLabel"].str.contains("Obspack")
        #     & ~data_pids["specLabel"].str.contains("csv")
        #     & ~data_pids["station"].str.contains("ATMO_")
        # ]

    if dataset_source == "EYE-AVE-PAR":
        project = None
        filename_str = "EYE-AVE-PAR"
    else:
        project = ["icos", "euroObspack"]
        filename_str = None

    # TODO: ALSO: Do we need to / could filter by "ATMO_" station first?
    # Previous: For some reason they have separate station record pages that contain "ATMO_"

    data_object_info = _queries.dobj_info(
        site=site,
        data_level=data_level,
        species=species,
        inlet=inlet,
        spec_label=spec_label,
        spec_label_include=spec_label_include,
        project=project,
        filename_str=filename_str,
        format_info=True,  # May or may not need this?
    )

    # Load our site metadata for a few things like the station's long_name that
    # isn't in the ICOS metadata in the way we want it at the momenet - 2023-03-20
    site_info_fpath = openghg_defs.site_info_file
    openghg_site_metadata = load_json(path=site_info_fpath)

    station_meta = _stations.retrieve_station_meta(site)

    keep_dataset_source_names = ["ICOS FastTrack", "EYE-AVE-PAR", "ICOS Combined"]

    # user_input = {
    #     "site": site,
    #     "data_level": data_level,
    #     "inlet": inlet,
    #     "sampling_height": sampling_height,
    #     "species": species,
    # }

    standardised_data: dict[str, dict] = {}

    for n, data_info in data_object_info.iterrows():
        dobj_uri = data_info["dobj_uri"]
        logger.info(f"Retrieving {dobj_uri}...")

        # data = get_icos_data(row)
        # print(data)

        dataset, data_attributes = retrieve_and_parse_icos_data(data_info, dataset_source=dataset_source)

        # Apply ICOS flags - O, U and R are all valid data, set mf to nan for everything else
        dataset = dataset.where(dataset["flag"].isin(["O", "U", "R"]), drop=True)

        network = "ICOS"

        # Add ICOS in directly here for now
        additional_data = {}
        additional_data["network"] = network
        additional_data["data_type"] = "surface"
        additional_data["data_source"] = "icoscp"
        additional_data["source_format"] = "icos"
        # additional_data["icos_data_level"] = str(data_level)
        additional_data["data_level"] = format_data_level(data_level)

        species_label = data_info["species"]

        attributes = create_icos_attributes(
            dataset,
            data_info,
            data_attributes,
            species_label,
            additional_data,
            station_meta=station_meta,
            dataset_source=dataset_source,
            keep_dataset_source_names=keep_dataset_source_names,
        )

        dataset.attrs.update(attributes)

        metadata = create_metadata(
            site, network, attributes, additional_data, openghg_site_metadata=openghg_site_metadata
        )

        # So there isn't an easy way of getting a hash of a Dataset, can we do something
        # simple here we can compare data that's being added? Then we'll be able to make sure
        # ObsSurface.store_data won't accept data it's already seen
        data_key = f"key-{n}"

        standardised_data[data_key] = {
            "metadata": metadata,
            "data": dataset,
            "attributes": attributes,
        }

    standardised_data = dataset_formatter(data=standardised_data)
    standardised_data = assign_attributes(data=standardised_data, update_mismatch=update_mismatch)

    standardised_data_list = convert_to_list_of_metadata_and_data(standardised_data)
    align_metadata_attributes(
        data=standardised_data_list, update_mismatch=update_mismatch, data_type="surface"
    )

    return standardised_data_list


def _retrieve_remote_dobj(
    site: str,
    data_level: int,
    species: str | list | None = None,
    inlet: str | None = None,
    sampling_height: str | None = None,
    dataset_source: str | None = None,
    update_mismatch: str = "never",
) -> list[MetadataAndData] | None:
    """Retrieve ICOS data from the ICOS Carbon Portal and standardise it into
    a format expected by OpenGHG. A dictionary of metadata and Datasets

    Args:
        site: Site code
        data_level: ICOS data level (1, 2)
        - Data level 1: Near Real Time Data (NRT) or Internal Work data (IW).
        - Data level 2: The final quality checked ICOS RI data set, published by the CFs,
                        to be distributed through the Carbon Portal.
                        This level is the ICOS-data product and free available for users.
        See https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone
        species: Species name
        inlet: Height of the inlet for sampling in metres.
        sampling_height: Alias for inlet
        dataset_source: Dataset source name, for example ICOS, InGOS, European ObsPack. Specify "ICOS
            Combined" here in order to retrieve the combined timeseries including all Obspack and ICOS
            data (e.g. https://doi.org/10.18160/0HYS-FF7X)
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
    from openghg.util import format_inlet, format_data_level, load_internal_json
    from pandas import to_datetime, Timedelta

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
    # If we want the combined .nc file we search for Obspack
    # Otherwise filter out any data that contains "Obspack" or "csv" in the specLabel
    # Also filter out some drought files which cause trouble being read in
    # For some reason they have separate station record pages that contain "ATMO_"
    if dataset_source == "ICOS Combined":
        filtered_sources = data_pids[
            data_pids["specLabel"].str.contains(search_str) & data_pids["specLabel"].str.contains("Obspack")
        ]
    else:
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
        dobj.dateTimeConvert = False
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
        elif dataset_source == "ICOS Combined":
            dobj_dataset_source = "ICOS Combined"
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

        if dataset_source == "ICOS Combined":
            species_info = next(i for i in col_data if i["label"] == "value")
        else:
            not_the_species = {"TIMESTAMP", "Flag", "NbPoints", "Stdev"}
            species_info = next(i for i in col_data if i["label"] not in not_the_species)

        measurement_type = species_info["valueType"]["self"]["label"].lower()
        spec = measurement_type.split()[0]
        attributes["species"] = spec
        acq_data = specific_info["acquisition"]
        station_data = acq_data["station"]

        # Hack to convert units for Obspack nc files
        if dataset_source == "ICOS Combined":
            if spec == "co2":
                units = "ppm"
            else:
                units = "ppb"

            attributes_data = load_internal_json("attributes.json")
            unit_interpret = attributes_data["unit_interpret"]
            unit_value = float(unit_interpret.get(units, "1"))

            dataframe["value"] = dataframe["value"] / unit_value
            dataframe["value_std_dev"] = dataframe["value_std_dev"] / unit_value
            dataframe["icos_LTR"] = dataframe["icos_LTR"] / unit_value
            dataframe["icos_SMR"] = dataframe["icos_SMR"] / unit_value
            dataframe["icos_STTB"] = dataframe["icos_STTB"] / unit_value

        else:
            units = species_info["valueType"]["unit"].lower()

        to_store: dict[str, Any] = {}

        if dataset_source == "ICOS Combined":
            to_store["instrument"] = "NA"
            to_store["instrument_data"] = "NA"
        else:
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

        dataframe.columns = [x.lower() for x in dataframe.columns]

        # If there is a stdev column, replace missing values with nans
        # Then rename columns
        if dataset_source == "ICOS Combined":
            dataframe["value_std_dev"] = dataframe["value_std_dev"].where(dataframe["value_std_dev"] >= 0)
            rename_cols = {
                "value": attributes["species"],
                "qc_flag": "flag",
                "value_std_dev": spec + " variability",
                "icos_ltr": spec + " repeatability",
            }
        else:
            try:
                dataframe["stdev"] = dataframe["stdev"].where(dataframe["stdev"] >= 0)
                rename_cols = {
                    "timestamp": "time",
                    "stdev": spec + " variability",
                    "nbpoints": spec + " number_of_observations",
                }
            except KeyError:
                rename_cols = {
                    "timestamp": "time",
                    "nbpoints": spec + " number_of_observations",
                }

        dataframe = dataframe.rename(columns=rename_cols)

        # Apply ICOS flags - O, U and R are all valid data, set mf to nan for everything else
        dataframe[spec] = dataframe[spec].where(dataframe["flag"].isin(["O", "U", "R"]))
        dataframe = dataframe[dataframe[spec].notna()]

        if not dataframe.index.is_monotonic_increasing:
            dataframe = dataframe.sort_index()

        dataframe = dataframe.set_index("time")

        dataframe.index = to_datetime(dataframe.index, format="%Y-%m-%d %H:%M:%S")
        if dataset_source == "ICOS Combined":
            dataframe.index = dataframe.index - Timedelta(minutes=30)

        dataset = dataframe.to_xarray()
        dataset.attrs.update(attributes)

        if dataset_source == "ICOS Combined":
            dataset[spec + " repeatability"].attrs[
                "comment"
            ] = "ICOS LTR as defined by Yver Kwok et al., 2015, doi:10.5194/amt-8-3867-2015"

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

    standardised_data_list = convert_to_list_of_metadata_and_data(standardised_data)
    align_metadata_attributes(
        data=standardised_data_list, update_mismatch=update_mismatch, data_type="surface"
    )

    return standardised_data_list


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
