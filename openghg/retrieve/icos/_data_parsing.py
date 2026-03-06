""" """

import logging
from collections import defaultdict
import io
import re
import numpy as np
import pandas as pd
import xarray as xr
from zipfile import ZipFile
from typing import cast, Any
from dataclasses import asdict

from icoscp_core.icos import data, meta
from icoscp_core.metacore import DataObject, References, StationTimeSeriesMeta
from icoscp_core.metaclient import Station

from ._queries import attrs_query, icos_format_info

logger = logging.getLogger("openghg.retrieve.icos")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def camel_to_snake(s: str) -> str:
    """Convert camelCase and PascalCase to snake_case."""
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()


def _retrieve_dobj_meta(dobj_uri: str) -> DataObject:
    """
    Retrieve the Data Object metadata. This is a thin wrapper for the `icoscp_core.meta.get_dobj_meta`
    function.
    Args:
        dobj_uri: Data object URI details.
    Returns:
        icoscp_core.metacore.DataObject : Data Object metadata from ICOS CP
    """
    dobj_meta = meta.get_dobj_meta(dobj_uri)
    return dobj_meta


def _check_and_get_dobj_meta(dobj_uri: str | None = None, dobj_meta: DataObject | None = None) -> DataObject:
    """
    Check if we have already downloaded the dobj_meta DataObject and retrieve this otherwise.
    This is to allow dobj_meta to be downloaded and used multiple times without
    needing to make numerous calls to ICOS CP.
    Args:
        dobj_uri: Data object URI details. Either this of dobj_meta must be specified.
        dobj_meta: Retrieved DataObject for Dobj metadata
    Returns:
        icoscp_core.metacore.DataObject : Data Object metadata
    """
    if dobj_meta is None and dobj_uri is not None:
        dobj_meta = _retrieve_dobj_meta(dobj_uri)

    if dobj_meta is None:
        msg = "Either dobj_uri or dobj_meta DataObject must be specified to retrieve Data Object details (e.g. references, instrument, measurement details)."
        logger.exception(msg)
        raise ValueError(msg)

    return dobj_meta


def _retrieve_references_object(
    dobj_uri: str | None = None, dobj_meta: DataObject | None = None
) -> References:
    """
    Retrieve the References object from ICOS CP for a data object (Dobj).
    Args:
        dobj_uri: Data object URI details. Either this of dobj_meta must be specified.
        dobj_meta: Retrieved DataObject for Dobj metadata
    Returns:
        icoscp_core.metacore.References: Object related to ICOS Dobj
    """
    dobj_meta = _check_and_get_dobj_meta(dobj_uri, dobj_meta)
    return dobj_meta.references


def retrieve_dobj_references(dobj_uri: str | None = None, dobj_meta: DataObject | None = None) -> dict:
    """
    Retrieve key reference details for an ICOS data object as a dictionary.

    Currently includes keys:
        - citation_string - Citation string
        - licence_name - Name of the licence associated with the data
        - licence_info - URL for the licence itself

    Args:
        dobj_uri: Data object URI details. Either this of dobj_meta must be specified.
        dobj_meta: Retrieved DataObject for Dobj metadata
    Returns:
        dict: Dictionary of key reference details for the ICOS data object
    """

    references = _retrieve_references_object(dobj_uri, dobj_meta)

    references_dict = {}
    references_dict["citation_string"] = references.citationString

    licence = references.licence
    if licence is not None:
        references_dict["licence_name"] = licence.name
        references_dict["licence_info"] = licence.url
    else:
        logger.warning("No licence details available from references for ICOS data object")

    return references_dict


def _retrieve_specific_info_object(
    dobj_uri: str | None = None, dobj_meta: DataObject | None = None
) -> StationTimeSeriesMeta:
    """
    Retrieve the specific info details (StationTimeSeriesMeta) from ICOS CP for a data object (Dobj).
    Args:
        dobj_uri: Data object URI details. Either this of dobj_meta must be specified.
        dobj_meta: Retrieved DataObject for Dobj metadata
    Returns:
        icoscp_core.metacore.StationTimeSeriesMeta: Object related to specific info stored for the ICOS CP data object
    """
    dobj_meta = _check_and_get_dobj_meta(dobj_uri, dobj_meta)

    specific_info = dobj_meta.specificInfo

    if not isinstance(specific_info, StationTimeSeriesMeta):
        raise ValueError("Unable to parse specific_info metadata (wrong type)")

    return specific_info


def retrieve_dobj_instrument(dobj_uri: str | None = None, dobj_meta: DataObject | None = None) -> dict:
    """
    Retrieve key instrument details for an ICOS data object as a dictionary.

    Currently includes keys:
        - If multiple instruments, the included keys take the form "instrument1", "instrument2" etc.
        - Otherwise, this included key is "instrument"

    Args:
        dobj_uri: Data object URI details. Either this of dobj_meta must be specified.
        dobj_meta: Retrieved DataObject for Dobj metadata
    Returns:
        dict: Dictionary of instrument details for the ICOS data object
    """
    specific_info_meta = _retrieve_specific_info_object(dobj_uri, dobj_meta)
    acquisition_meta = specific_info_meta.acquisition
    instrument = acquisition_meta.instrument

    instrument_dict: dict[str, str | list] = {}

    if instrument is None:
        logger.warning(f"Unable to determine instrument for {dobj_uri}")
        instrument_dict["instrument"] = "NA"
        instrument_dict["instrument_data"] = "NA"
    elif isinstance(instrument, list):
        instrument_dict["instrument"] = "multiple"

        instrument_name_details = []
        for item in instrument:
            label = item.label
            uri = item.uri
            instrument_name_details.extend([label, uri])
        instrument_dict["instrument_data"] = instrument_name_details
    else:
        if instrument.label is not None:
            instrument_dict["instrument"] = instrument.label
            instrument_dict["instrument_data"] = [instrument.label, instrument.uri]
        else:
            instrument_dict["instrument"] = "NA"
            instrument_dict["instrument_data"] = "NA"

    return instrument_dict


def retrieve_dobj_measurement_type(
    species_label: str, dobj_uri: str | None = None, dobj_meta: DataObject | None = None
) -> dict:
    """
    Retrieve key measurement type details for an ICOS data object as a dictionary.

    Currently includes keys:
        - "measurement_type"
          - contains the measurement details based on the species label within the columns
          - if the species label is not found for the columns this includes "NA"

    Args:
        dobj_uri: Data object URI details. Either this of dobj_meta must be specified.
        dobj_meta: Retrieved DataObject for Dobj metadata
    Returns:
        dict: Dictionary of measurement type details for the ICOS data object
    """
    specific_info_meta = _retrieve_specific_info_object(dobj_uri, dobj_meta)
    columns_data = specific_info_meta.columns

    measurement_dict = {}

    if isinstance(columns_data, list):
        column_labels = [column.label for column in columns_data]
    else:
        logger.warning(
            "Unable to find measurement type in ICOS data. No columns data available specific_info_meta."
        )
        measurement_dict["measurement_type"] = "NA"
        return measurement_dict

    try:
        index = column_labels.index(species_label)
    except (TypeError, ValueError):
        logger.warning(f"Unable to find measurement type in ICOS data for {species_label}")
        measurement_dict["measurement_type"] = "NA"
        return measurement_dict

    species_column = columns_data[index]
    label = species_column.valueType.self.label

    if label is not None:
        measurement_dict["measurement_type"] = label
    else:
        logger.warning(f"Unable to find measurement type in ICOS data for {species_label}")
        measurement_dict["measurement_type"] = "NA"

    return measurement_dict


def _check_and_get_station_meta(
    site: str | None = None, station_meta: Station | None = None, atmospheric: bool = True
) -> Station:
    """
    Check if we have already downloaded the station_meta Station object and retrieve this otherwise
    using site and atmospheric.
    This is to allow station_meta to be downloaded and used multiple times without
    needing to make numerous calls to ICOS CP.
    Args:
        site: ICOS site ID e.g. "BIK". Either this is station_meta must be specified.
        station_meta: Station object (typically from retrieve_station_meta).
        atmospheric: Whether to initially filter station list to only include atmospheric stations.
    Returns:
        icoscp_core.metaclient.Station : Station metadata
    """

    from openghg.retrieve.icos._stations import retrieve_station_meta

    if site and station_meta is None:
        station_meta = retrieve_station_meta(site, atmospheric)

    if station_meta is None:
        msg = "Either site or station_meta must be specified to retrieve staff details."
        logger.exception(msg)
        raise ValueError(msg)

    return station_meta


def retrieve_station_staff(
    site: str | None = None,
    station_meta: Station | None = None,
    role: str | None = None,
    atmospheric: bool = True,
) -> pd.DataFrame:
    """
    Find details of staff associated with a particular station. This can be filtered by the role.
    Option to search by site or to supply the station meta (Station object) directly
    One of site or station_meta must be specified.

    Args:
        site: ICOS site ID e.g. "BIK". Either this is station_meta must be specified.
        station_meta: Station object (typically from retrieve_station_meta).
        role: Name of staff role to filter by. This (non-exhaustively) includes:
         - "Principal Investigator" (can use "PI" for short)
         - "Administrator"
         - "Engineer"
        atmospheric: Whether to initially filter station list to only include atmospheric stations.
    Returns:
        pandas.DataFrame: Summarised details for staff members associated with the site
            (extracted from meta.get_station_meta(uri))
    """

    station_meta = _check_and_get_station_meta(site, station_meta, atmospheric)

    station_staff = station_meta.staff

    staff_list = []
    for staff in station_staff:
        # Want to combine details for both individual person and role for the site
        overall_dict = {}
        person_dict = asdict(staff.person)
        role_dict = asdict(staff.role.role)

        # Two 'uri' details (one for person and one for role) so ensure these are distinct
        role_dict["role_uri"] = role_dict.pop("uri")

        # Include just the 'uri' from the `staff.person.self` entry
        person_dict["staff_uri"] = staff.person.self.uri
        person_dict.pop("self")

        overall_dict.update(person_dict)
        overall_dict.update(role_dict)
        staff_list.append(overall_dict)

    staff_df = pd.DataFrame(staff_list)

    if role is not None:
        if role == "PI":
            role = "Principal Investigator"

        role_filter = staff_df["label"] == role
        staff_df = staff_df[role_filter]

    return staff_df


def get_data_attrs(dobj_uri: str, species: str) -> dict[str, dict]:
    """
    Get the attributes associated with a data URI (Uniform Resource Identifier).
    See openghg.retrieve.icos._queries.attrs_query for details of
    how this query is constructed and what is returned.
    This creates the "dtype", "long_name" and (where applicable)
    "units" attributes for individual variables.

    Args:
        dobj_uri: Data object URI details
        species: Particular species of interest. This will be used
            to select details just related to that species if the data
            is related to more than one species.
    Returns:
        dict: Dictionary for the variables and associated attributes
    """

    from icoscp_core.sparql import BoundLiteral, BoundUri

    res = meta.sparql_select(attrs_query(dobj_uri))

    # mypy casting: basically anything ending with
    # _label has .value and otherwise it has .uri, but this
    # is due to our particular query.
    # As we know this we can cast to each type as appropriate
    attrs: dict = defaultdict(dict)
    for b in res.bindings:
        col_label = cast(BoundLiteral, b["col_label"])
        p_label = cast(BoundLiteral, b["p_label"])

        data_var = col_label.value
        key = p_label.value.replace(" ", "_")

        # the query might get flag columns from other species, so skip these
        o_label = cast(BoundLiteral, b["o_label"])
        o = cast(BoundUri, b["o"])
        if key.startswith("is_a_quality_flag") and species not in o_label.value:
            continue
        elif key == "value_format":
            key = "dtype"
            val = o.uri.split("/")[-1]
        else:
            val = o_label.value

        if key == "value_type":
            key = "long_name"

        attrs[data_var][key] = val

        if "unit" in b:
            unit = cast(BoundLiteral, b["unit"])
            attrs[data_var]["units"] = unit.value

    return attrs


# get formats using:
# `list(icos_format_info().format.str.split("/").str[-1])`
icos_formats = [
    "asciiWdcggTimeSer",
    "asciiAtcTimeSer",
    "ingosRar",
    "asciiAtcProductTimeSer",
    "csvWithIso8601tsFirstCol",
    "netcdfTimeSeries",
    "asciiAtcFlaskTimeSer",
]


def _retrieve_dobj_format(data_info: dict | pd.Series | None = None, spec_label: str | None = None) -> Any:
    """
    Retrieve the format for an ICOS data object. For this we will need to retrieve the overall
    ICOS format information and use the spec_label to identify the format.
    Either the full data_info (best extracted using the `openghg.retrieve.icos._queries.dobj_info()` function)
    or the spec_label string will need to be specified for this.

    Args:
        data_info: ICOS data object information returned from _queries.dobj_info()
        spec_label: Specific label for the data object
    Returns:
        str: Format value for the data object
    """
    if spec_label is None and data_info is not None:
        spec_label = data_info["spec_label"]

    if spec_label is None:
        msg = "Unable to retrieve format for ICOS data object. Please specify either data_info or spec_label directly."
        logger.exception(msg)
        raise ValueError(msg)

    format_info_row = icos_format_info().loc[spec_label]
    dobj_format = format_info_row["fmt"]

    return dobj_format


def get_icos_text_file(dobj_uri: str) -> str:
    """
    Files on ICOS Carbon Portal can sometimes be stored as netcdf or zipped text file format.
    This function is to retrieve zipped text data.

    This works for the formats:
    - ICOS ATC time series
    - ICOS ATC Flask time series
    """
    _, resp = data.get_file_stream(dobj_uri)

    with ZipFile(io.BytesIO(resp.read()), "r") as z:
        with z.open(z.infolist()[0].filename) as f:
            text = f.read().decode("utf-8")

    return text


def get_icos_nc_file(dobj_uri: str) -> xr.Dataset:
    """Get netCDF data from ICOS CP.

    TODO: this requires h5py... there is a workaround if
    you just have netcdf4: https://github.com/pydata/xarray/issues/1075
    """
    _, resp = data.get_file_stream(dobj_uri)

    ds = xr.open_dataset(io.BytesIO(resp.read()))

    return ds


# TODO: convert columns/keys to snake case since some of the names in the
# data file and the header comments are inconsistent
def get_icos_text_header(icos_text: str) -> dict:
    """
    Parse header from ICOS text file.

    This applies to data parsed using `get_icos_text_file`.
    """
    header_lines = [
        line.removesuffix(",").removeprefix("#").strip()
        for line in icos_text.split("\n")
        if line.startswith("#")
    ]
    header_lines = [line for line in header_lines if line]
    metadata: dict[str, Any] = {}
    comment_line = -1
    for i, line in enumerate(header_lines):
        if "COMMENT" in line:
            comment_line = i
            break
        k = line.split(":")[0]
        v = line.removeprefix(k + ":").strip()
        k = k.strip().lower()
        k = k.split("/")[0]
        k = k.replace(" ", "_")
        metadata[k] = v
    metadata["comment"] = [line.removeprefix("-").strip() for line in header_lines[comment_line + 1 : -2]]
    metadata["columns"] = header_lines[-1].split(";")
    return metadata


def get_attrs_from_header(header: dict) -> dict:
    """
    This function is to allow the header lines at the top of the text file format
    to be parsed and details extracted.

    This zipped text file data data can be retrieved using `get_icos_text_file()`
    and parsed into a dictionary using `get_icos_text_header()` function.

    Args:
        header: Header details in dictionary format from ICOS text file.
            Matches to output from `get_icos_text_header()` function.
    Returns:
        dict: Variable attributes based on the header
    """
    attrs = defaultdict(list)
    for comment in header["comment"]:
        if (col := comment.split(":")[0]) in header["columns"]:
            val = comment.removeprefix(col + ":").strip()
            attrs[col].append(val)
        elif (col := comment.split()[0]) in header["columns"]:
            val = comment.removeprefix(col).strip()
            attrs[col].append(val)
    result = {}
    for k, v in attrs.items():
        if len(v) == 1:
            result[k] = v[0]
        else:
            result[k] = v[0] + " " + "; ".join(v[1:])
    return result


def get_full_attrs(dobj_uri: str, species: str, header: dict) -> dict:
    """
    Combines the variable attributes from the sparql query with the variable attributes from the text file header.

    Args:
        dobj_uri: Data object URI details
        species: Particular species of interest. This will be used
            to select details just related to that species if the data
            is related to more than one species.
        header: Dictionary for variable attributes based on the header
    Returns:
        dict: Dictionary for the variables and associated attributes
    """
    attrs = get_data_attrs(dobj_uri, species)
    for k, v in get_attrs_from_header(header).items():
        attrs[k]["description"] = v
    return attrs


def parse_date_columns(df: pd.DataFrame) -> tuple[list[str], pd.Series]:
    """Convert multiple date/time columns to single datetime column.

    This returns the list of columns used to for the datetime column, along
    with the datetime Series itself.
    """
    possible_cols = ["year", "month", "day", "hour", "minute", "second"]
    found_cols = [col for col in df.columns if col.lower() in possible_cols]

    # need lower case for pd.to_datetime
    date_df = df[found_cols].rename(columns={col: col.lower() for col in df.columns})
    return found_cols, pd.to_datetime(date_df)


def icos_format_to_dtype(value_format: str) -> np.dtype | None:
    """Convert ICOS-specific format strings into NumPy data types.

    Returns NumPy data types of ICOS-specific format strings.
    """
    # adapted from icoscp_core.cpb._type_post_process
    icos_format_to_dtype_dict = {
        "bmpChar": np.dtype("U1"),
        "iso8601date": np.dtype("datetime64[D]"),
        "etcDate": np.dtype("datetime64[D]"),
        "iso8601month": np.dtype("datetime64[M]"),
        "iso8601timeOfDay": np.dtype("timedelta64[s]"),
        "iso8601dateTime": np.dtype("datetime64[ms]"),
        "isoLikeLocalDateTime": np.dtype("datetime64[ms]"),
        "etcLocalDateTime": np.dtype("datetime64[ms]"),
    }
    if value_format in icos_format_to_dtype_dict:
        return icos_format_to_dtype_dict[value_format]

    try:
        return np.dtype(value_format)
    except TypeError:
        return None


def dtypes_dict(columns: list[str], attrs: dict) -> dict[str, np.dtype]:
    """Get dict for converting dtypes.

    TODO: Stdev doesn't seem to have a value type so we should probably
    make it have the same type as the obs..

    In general, if the obs are float32, it would be good to conver the errors/
    uncertainties to the same precision
    """
    dtypes = {}
    for col in columns:
        if col in attrs and (vtype := attrs[col].get("dtype")):
            if (dtype := icos_format_to_dtype(vtype)) is not None:
                dtypes[col] = dtype
    return dtypes


def parse_icos_atc_time_series_text(
    icos_text: str, species: str | None = None, attrs: dict | None = None
) -> pd.DataFrame:
    """Parse ICOS ATC time series data from text.

    This applies to ICOS text files with the format:
    http://meta.icos-cp.eu/ontologies/cpmeta/asciiAtcProductTimeSer

    The text format is: "Semicolon-separated ASCII, with #-prefixed multi-line header.
    Timestamp is represented my multiple columns (year, month, day, etc.)""
    """
    header = get_icos_text_header(icos_text)
    skiprows = int(header["header_lines"]) - 1

    # skip first column becaues it is just the site name, and skip DecimalDate
    usecols = [col for col in header["columns"][1:] if col != "DecimalDate"]

    na_values = ["-999.99", "-9.99"]  # these are used in some ICOS files

    df = pd.read_csv(io.StringIO(icos_text), skiprows=skiprows, sep=";", usecols=usecols, na_values=na_values)

    # make time index
    date_cols, times = parse_date_columns(df)
    df["time"] = times
    df = df.drop(columns=date_cols)
    df = df.set_index("time")

    # drop na
    drop_na_cols = [col for col in df.columns if col in [species, "Stdev", "NbPoints"]]
    df = df.dropna(subset=drop_na_cols)

    # try to get dtypes
    dtypes = dtypes_dict(df.columns.to_list(), attrs) if attrs is not None else None
    if dtypes is not None:
        # if Stdev dtype not specified, make it the same as the mole fraction column
        if species in dtypes and "Stdev" not in dtypes:
            dtypes["Stdev"] = dtypes[species]

        df = df.astype(dtypes)

    return df


def parse_icos_atc_flask_time_series_text(
    icos_text: str, species: str | None = None, attrs: dict | None = None
) -> pd.DataFrame:
    """Parse ICOS ATC time series data from text.

    This applies to ICOS text files with the format:
    http://meta.icos-cp.eu/ontologies/cpmeta/asciiAtcFlaskTimeSer

    The text format is: "Semicolon-separated ASCII, with #-prefixed multi-line header.
    ISO-8601 standard is used for time stamps, no synthetic 'TIMESTAMP' column is needed."
    """
    header = get_icos_text_header(icos_text)
    skiprows = int(header["header_lines"]) - 1

    usecols = header["columns"][1:]  # skip first column becaues it is just the site name
    parse_dates = ["SamplingStart", "SamplingEnd"]
    na_values = ["-999.99", "-9.99"]  # these are used in some ICOS files
    df = pd.read_csv(
        io.StringIO(icos_text),
        skiprows=skiprows,
        sep=";",
        usecols=usecols,
        parse_dates=parse_dates,
        na_values=na_values,
    )

    # convert to timezone-naive
    for col in parse_dates:
        df[col] = df[col].dt.tz_localize(None)

    # get sampling period and set index to SamplingStart
    df["SamplingPeriod"] = df["SamplingEnd"] - df["SamplingStart"]
    df = df.set_index("SamplingStart")
    df.index.name = "time"

    # drop na
    drop_na_cols = [col for col in df.columns if col in [species, "Stdev", "NbPoints"]]
    df = df.dropna(subset=drop_na_cols)

    # try to get dtypes
    dtypes = dtypes_dict(df.columns.to_list(), attrs) if attrs is not None else None
    if dtypes is not None:
        df = df.astype(dtypes)

    return df


# FULL PIPELINE
def make_icos_dataset(
    icos_df: pd.DataFrame, attrs: dict | None = None, global_attrs: dict | None = None
) -> xr.Dataset:
    """
    Converts the parsed timeseries data from a pandas DataFrame into an xarray Dataset and adds
    the attribute information for variables and global variables and attributes about the whole dataset.

    Args:
        icos_df: parsed timeseries data.
        attrs: full variables and associated attribute details.
        global_attrs: global variables and associated attributes.
    Returns:
        xr.Dataset: x array dataset of parsed timeseries data with associated attribute information.
    """
    attrs = attrs or {}
    attrs = {camel_to_snake(k): v for k, v in attrs.items()}
    icos_df.columns = [camel_to_snake(col) for col in icos_df.columns]
    ds: xr.Dataset = icos_df.to_xarray()
    for dv in ds.data_vars:
        ds[dv].attrs = attrs.get(dv, {})
    if global_attrs is not None:
        ds.attrs = global_attrs
    return ds


def get_icos_data(data_info: dict | pd.Series) -> xr.Dataset:
    """
    get_icos_data takes a dict or row from the "data query" DataFrame and produces
    a lightly formated xr.Dataset with attributes. Only works for ATC Product/Flask
    time series and netCDF time series formats. Data vars are all "snake case"
    (e.g. "nb_points" instead of "NbPoints").

    Args:
        data_info: takes a dict or row from the "data query" DataFrame.
    Returns:
        xr.Dataset: x array dataset of parsed timeseries data with associated attribute information.
    """
    # find format
    fmt = _retrieve_dobj_format(data_info)

    dobj_uri = data_info["dobj_uri"]
    species = data_info["species"]

    if fmt in ("asciiAtcFlaskTimeSer", "asciiAtcProductTimeSer"):
        icos_text = get_icos_text_file(dobj_uri)
        header = get_icos_text_header(icos_text)
        attrs = get_full_attrs(dobj_uri, species, header)

        if "Flask" in fmt:
            df = parse_icos_atc_flask_time_series_text(icos_text, species, attrs)
        else:
            df = parse_icos_atc_time_series_text(icos_text, species, attrs)

        del header["columns"]  # we're going to change the column names...
        return make_icos_dataset(df, attrs, header)

    if fmt == "netcdfTimeSeries":
        ds = get_icos_nc_file(dobj_uri)
        drop_vars = [
            "datetime",
            "time_decimal",
            "time_components",
            "solartime_components",
            "assimilation_concerns",
            "obspack_id",
            "obs_num",
            "obspack_num",
        ]
        ds = ds.drop_vars(drop_vars)
        rename_dict = {dv: camel_to_snake(str(dv)) for dv in ds.data_vars}
        rename_dict["value"] = species
        return ds.rename(rename_dict)

    raise NotImplementedError(f"Cannot parse ICOS format {fmt}.")
