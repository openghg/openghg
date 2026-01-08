""" """

import logging
from collections import defaultdict
import io
import re
from zipfile import ZipFile
from typing import cast, Any

from icoscp_core.icos import data, meta
from icoscp_core.metacore import References
import numpy as np
import pandas as pd
import xarray as xr

from ._queries import attrs_query, icos_format_info

logger = logging.getLogger("openghg.retrieve")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def camel_to_snake(s: str) -> str:
    """Convert camelCase and PascalCase to snake_case."""
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()


def retrieve_references_object(dobj_uri: str) -> References:
    """
    Retrieve the References object from ICOS CP for a data object (Dobj).
    Args:
        dobj_uri: Data object URI details
    Returns:
        icoscp_core.metacore.References: Object related to ICOS Dobj
    """
    dobj_meta = meta.get_dobj_meta(dobj_uri)
    return dobj_meta.references


def retrieve_dobj_references(dobj_uri: str) -> dict:
    """
    Retrieve key reference details for an ICOS data object as a dictionary.

    Currently includes keys:
        - citation - Citation string
        - licence_name - Name of the licence associated with the data (when available)
        - licence_info - URL for the licence itself (when available)

    Args:
        dobj_uri: Data object URI details
    Returns:
        dict: Dictionary of key reference details for the ICOS data object
    """

    references = retrieve_references_object(dobj_uri)

    references_dict = {}
    references_dict["citation"] = references.citationString

    licence = references.licence
    if licence is not None:
        references_dict["licence_name"] = licence.name
        references_dict["licence_info"] = licence.url
    else:
        logger.warning("No licence details available from references for ICOS data object")

    return references_dict


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
    """ """
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
    """ """
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
    """ """
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
    """ """
    # find format
    fmt = icos_format_info().loc[data_info["spec_label"]].fmt
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
