from collections import defaultdict
import io
import re
from zipfile import ZipFile

from icoscp_core.icos import data, meta
import numpy as np
import pandas as pd
import xarray as xr

from ._queries import attrs_query


def camel_to_snake(s: str) -> str:
    """Convert camelCase and PascalCase to snake_case."""
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()


def get_data_attrs(dobj_uri, species) -> dict[str, dict]:
    res = meta.sparql_select(attrs_query(dobj_uri))

    # TODO mypy fixes... basically anything ending with
    # _label has .value and otherwise it has .uri, but this
    # is due to our particular query...
    attrs = defaultdict(dict)
    for b in res.bindings:
        data_var = b["col_label"].value
        key = b["p_label"].value.replace(" ", "_")

        # this query gets flag columns from other species, so skip these
        if key.startswith("is_a_quality_flag") and species not in b["o_label"].value:
            continue
        elif key == "value_format":
            val = b["o"].uri.split("/")[-1]
        else:
            val = b["o_label"].value

        attrs[data_var][key] = val

        if "unit" in b:
            attrs[data_var]["unit"] = b["unit"].value

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
    """Get zipped data from ICOS CP.

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
def get_icos_text_header(icos_text):
    """Parse header from ICOS text file.

    This applies to data parsed using `get_icos_text_file`.
    """
    header_lines = [
        line.removesuffix(",").removeprefix("#").strip()
        for line in icos_text.split("\n")
        if line.startswith("#")
    ]
    header_lines = [line for line in header_lines if line]
    metadata = {}
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
        if col in attrs and (vtype := attrs[col].get("value_format")):
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

    df = pd.read_csv(io.StringIO(icos_text), skiprows=skiprows, sep=";", usecols=usecols)

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

    # skip first column becaues it is just the site name
    usecols = header["columns"][1:]
    parse_dates = ["SamplingStart", "SamplingEnd"]
    df = pd.read_csv(
        io.StringIO(icos_text), skiprows=skiprows, sep=";", usecols=usecols, parse_dates=parse_dates
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
