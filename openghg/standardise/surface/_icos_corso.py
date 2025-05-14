from pathlib import Path
import logging
import numpy as np
import pandas as pd

from openghg.standardise.meta import dataset_formatter
from openghg.types import pathType, MetadataMissingError
from openghg.standardise.meta import assign_attributes
from openghg.util import clean_string, format_inlet, synonyms, read_header

logger = logging.getLogger("openghg.standardise.surface")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_icos_corso(
    filepath: str | Path,
    site: str,
    instrument: str,
    inlet: str | None = None,
    network: str = "ICOS",
    sampling_period: str | None = None,
    measurement_type: str | None = None,
    update_mismatch: str = "never",
    site_filepath: pathType | None = None,
    **kwargs: dict,
) -> dict:
    """
    Parse an icos files for the corso project

    Args:
        filepath: Path to file
        site: Three letter site code
        network: Network name
        instrument: Instrument name
        inlet: Optionally specify inlet height to check against filename
        sampling_period: Sampling period e.g. 2 hour: 2H, 2 minute: 2m
        measurement_type: Measurement type e.g. insitu, flask
        header_type: ICOS data file with large (40 line) header or shorter single line header
            Options: large, small
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
              - "never" - don't update mismatches and raise an AttrMismatchError
              - "from_source" / "attributes" - update mismatches based on input data (e.g. data attributes)
              - "from_definition" / "metadata" - update mismatches based on associated data (e.g. site_info.json)
        site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
    Returns:
        dict: Dictionary of gas data
    """

    input_site = clean_string(site)
    instrument = clean_string(instrument)
    network = clean_string(network)
    sampling_period = clean_string(sampling_period)
    measurement_type = clean_string(measurement_type)

    inlet = clean_string(inlet)
    inlet = format_inlet(inlet)

    if not isinstance(filepath, Path):
        filepath = Path(filepath)

    contributors = ["gns", "uheicrl", "noaa", "ucsd", "uheiiup"]
    data_flag = None
    columns_to_keep = None

    # Currently contributors name included in the filename is the direct classifier as clean_14c_data. In future this may change and thus the contributors list will be required to be updated.
    data_flag = ["clean_14c_data" for contributor_name in contributors if contributor_name in filepath.name]

    species_fname = filepath.name.split(".")[-1]
    species = synonyms(species_fname)

    rename_dict = {
        species_fname.lower(): species,
        "samplingstart": "sampling_start",
        "samplingend": "sampling_end",
        "measunc": species + "_variability",
        "estrep": species + "_repeatability",
        "nbpoints": species + "_number_of_observations",
        "stdev": species + "_variability",
        "scallink": species + "_calibration_uncertainty",
        "combunc": species + "_combined_uncertainty",
        "integrationtime": "integration_time",
        "weightedstderr": "weighted_std_err",
        "analyticalstdev": species + "_variability",
        "samplingpattern": "sampling_pattern",
    }

    if not data_flag:
        # icos_data_level = 1 data is classified as ICOS_ATC_L1_FAST_TRACK
        if "l1" in filepath.name.lower():
            site_fname = filepath.name.split("_")[-4]
            inlet_height_fname = filepath.name.split("_")[-3]

            species, site, inlet = initial_checks_and_setup(
                species=species_fname,
                input_site=input_site,
                site_fname=site_fname.lower(),
                input_inlet=inlet,
                inlet_height_fname=inlet_height_fname,
            )

            header, df = convert_icos_file_to_dataframe(filepath=filepath, rename_dict=rename_dict)

            df.columns = [str(c).lower() for c in df.columns]

            # Currently we are aware of species_fname to be "14c", "co2" and "deltao2n2"
            if species_fname.lower() == "14c":
                columns_to_keep = [
                    species,
                    "sampling_start",
                    "sampling_end",
                    species + "_variability",
                    species + "_repeatability",
                    species + "_number_of_observations",
                    "flag",
                ]

                df = df[columns_to_keep]

                df = set_time_as_dataframe_index(dataframe=df)

            else:
                columns_to_keep = [
                    species,
                    "sampling_start",
                    "sampling_end",
                    species + "_variability",
                    species + "_repeatability",
                    species + "_calibration_uncertainty",
                    species + "_combined_uncertainty",
                    "flag",
                ]

                df = df[columns_to_keep]

                df = set_time_as_dataframe_index(dataframe=df)

        # icos_data_level = 2 data is classified as ICOS_ATC_L2
        elif "l2" in filepath.name.lower():
            if "flask" in filepath.name.lower():
                site_fname = filepath.name.split("_")[-4]
                inlet_height_fname = filepath.name.split("_")[-3]

                species, site, inlet = initial_checks_and_setup(
                    species=species_fname,
                    input_site=input_site,
                    site_fname=site_fname,
                    input_inlet=inlet,
                    inlet_height_fname=inlet_height_fname,
                )

                header, df = convert_icos_file_to_dataframe(filepath=filepath, rename_dict=rename_dict)

                if species_fname.lower() == "14c":
                    columns_to_keep = [
                        species,
                        "sampling_start",
                        "sampling_end",
                        species + "_variability",
                        species + "_repeatability",
                        species + "_number_of_observations",
                        "flag",
                    ]
                df = df[columns_to_keep]

                df = set_time_as_dataframe_index(dataframe=df)

            else:
                site_fname = filepath.name.split("_")[-3]
                inlet_height_fname = filepath.name.split("_")[-2]

                species, site, inlet = initial_checks_and_setup(
                    species=species_fname,
                    input_site=input_site,
                    site_fname=site_fname,
                    input_inlet=inlet,
                    inlet_height_fname=inlet_height_fname,
                )

                header, df = convert_icos_file_to_dataframe(filepath=filepath, rename_dict=rename_dict)

                if species_fname.lower() == "14c":
                    df["time"] = pd.to_datetime(df[["year", "month", "day", "hour", "minute"]])
                    df.index = df["time"]
                    df["sampling_start_date"] = pd.to_datetime(df[["year", "month", "day"]])

                    columns_to_keep = [
                        species,
                        "time",
                        "sampling_start_date",
                        "integration_time",
                        "weighted_std_err",
                        species + "_variability",
                        "sampling_pattern",
                        "flag",
                    ]
                    df = df[columns_to_keep]

        else:
            raise NotImplementedError()
    else:

        site_fname = filepath.name.split("_")[-5]
        inlet_height_fname = filepath.name.split("_")[-4]

        species, site, inlet = initial_checks_and_setup(
            species=species_fname,
            input_site=input_site,
            site_fname=site_fname,
            input_inlet=inlet,
            inlet_height_fname=inlet_height_fname,
        )

        header, df = convert_icos_file_to_dataframe(filepath=filepath, rename_dict=rename_dict)

        df["time"] = pd.to_datetime(df[["year", "month", "day", "hour", "minute"]])
        df.index = df["time"]
        df["sampling_start_date"] = pd.to_datetime(df[["year", "month", "day"]])

        columns_to_keep = [
            species,
            "time",
            "sampling_start_date",
            "integration_time",
            "weighted_std_err",
            species + "_variability",
            "sampling_pattern",
            "flag",
        ]

        df = df[columns_to_keep]

    df = clean_dataframe(df=df, species_name=species)
    df = calculate_sampling_period(dataframe=df, species=species, header=header)

    data = df.to_xarray()
    # setting units to sampling period data var
    data[f"{species}_sampling_period"].attrs["units"] = "s"
    data["flag"] = data["flag"].astype(str)

    metadata = {
        "site": site,
        "species": species,
        "inlet": inlet_height_fname,
        "inlet_height_magl": inlet,
        "sampling_period": sampling_period,
        "network": network,
        "instrument": instrument,
    }

    attributes = {"inlet_height_magl": metadata["inlet_height_magl"], "data_owner": "See data_owner_email"}

    if measurement_type is not None:
        metadata["measurement_type"] = measurement_type

    f_header = [s for s in header if "MEASUREMENT UNIT" in s]
    if len(f_header) == 1:
        units = f_header[0].split(":")[1].lower().strip()
        metadata["units"] = units
    else:
        raise ValueError("No unique MEASUREMENT UNIT in file header")

    f_header = [s for s in header if "CONTACT POINT" in s]
    if len(f_header) == 1:
        data_owner_email = f_header[0].split(":")[1].strip()
        metadata["data_owner_email"] = data_owner_email
    else:
        f_header = [s for s in header if "CONTACT POINT EMAIL" in s]
        if len(f_header) == 1:
            data_owner_email = f_header[0].split(":")[1].strip()
            metadata["data_owner_email"] = data_owner_email
        else:
            raise ValueError("Couldn't identify data owner email")

    if sampling_period is None:

        f_header = [s for s in header if "TIME INTERVAL" in s]
        interval_str = f_header[0].split(":")[1].strip()
        if interval_str == "hourly":
            metadata["sampling_period"] = "3600.0"
        elif f"{species}_sampling_period" in data.data_vars:

            rounded_values = np.round(data[f"{species}_sampling_period"].values, decimals=2)
            unique_values = np.unique(rounded_values)

            # Set a tolerance of 1 second
            tolerance = 1

            # Check if all unique values are close to 3600 (within the tolerance)
            if np.all(np.isclose(unique_values, 3600, atol=tolerance)):
                metadata["sampling_period"] = "3600.0"
            else:
                metadata["sampling_period"] = "multiple"
        else:
            missing_err_msg = (
                "Unable to determine sampling_period from input header/data. Please specify value in seconds."
            )
            logger.exception(missing_err_msg)
            MetadataMissingError(missing_err_msg)

    species_data = {species: {"metadata": metadata, "data": data, "attributes": attributes}}

    gas_data = dataset_formatter(data=species_data)

    # Ensure the data is CF compliant
    gas_data = assign_attributes(
        data=gas_data,
        site=site,
        sampling_period=sampling_period,
        update_mismatch=update_mismatch,
        site_filepath=site_filepath,
    )
    return gas_data


def convert_icos_file_to_dataframe(filepath: Path, rename_dict: dict) -> pd.DataFrame:
    """Reads an ICOS file and returns a cleaned dataframe.

    Args:
    filepath: icos filepath
    rename_dict: dictionary to rename column names

    Returns:
    df: pandas.Dataframe
    """
    header = read_header(filepath=filepath)
    len_header = len(header)

    if len_header < 40:
        logger.warning(f"We expect a header length of 40 or more but got {len_header}.")

    df = pd.read_csv(
        filepath,
        header=len_header - 1,
        sep=";",
        date_format="%Y %m %d %H %M",
        na_values=["-9.990", "-999.990"],
    )

    df.columns = df.columns.str.lower()
    existing_columns = df.columns
    filtered_rename_dict = {col: new_col for col, new_col in rename_dict.items() if col in existing_columns}

    if "o2" in existing_columns:
        df = df.rename(columns={"o2": "deltao2n2"})
    elif "14c" in existing_columns:
        df = df.rename(columns={"14c": "dco2c14"})

    df = df.rename(columns=filtered_rename_dict)

    return header, df


# TODO: Evaluate in separate issue if below function can be generalised, especially for both of the icos parsers


# These are some of the util functions specific to icos corso.
def clean_dataframe(df: pd.DataFrame, species_name: str) -> pd.DataFrame:
    """Cleans the dataframe by removing duplicates, sorting, and filtering flags.

    Args:
        df: icos dataframe
        species_name: species name
    """
    df = df.dropna(axis="rows", subset=[species_name.lower()])
    df = df.loc[~df.index.duplicated(keep="first")]
    df = df[~df["flag"].isin(["K", "N"])]

    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    dataframe_columns = df.columns.to_list()

    if "sampling_start" in dataframe_columns:
        df["sampling_start"]

    return df


def initial_checks_and_setup(
    species: str,
    input_site: str,
    site_fname: str,
    input_inlet: str | None = None,
    inlet_height_fname: str | None = None,
) -> tuple[str, str, str | None]:
    """
    This function sets up the inital checks for site, inlet and standardises to internal format.

    Args:
        species: species_fname
        input_site: user specified site name
        site_fname: site name fetched from filename
        input_inlet: user specified site name
        inlet_height_fname: inlet fetched from filename

    Returns:
        site, input_site, inlet_height_magl
    """

    if input_site.lower() != site_fname.lower():
        raise ValueError("Site mismatch between site argument passed and filename.")

    inlet_height_fname = format_inlet(inlet_height_fname)
    species = synonyms(species)

    if inlet_height_fname is not None:
        if input_inlet is not None and inlet_height_fname.lower() != input_inlet:
            raise ValueError("Mismatch between inlet height passed and in filename.")

    inlet_height_magl = format_inlet(inlet_height_fname, key_name="inlet_height_magl")

    return species, input_site, inlet_height_magl


def set_time_as_dataframe_index(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Sets time as the dataframe index based on the dataset type
    Args:
        dataframe: Pandas dataframe

    Returns:
        dataframe: Pandas dataframe
    """

    dataframe["sampling_start"] = pd.to_datetime(dataframe["sampling_start"]).values.astype("datetime64[ns]")
    dataframe["sampling_end"] = pd.to_datetime(dataframe["sampling_end"]).values.astype("datetime64[ns]")
    dataframe["time"] = dataframe[["sampling_start", "sampling_end"]].min(axis=1)
    dataframe["time"] = dataframe["time"].values.astype("datetime64[ns]")

    dataframe.set_index("time", inplace=True)

    return dataframe


def calculate_sampling_period(dataframe: pd.DataFrame, species: str, header: list) -> pd.DataFrame:
    """This function is used to calculate the difference between sampling_start and sampling_end to calculate the sampling period for each of the data points

    Args:
        df: Accepts pandas dataframe
        species: species name
        header: file header

    returns: dataframe containing sampling_period column
    """
    columns = dataframe.columns
    integration_time_flag = any("integrationtime is given in days" in s.lower() for s in header)

    if not integration_time_flag:
        if "sampling_end" in columns:
            dataframe[f"{species}_sampling_period"] = (
                dataframe["sampling_end"] - dataframe["sampling_start"]
            ).dt.total_seconds()
        else:
            ValueError(
                f"Unable to find sampling_start, sampling_end, or  {species}_sampling_period in the data to convert the time values into seconds"
            )
    else:
        dataframe[f"{species}_sampling_period"] = pd.to_timedelta(
            dataframe["integration_time"], unit="D"
        ).dt.total_seconds()

    return dataframe
