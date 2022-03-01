from typing import Dict, Union, Optional
from pathlib import Path


pathType = Union[str, Path]


def parse_aqmesh(
    data_filepath: pathType,
    metadata_filepath: pathType,
    sampling_period: Optional[str] = None,
) -> Dict:
    """Read AQMesh data files

    Args:
        data_filepath: Data filepath
        metadata_filepath: Metadata filepath
        sampling_period: Measurement sampling period (str)
    Returns:
        dict: Dictionary of data
    """
    from addict import Dict as aDict
    from pandas import read_csv

    if sampling_period is None:
        sampling_period = "NOT_SET"

    use_cols = [0, 1, 4, 6]
    datetime_cols = {"time": ["date_UTC"]}
    na_values = [-999, -999.0]

    df = read_csv(
        data_filepath,
        index_col="time",
        usecols=use_cols,
        parse_dates=datetime_cols,
        na_values=na_values,
    )

    # This might change so we'll read it each time for now
    metadata = _parse_metadata(filepath=metadata_filepath)

    # Species is given in the data column
    orig_species = df.columns[0]
    species_split = orig_species.split("_")

    species = species_split[0]
    units = species_split[1]

    species_lower = species.lower()
    rename_cols = {orig_species: species_lower, "location_name": "site"}
    df = df.rename(columns=rename_cols)
    df = df.dropna(axis="rows", subset=[species_lower])

    # TODO - add in assignment of attributes
    # assign_attributes

    site_groups = df.groupby(df["site"])
    site_data = aDict()
    for site, site_df in site_groups:
        site_name = site.replace(" ", "").lower()
        site_df = site_df.drop("site", axis="columns")
        site_data[site_name]["data"] = site_df.to_xarray()
        site_data[site_name]["metadata"] = metadata[site_name]
        # Add in the species to the metadata
        site_data[site_name]["metadata"]["species"] = species_lower
        site_data[site_name]["metadata"]["units"] = units
        site_data[site_name]["metadata"]["sampling_period"] = sampling_period

    site_dict: Dict = site_data.to_dict()
    return site_dict


def _parse_metadata(filepath: pathType) -> Dict:
    """Parse AQMesh metadata

    Args:
        filepath: Path to metadata CSV
        pipeline: If running in pipeline skip the writing of metadata to file
    Returns:
        dict: Dictionary of metadata
    """
    from addict import Dict as aDict
    from pandas import read_csv
    from openghg.util import check_date

    filepath = Path(filepath)
    raw_metadata = read_csv(filepath)

    site_metadata = aDict()

    for _, row in raw_metadata.iterrows():
        site_name = row["location_name"].replace(" ", "").lower()
        site_data = site_metadata[site_name]

        site_data["site"] = site_name
        site_data["pod_id"] = row["pod_id_location"]
        site_data["start_date"] = check_date(row["start_date_UTC"])
        site_data["end_date"] = check_date(row["end_date_UTC"])
        site_data["relocate_date"] = check_date(row["relocate_date_UTC"])
        site_data["long_name"] = row["location_name"]
        site_data["borough"] = row["Borough"]
        site_data["site_type"] = row["Type"]
        site_data["in_ulez"] = row["ULEZ"]
        site_data["latitude"] = row["Latitude"]
        site_data["longitude"] = row["Longitude"]
        site_data["inlet"] = row["Height"]
        site_data["network"] = "aqmesh_glasgow"
        site_data["sampling_period"] = "NA"

    # TODO - I feel this is a bit clunky
    dict_metadata: Dict = site_metadata.to_dict()
    return dict_metadata
