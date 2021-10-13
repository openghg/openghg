from typing import Dict, Optional, Union
from pathlib import Path

__all__ = ["read_aqmesh"]

pathType = Union[str, Path]


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
    from openghg.util import is_date

    filepath = Path(filepath)
    raw_metadata = read_csv(filepath)

    site_metadata = aDict()

    for index, row in raw_metadata.iterrows():
        site_name = row["location_name"].replace(" ", "").lower()
        site_data = site_metadata[site_name]

        site_data["site"] = site_name
        site_data["pod_id"] = row["pod_id_location"]
        site_data["start_date"] = is_date(row["start_date_UTC"])
        site_data["end_date"] = is_date(row["end_date_UTC"])
        site_data["relocate_date"] = is_date(row["relocate_date_UTC"])
        site_data["long_name"] = row["location_name"]
        site_data["borough"] = row["Borough"]
        site_data["site_type"] = row["Type"]
        site_data["in_ulez"] = row["ULEZ"]
        site_data["latitude"] = row["Latitude"]
        site_data["longitude"] = row["Longitude"]
        site_data["inlet"] = row["Height"]
        site_data["network"] = "aqmesh_glasgow"
        site_data["sampling_period"] = "NA"

    return site_metadata.to_dict()


def read_aqmesh(data_filepath: pathType, metadata_filepath: pathType) -> Dict:
    """Read AQMesh data files

    Args:
        data_filepath: Data filepath
        metadata_filepath: Metadata filepath
    Returns:
        dict: Dictionary of data
    """
    from addict import Dict as aDict
    from pandas import read_csv

    # use_cols = [date_UTC,co2_ppm,location_name,ratification_status]
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

    # Species is given in the data column
    species = df.columns[0].split("_")[0]
    species_lower = species.lower()

    rename_cols = {f"{species_lower}_ppm": species_lower, "location_name": "site"}
    df = df.rename(columns=rename_cols)
    df = df.dropna(axis="rows", subset=[species_lower])

    site_groups = df.groupby(df["site"])
    # This might change so we'll read it each time for now
    metadata = _parse_metadata(filepath=metadata_filepath)

    # TODO - add in assignment of attributes
    # assign_attributes

    site_data = aDict()
    for site, site_df in site_groups:
        site_name = site.replace(" ", "").lower()
        site_df = site_df.drop("site", axis="columns")
        site_data[site_name]["data"] = site_df.to_xarray()
        site_data[site_name]["metadata"] = metadata[site_name]
        # Add in the species to the metadata
        site_data[site_name]["metadata"]["species"] = species_lower

    return site_data.to_dict()
