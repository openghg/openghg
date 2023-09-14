from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List, Literal, Union, TYPE_CHECKING, Optional
import addict
from pandas import DataFrame
import logging
import gzip

if TYPE_CHECKING:
    from openghg.dataobjects import ObsData

__all__ = ["to_dashboard", "to_dashboard_mobile"]

logger = logging.getLogger("openghg.util")
logger.setLevel("DEBUG")


def to_dashboard(
    data: Union[ObsData, List[ObsData]],
    export_folder: Path,
    downsample_n: int = 3,
    output_format: Literal["json", "parquet"] = "json",
    compress_json: bool = False,
    parquet_compression: Literal["brotli", "snappy", "gzip"] = "gzip",
    float_to_int: bool = False,
    default_site: Optional[str] = None,
    default_species: Optional[str] = None,
    default_inlet: Optional[str] = None,
    default_instrument: Optional[str] = None,
) -> None:
    """Takes ObsData objects produced by OpenGHG and outputs them to JSON
    files. Files are named using the following convention:

    export_filename = f"{species}_{network}_{site}_{inlet}_{instrument}.json"

    A separate metadata file, metadata_complete.json is created in the same
    export folder containing the metadata for each site and the filename for
    that specific data set. This chunking allows a larger amount of data to be used
    by the dashboard due to the separation into separate files.

    Args:
        data: ObsData object or list of ObsData objects
        export_folder: Folder path to write files
        downsample_n: Take every nth value from the data
        output_format: json or parquet
        compress_json: compress JSON using gzip
        parquet_compression: One of ["brotli", "snappy", "gzip"]
        float_to_int: Convert floats to ints by multiplying by 100
    Returns:
        None
    """
    allowed_formats = ("json", "parquet")
    if output_format not in allowed_formats:
        raise ValueError(f"Invalid output format, please select one of {allowed_formats}")

    export_folder = Path(export_folder)
    if not export_folder.exists():
        logger.info(f"Creating export folder at {export_folder}")
        export_folder.mkdir()
    # Here we'll store the metadata that can be used to populate the interface
    # it'll also hold the filenames for the retrieval of data
    metadata_complete = addict.Dict()
    metadata_complete_filepath = Path(export_folder).joinpath("metadata_complete_compressed.json")

    # Create the data directory
    data_foldername = "measurements"
    data_dir = export_folder.joinpath(data_foldername)
    data_dir.mkdir(exist_ok=True)

    if not isinstance(data, list):
        data = [data]

    # Hold a list of all the files we export so we can check the size of the exported data
    file_sizes_bytes = 0
    one_MB = 1024 * 1024

    for obs in data:
        measurement_data = obs.data
        attributes = measurement_data.attrs
        metadata = obs.metadata

        df: DataFrame = measurement_data.to_dataframe()

        rename_lower = {c: str(c).lower() for c in df.columns}
        df = df.rename(columns=rename_lower)

        species_name = obs.metadata["species"]

        # Some of the AGAGE data variables are named differently from the species in the metadata
        try:
            df = df[[species_name]]
        except KeyError:
            species_label = obs.metadata["species_label"]
            df = df[[species_label]]

        # Drop any NaNs
        df = df.dropna()

        if float_to_int:
            float_to_int_multiplier = 100
            key = next(iter(df))
            df[key] = df[key] * float_to_int_multiplier
            df = df.astype(int)

        # Downsample the data
        if downsample_n > 1:
            df = df.iloc[::downsample_n]

        try:
            station_latitude = attributes["station_latitude"]
        except KeyError:
            try:
                station_latitude = metadata["station_latitude"]
            except KeyError:
                station_latitude = metadata["inlet_latitude"]

        try:
            station_longitude = attributes["station_longitude"]
        except KeyError:
            try:
                station_longitude = metadata["station_longitude"]
            except KeyError:
                station_longitude = metadata["inlet_longitude"]

        # TODO - remove this if we add site location to standard metadata
        location = {
            "station_latitude": station_latitude,
            "station_longitude": station_longitude,
        }

        metadata.update(location)

        species = metadata["species"]
        site = metadata["site"]
        inlet = str(int(float(metadata["inlet"])))
        network = metadata["network"]
        instrument = metadata["instrument"]

        if output_format == "json":
            file_extension = ".json"
            if compress_json:
                file_extension += ".gz"
        elif output_format == "parquet":
            file_extension = ".parquet"

        export_filename = f"{species}_{network}_{site}_{inlet}_{instrument}{file_extension}"
        export_filepath = data_dir.joinpath(export_filename)

        file_data = {
            "metadata": metadata,
            "filepath": f"{data_foldername}/{export_filename}",
            "float_to_int": float_to_int,
        }

        if float_to_int:
            file_data["float_to_int_multiplier"] = float_to_int_multiplier

        metadata_complete[species][network][site][inlet][instrument] = file_data

        # TODO - Check if this hoop jumping is required, I can't remember exactly why
        # I did it
        if output_format == "json":
            data_dict = json.loads(df.to_json())
            # Let's trim the species name as we don't need that
            key = next(iter(data_dict))
            data_dict = data_dict[key]

            for_export_str = json.dumps(data_dict)
            if compress_json:
                for_export_bytes = gzip.compress(for_export_str.encode())
                export_filepath.write_bytes(for_export_bytes)
            else:
                export_filepath.write_text(for_export_str)
        else:
            logger.warning(
                "The dashboard doesn't currently support the parquet format. "
                + "This is for testing purposes only."
            )
            df.to_parquet(export_filepath, compression=parquet_compression)

        logger.info(f"Writing dashboard data to: {export_filename}")

        file_size = export_filepath.stat().st_size
        file_sizes_bytes += file_size
        if file_size > one_MB:
            logger.warn(
                msg=f"The file {export_filename} is larger than 1 MB, consider ways to reduce its size."
            )

    metadata_complete_filepath.write_text(json.dumps(metadata_complete))
    file_sizes_bytes += metadata_complete_filepath.stat().st_size

    logger.info(f"\n\nComplete metadata file written to: {metadata_complete_filepath}")
    logger.info(f"Total size of exported data package: {file_sizes_bytes/one_MB:.2f} MB")


def to_dashboard_mobile(data: Dict, filename: Union[str, Path, None] = None) -> Union[Dict, None]:
    """Export the Glasgow LICOR data to JSON for the dashboard

    Args:
        data: Data dictionary
        filename: Filename for export of JSON
    Returns:
        dict or None: Dictonary if no filename given
    """
    to_export = addict.Dict()

    for species, species_data in data.items():
        spec_data = species_data["data"]
        metadata = species_data["metadata"]

        latitude = spec_data["latitude"].values.tolist()
        longitude = spec_data["longitude"].values.tolist()
        ch4 = spec_data["ch4"].values.tolist()

        to_export[species]["data"] = {"lat": latitude, "lon": longitude, "z": ch4}
        to_export[species]["metadata"] = metadata

    if filename is not None:
        with open(filename, "w") as f:
            json.dump(to_export, f)
        return None
    else:
        to_return: Dict = to_export.to_dict()
        return to_return
