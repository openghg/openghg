from __future__ import annotations
import logging
import gzip
import json
import pandas as pd
from pathlib import Path
from typing import Any, Literal, TYPE_CHECKING
from addict import Dict as aDict

if TYPE_CHECKING:
    from openghg.dataobjects import ObsData

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def to_dashboard(
    data: ObsData | list[ObsData],
    selected_vars: list,
    downsample_n: int = 3,
    filename: str | None = None,
) -> dict | None:
    """Takes a Dataset produced by OpenGHG and outputs it into a JSON
    format readable by the OpenGHG dashboard or a related project.

    This also exports a separate file with the locations of the sites
    for use with map selector component.

    Note - this function does not currently support export of data from multiple
    inlets.

    Args:
        data: Dictionary of retrieved data
        selected_vars: The variables to want to export
        downsample_n: Take every nth value from the data
        filename: filename to write output to
    Returns:
        None
    """
    to_export = aDict()

    if not isinstance(selected_vars, list):
        selected_vars = [selected_vars]

    selected_vars = [str(c).lower() for c in selected_vars]

    if not isinstance(data, list):
        data = [data]

    for obs in data:
        measurement_data = obs.data
        attributes = measurement_data.attrs
        metadata = obs.metadata

        df = measurement_data.to_dataframe()

        rename_lower = {c: str(c).lower() for c in df.columns}
        df = df.rename(columns=rename_lower)
        # We just want the selected variables
        to_extract = [c for c in df.columns if c in selected_vars]

        if not to_extract:
            continue

        df = df[to_extract]

        # Downsample the data
        if downsample_n > 1:
            df = df.iloc[::downsample_n]

        network = metadata["network"]
        instrument = metadata["instrument"]

        try:
            latitude = attributes["latitude"]
        except KeyError:
            latitude = metadata["latitude"]

        try:
            longitude = attributes["longitude"]
        except KeyError:
            longitude = metadata["longitude"]

        # TODO - remove this if we add site location to standard metadata
        location = {
            "latitude": latitude,
            "longitude": longitude,
        }
        metadata.update(location)

        json_data = json.loads(df.to_json())

        species = metadata["species"]
        site = metadata["site"]
        inlet = metadata["inlet"]

        to_export[species][network][site][inlet][instrument] = {
            "data": json_data,
            "metadata": metadata,
        }

    if filename is not None:
        with open(filename, "w") as f:
            json.dump(obj=to_export, fp=f)
        return None
    else:
        # TODO - remove this once addict is stubbed
        export_dict: dict = to_export.to_dict()
        return export_dict


def to_dashboard_mobile(data: dict, filename: str | Path | None = None) -> dict | None:
    """Export the Glasgow LICOR data to JSON for the dashboard

    Args:
        data: Data dictionary
        filename: Filename for export of JSON
    Returns:
        dict or None: Dictonary if no filename given
    """
    to_export = aDict()

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
        to_return: dict = to_export.to_dict()
        return to_return


def to_dashboard_agage(
    data: ObsData | list[ObsData],
    export_folder: str | Path,
    downsample_n: int = 3,
    compress_json: bool = False,
    float_to_int: bool = False,
    selection_level: Literal["site", "inlet"] = "inlet",
    mock_inlet: bool = False,
    drop_na: bool = True,
    default_site: str | None = None,
    default_species: str | None = None,
    default_inlet: str | None = None,
) -> None:
    """Takes ObsData objects produced by OpenGHG and outputs them to JSON
    files. Files are named using the following convention:

    TODO - add compression of metadata file

    if selection_level == "site":
        export_filename = f"{species}_{network}_{site}.json"
    elif selection_level == "inlet":
        export_filename = f"{species}_{network}_{site}_{inlet}_{instrument}.json"

    A separate metadata file, metadata_complete.json is created in the same
    export folder containing the metadata for each site and the filename for
    that specific data set. This chunking allows a larger amount of data to be used
    by the dashboard due to the separation into separate files.

    Args:
        data: ObsData object or list of ObsData objects
        export_folder: Folder path to write files
        selection_level: Do we want the user to select by site or inlet in the dashboard
        downsample_n: Take every nth value from the data
        compress_json: compress JSON using gzip
        float_to_int: Convert floats to ints by multiplying by 100
        mock_inlet: Use a mock "888m" inlet for the the dashboard as it doesn't currently support
        selection only by site.
        drop_na: Drop any NaNs from the datasets
        default_site: Set a default site for the dashboard
        default_species: Set a default species for the dashboard
        default_inlet: Set a default inlet for the dashboard
    Returns:
        None
    """
    allowed_selection_levels = ("site", "inlet")
    if selection_level not in allowed_selection_levels:
        raise ValueError(f"Invalid selection level, please select one of {allowed_selection_levels}")

    if selection_level == "site":
        raise NotImplementedError("Selection by site is not currently supported")

    if mock_inlet:
        logger.warning("Assuming multiple inlet height data and setting inlet = 'single_inlet'")

    export_folder = Path(export_folder)
    if not export_folder.exists():
        logger.info(f"Creating export folder at {export_folder}")
        export_folder.mkdir()
    # Here we'll store the metadata that can be used to populate the interface
    # it'll also hold the filenames for the retrieval of data

    metadata_complete_filepath = export_folder.joinpath("metadata_complete.json")
    dashboard_config_filepath = export_folder.joinpath("dashboard_config.json")

    # Create the data directory
    data_foldername = "measurements"
    data_dir = export_folder.joinpath(data_foldername)
    data_dir.mkdir(exist_ok=True)

    if not isinstance(data, list):
        data = [data]

    # Hold a list of all the files we export so we can check the size of the exported data
    file_sizes_bytes = 0
    one_MB = 1024 * 1024
    # We'll use this to convert floats to ints
    float_to_int_multiplier = 1000

    dashboard_config: dict[str, Any] = {}
    dashboard_config["selection_level"] = selection_level
    dashboard_config["float_to_int"] = float_to_int
    dashboard_config["compressed_json"] = compress_json

    if default_site is not None:
        dashboard_config["default_site"] = default_site
    if default_species is not None:
        dashboard_config["default_species"] = default_species
    if default_inlet is not None:
        dashboard_config["default_inlet"] = default_inlet

    if float_to_int:
        dashboard_config["float_to_int_multiplier"] = float_to_int_multiplier

    # We'll store the filename information and source metadata here
    metadata_complete = aDict()
    # We'll record the inlets for each site
    # so we can warn the user if they're exporting multiple inlets
    # for the same site
    site_inlets = aDict()

    for obs in data:
        measurement_data = obs.data
        attributes = measurement_data.attrs
        metadata = obs.metadata

        df: pd.DataFrame = measurement_data.to_dataframe()

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
        if drop_na:
            df = df.dropna()

        if float_to_int:
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
                station_latitude = obs.metadata["station_latitude"]
            except KeyError:
                station_latitude = obs.metadata["inlet_latitude"]

        try:
            station_longitude = attributes["station_longitude"]
        except KeyError:
            try:
                station_longitude = obs.metadata["station_longitude"]
            except KeyError:
                station_longitude = obs.metadata["inlet_longitude"]

        species = metadata["species"]
        site = metadata["site"]
        network = obs.metadata["network"]
        # TODO - remove this as we won't want to select by instrument
        # use a mock instrument name for now
        instrument = "instrument_key"

        # This is all the metadata we need for the dashboard itself
        source_metadata = {
            "station_latitude": station_latitude,
            "station_longitude": station_longitude,
            "species": species,
            "site": site,
            "network": network,
            "instrument": instrument,
            "units": obs.metadata["units"],
            "station_long_name": obs.metadata["station_long_name"],
        }

        # TODO - remove this once we've updated the dashboard to support selection by site or inlet
        if mock_inlet:
            inlet = "single_inlet"
        else:
            try:
                inlet = obs.metadata["inlet"]
            except ValueError:
                inlet = str(int(float(obs.metadata["inlet"])))

        source_metadata["inlet"] = inlet

        file_extension = ".json"
        if compress_json:
            file_extension += ".gz"

        if selection_level == "site":
            export_filename = f"{species}_{network}_{site}{file_extension}"
        else:
            export_filename = f"{species}_{network}_{site}_{inlet}_{instrument}{file_extension}"

        export_filepath = data_dir.joinpath(export_filename)

        file_data = {
            "metadata": source_metadata,
            "filepath": f"{data_foldername}/{export_filename.lower()}",
        }

        if selection_level == "site":
            metadata_complete[species][network][site] = file_data
            site_inlets[species][network][site] = (
                site_inlets[species][network][site].get(site, []).append(inlet)
            )
        else:
            metadata_complete[species][network][site][inlet][instrument] = file_data

        # TODO - Check if this hoop jumping is required, I can't remember exactly why
        # I did it
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

        logger.info(f"Writing dashboard data to: {export_filename}")

        file_size = export_filepath.stat().st_size
        file_sizes_bytes += file_size
        if file_size > one_MB:
            logger.warn(
                msg=f"The file {export_filename} is larger than 1 MB, consider ways to reduce its size."
            )

    if selection_level == "site":
        for species, networkData in site_inlets.items():
            for network, siteData in networkData.items():
                for site, inlets in siteData.items():
                    if len(inlets) > 1:
                        logger.warn(
                            msg=f"Site {site} has multiple inlets: {inlets}. "
                            "You've set selection_level == 'site' meaning only data for the last inlet will be kept."
                            "Please make sure are more specific in your selection of data or select"
                            "selection_level == 'inlet'"
                        )

    # Add in the config
    dashboard_config_filepath.write_text(json.dumps(dashboard_config))
    metadata_complete_filepath.write_text(json.dumps(metadata_complete))
    file_sizes_bytes += metadata_complete_filepath.stat().st_size
    file_sizes_bytes += dashboard_config_filepath.stat().st_size

    logger.info(f"\n\nComplete metadata file written to: {metadata_complete_filepath}")
    logger.info(f"Dashboard configuration file written to: {metadata_complete_filepath}")
    logger.info(f"\nTotal size of exported data package: {file_sizes_bytes / one_MB:.2f} MB")
