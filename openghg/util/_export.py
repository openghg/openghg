from addict import Dict as aDict
from typing import Dict, List, Union
from json import loads, dump
from pathlib import Path
from openghg.dataobjects import ObsData


__all__ = ["to_dashboard", "to_dashboard_mobile"]


def to_dashboard(
    data: Dict, selected_vars: List, downsample_n: int = 3, filename: str = None
) -> Union[Dict, None]:
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

    for site, species_data in data.items():
        for species, inlet_data in species_data.items():
            measurement_data: ObsData
            for inlet, measurement_data in inlet_data.items():
                dataset = measurement_data.data
                metadata = measurement_data.metadata
                attributes = dataset.attrs

                df = dataset.to_dataframe()

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

                # TODO - remove this if we add site location to standard metadata
                location = {
                    "latitude": attributes["station_latitude"],
                    "longitude": attributes["station_longitude"],
                }
                metadata.update(location)

                json_data = loads(df.to_json())
                metadata = measurement_data.metadata

                to_export[species][network][site][inlet][instrument] = {
                    "data": json_data,
                    "metadata": metadata,
                }

    if filename is not None:
        with open(filename, "w") as f:
            dump(obj=to_export, fp=f)
        return None
    else:
        # TODO - remove this once addict is stubbed
        export_dict: Dict = to_export.to_dict()
        return export_dict


def to_dashboard_mobile(data: Dict, filename: Union[str, Path] = None) -> Union[Dict, None]:
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
            dump(to_export, f)
        return None
    else:
        to_return: Dict = to_export.to_dict()
        return to_return
