from __future__ import annotations
from json import dump, loads
from pathlib import Path
from typing import Dict, List, Union, Optional, TYPE_CHECKING
from addict import Dict as aDict
from pandas import DataFrame

if TYPE_CHECKING:
    from openghg.dataobjects import ObsData

__all__ = ["to_dashboard", "to_dashboard_mobile"]


def to_dashboard(
    data: Union[ObsData, List[ObsData]],
    downsample_n: int = 3,
    filepath: Optional[str] = None,
) -> Union[Dict, None]:
    """Takes a Dataset produced by OpenGHG and outputs it into a JSON
    format readable by the OpenGHG dashboard or a related project.

    This also exports a separate file with the locations of the sites
    for use with map selector component.

    Note - this function does not currently support export of data from multiple
    inlets.

    Args:
        data: Dictionary of retrieved data
        downsample_n: Take every nth value from the data
        filepath: filepath to write out JSON data
    Returns:
        None
    """
    to_export = aDict()

    if not isinstance(data, list):
        data = [data]

    for obs in data:
        measurement_data = obs.data
        attributes = measurement_data.attrs
        metadata = obs.metadata

        df: DataFrame = measurement_data.to_dataframe()

        rename_lower = {c: str(c).lower() for c in df.columns}
        df = df.rename(columns=rename_lower)
        filtered_species = metadata['species']
        df = df[filtered_species]

        # Downsample the data
        if downsample_n > 1:
            df = df.iloc[::downsample_n]

        network = metadata["network"]
        instrument = metadata["instrument"]

        try:
            station_latitude = attributes["station_latitude"]
        except KeyError:
            station_latitude = metadata["station_latitude"]

        try:
            station_longitude = attributes["station_longitude"]
        except KeyError:
            station_longitude = metadata["station_longitude"]

        # TODO - remove this if we add site location to standard metadata
        location = {
            "station_latitude": station_latitude,
            "station_longitude": station_longitude,
        }
        metadata.update(location)

        json_data = loads(df.to_json())

        species = metadata["species"]
        site = metadata["site"]
        inlet = metadata["inlet"]

        to_export[species][network][site][inlet][instrument] = {
            "data": json_data,
            "metadata": metadata,
        }

    if filepath is not None:
        with open(filepath, "w") as f:
            dump(obj=to_export, fp=f)
        return None
    else:
        # TODO - remove this once addict is stubbed
        export_dict: Dict = to_export.to_dict()
        return export_dict


def to_dashboard_mobile(data: Dict, filename: Union[str, Path, None] = None) -> Union[Dict, None]:
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
