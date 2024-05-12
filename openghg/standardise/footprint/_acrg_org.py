import logging
from typing import Dict, List, Optional, Tuple
import xarray as xr

from openghg.util import species_lifetime, timestamp_now, check_function_open_nc
from openghg.store import update_zero_dim
from openghg.types import multiPathType

logger = logging.getLogger("openghg.standardise.footprint")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_acrg_org(
    filepath: multiPathType,
    chunks: Optional[Dict] = None,
) -> List[Tuple[xr.Dataset, Dict]]:
    """
    Read and parse input emissions data in original ACRG format.

    Args:
        filepath: Path of file to load
        chunks: Chunk schema to use when storing data the NetCDF.
                It expects a dictionary of dimension name and chunk size,
                for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
    Returns:
        list: List of (dataset, metadata) tuples
    """
    xr_open_fn, filepath = check_function_open_nc(filepath)

    with xr_open_fn(filepath).chunk(chunks) as fp_data:
        if chunks:
            logger.info(f"Rechunking with chunks={chunks}")

        species = fp_data.attrs["species"]
        species = species.lower()

        time_resolved = False
        if species == "co2":
            # # Expect co2 data to have high time resolution
            logger.info("Setting time_resolved to True for co2 data")
            time_resolved = True

        # QUESTION - RACHEL / BRENDAN - can we expect to read short_lifetime from the data?
        # if short_lifetime:
        #     if species == "inert":
        #         raise ValueError(
        #             "When indicating footprint is for short lived species, 'species' input must be included"
        #         )
        # else:
        short_lifetime = False
        if species == "inert":
            lifetime = None
        else:
            lifetime = species_lifetime(species)
            if lifetime is not None:
                # TODO: May want to add a check on length of lifetime here
                logger.info("Updating short_lifetime to True since species has an associated lifetime")
                short_lifetime = True

        dv_rename = {
            # "fp": "srr",
            "temperature": "air_temperature",
            "pressure": "air_pressure",
            "wind_direction": "wind_from_direction",
            "PBLH": "atmosphere_boundary_layer_thickness",
        }

        attribute_rename = {"fp_output_units": "lpdm_native_output_units"}

        # # Removed for now - this renaming to match to PARIS would mean the dimension names
        # # were inconsistent between data types/
        # dim_rename = {"lat": "latitude", "lon": "longitude"}

        dim_drop = "lev"

        dim_reorder = ("time", "height", "lat", "lon")

        dv_attribute_updates: Dict[str, Dict[str, str]] = {}
        variable_names = [
            # "srr",
            "fp",
            "air_temperature",
            "air_pressure",
            "wind_speed",
            "wind_from_direction",
            "atmosphere_boundary_layer_thickness",
            "release_lon",
            "release_lat",
        ]

        for dv in variable_names:
            dv_attribute_updates[dv] = {}

        # dv_attribute_updates["srr"]["long_name"] = "source_receptor_relationship"
        dv_attribute_updates["fp"]["long_name"] = "source_receptor_relationship"
        dv_attribute_updates["air_temperature"]["long_name"] = "air temperature at release"
        dv_attribute_updates["air_pressure"]["long_name"] = "air pressure at release"
        dv_attribute_updates["atmosphere_boundary_layer_thickness"][
            "long_name"
        ] = "atmospheric boundary layer thickness at release"

        dv_attribute_updates["wind_speed"]["units"] = "m s-1"
        dv_attribute_updates["wind_speed"]["long_name"] = "wind speed at release"

        dv_attribute_updates["wind_from_direction"]["units"] = "degree"
        dv_attribute_updates["wind_from_direction"]["long_name"] = "wind direction at release"

        dv_attribute_updates["release_lon"]["units"] = "degree_east"
        dv_attribute_updates["release_lon"]["long_name"] = "Release longitude"
        dv_attribute_updates["release_lat"]["units"] = "degree_north"
        dv_attribute_updates["release_lat"]["long_name"] = "Release latitude"

        # Ignore type - dv_rename type should be fine as a dict but mypy unhappy.
        fp_data = fp_data.rename(**dv_rename)  # type: ignore
        # fp_data = fp_data.rename(**dim_rename)  # removed for now - see above

        fp_data = fp_data.drop_dims(dim_drop)
        fp_data = fp_data.transpose(*dim_reorder, ...)

        for attr, new_attr in attribute_rename.items():
            if attr in fp_data:
                fp_data.attrs[new_attr] = fp_data.attrs.pop(attr)

        for dv, attr_details in dv_attribute_updates.items():
            for key, value in attr_details.items():
                fp_data[dv].attrs[key] = value

        metadata = {}

        metadata["species"] = species
        metadata["site"] = fp_data.attrs["site"]
        metadata["domain"] = fp_data.attrs["domain"]
        metadata["model"] = fp_data.attrs["model"]

        # Check if time has 0-dimensions and, if so, expand this so time is 1D
        if "time" in fp_data.coords:
            fp_data = update_zero_dim(fp_data, dim="time")

        metadata["max_longitude"] = round(float(fp_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(fp_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(fp_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(fp_data["lat"].min()), 5)

        # Try and read this from the data
        high_spatial_resolution = False
        if "lon_high" in fp_data:
            high_spatial_resolution = True
            metadata["max_longitude_high"] = round(float(fp_data["lon_high"].max()), 5)
            metadata["min_longitude_high"] = round(float(fp_data["lon_high"].min()), 5)
            metadata["max_latitude_high"] = round(float(fp_data["lat_high"].max()), 5)
            metadata["min_latitude_high"] = round(float(fp_data["lat_high"].min()), 5)

        metadata["time_resolved"] = time_resolved
        metadata["high_spatial_resolution"] = high_spatial_resolution
        metadata["short_lifetime"] = short_lifetime

        metadata["heights"] = [float(h) for h in fp_data.height.values]

        # QUESTION - Do we also need to save all the variables we have available in this footprints?
        metadata["variables"] = list(fp_data.data_vars)

        # if model_params is not None:
        #     metadata["model_parameters"] = model_params

        # Set the attributes of this Dataset
        fp_data.attrs = {"author": "OpenGHG Cloud", "processed": str(timestamp_now())}

        return [fp_data, metadata]
