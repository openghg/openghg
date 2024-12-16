import logging
from pathlib import Path
from collections import defaultdict
import warnings
from xarray import Dataset

from openghg.util import (
    check_species_time_resolved,
    check_species_lifetime,
    timestamp_now,
    # open_and_align_dataset,
    check_function_open_nc,
)
from openghg.store import infer_date_range, update_zero_dim
from openghg.types import multiPathType, ParseError

logger = logging.getLogger("openghg.standardise.footprint")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_paris(
    filepath: multiPathType,
    site: str,
    domain: str,
    model: str,
    inlet: str,
    species: str,
    met_model: str | None = None,
    network: str | None = None,
    period: str | tuple | None = None,
    continuous: bool = True,
    high_spatial_resolution: bool = False,
    time_resolved: bool = False,
    high_time_resolution: bool = False,
    short_lifetime: bool = False,
) -> dict:
    """
    Read and parse input footprints data in "paris" format.

    Args:
        filepath: Path of file to load
        site: Site name
        domain: Domain of footprints
        model: Model used to create footprint (e.g. NAME or FLEXPART)
        inlet: Height above ground level in metres. Format 'NUMUNIT' e.g. "10m"
        met_model: Underlying meteorlogical model used (e.g. UKV)
        species: Species name. For a long-lived species this should be "inert".
        network: Network name
        period: Period of measurements. Only needed if this can not be inferred from the time coords
        continuous: Whether time stamps have to be continuous.
        high_spatial_resolution : Indicate footprints include both a low and high spatial resolution.
        time_resolved: Indicate footprints are high time resolution (include H_back dimension)
            Note this will be set to True automatically if species="co2" (Carbon Dioxide).
        high_time_resolution:  This argument is deprecated and will be replaced in future versions with time_resolved.
        short_lifetime: Indicate footprint is for a short-lived species. Needs species input.
            Note this will be set to True if species has an associated lifetime.
    Returns:
        dict: Dictionary of data
    """

    if high_time_resolution:
        warnings.warn(
            "This argument is deprecated and will be replaced in future versions with time_resolved.",
            DeprecationWarning,
        )
        time_resolved = high_time_resolution

    # fp_data, filepath = open_and_align_dataset(filepath, domain)
    xr_open_fn, filepath = check_function_open_nc(filepath, domain)

    fp_data = xr_open_fn(filepath)

    time_resolved = check_species_time_resolved(species, time_resolved)
    short_lifetime = check_species_lifetime(species, short_lifetime)

    dv_rename = {"srr": "fp"}

    attribute_rename = {"fp_output_units": "LPDM_native_output_units"}

    dim_rename = {"latitude": "lat", "longitude": "lon"}

    dim_reorder = ("time", "height", "lat", "lon")

    try:
        # Ignore type - dv_rename type should be fine as a dict but mypy unhappy.
        fp_data = fp_data.rename(**dv_rename)  # type: ignore
    except ValueError:
        msg = "Unable to parse input data using source_format='paris'. "
        if "fp" in fp_data:
            msg += "May need to use source_format='acrg_org' ('fp' data variable is present)"
        logger.exception(msg)
        raise ParseError(msg)

    fp_data = fp_data.rename(dim_rename)

    fp_data = fp_data.transpose(*dim_reorder, ...)

    for attr, new_attr in attribute_rename.items():
        if attr in fp_data:
            fp_data.attrs[new_attr] = fp_data.attrs.pop(attr)

    # Need to read the metadata from the footprints and then store it
    # Do we need to chunk the footprints / will a Datasource store it correctly?
    metadata: dict[str, str | float | list[float]] = {}

    metadata["data_type"] = "footprints"
    metadata["site"] = site
    metadata["domain"] = domain
    metadata["model"] = model

    # Include both inlet and height keywords for backwards compatability
    metadata["inlet"] = inlet
    metadata["height"] = inlet
    metadata["species"] = species

    if met_model is not None:
        metadata["met_model"] = met_model

    if network is not None:
        metadata["network"] = network

    # Check if time has 0-dimensions and, if so, expand this so time is 1D
    if "time" in fp_data.coords:
        fp_data = update_zero_dim(fp_data, dim="time")
    else:
        msg = "Expect 'time' coordinate within footprint data for source_format='paris'"
        logger.exception(msg)
        raise ParseError(msg)

    fp_time = fp_data["time"]

    # If filepath is a single file, the naming scheme of this file can be used
    # as one factor to try and determine the period.
    # If multiple files, this input isn't needed.
    if isinstance(filepath, (str, Path)):
        input_filepath = filepath
    else:
        input_filepath = None

    start_date, end_date, period_str = infer_date_range(
        fp_time, filepath=input_filepath, period=period, continuous=continuous
    )

    metadata["start_date"] = str(start_date)
    metadata["end_date"] = str(end_date)
    metadata["time_period"] = period_str

    metadata["max_longitude"] = round(float(fp_data["lon"].max()), 5)
    metadata["min_longitude"] = round(float(fp_data["lon"].min()), 5)
    metadata["max_latitude"] = round(float(fp_data["lat"].max()), 5)
    metadata["min_latitude"] = round(float(fp_data["lat"].min()), 5)

    if high_spatial_resolution:
        try:
            metadata["max_longitude_high"] = round(float(fp_data["lon_high"].max()), 5)
            metadata["min_longitude_high"] = round(float(fp_data["lon_high"].min()), 5)
            metadata["max_latitude_high"] = round(float(fp_data["lat_high"].max()), 5)
            metadata["min_latitude_high"] = round(float(fp_data["lat_high"].min()), 5)

        except KeyError:
            raise KeyError("Expected high spatial resolution. Unable to find lat_high or lon_high data.")

    metadata["time_resolved"] = str(time_resolved)
    metadata["high_spatial_resolution"] = str(high_spatial_resolution)
    metadata["short_lifetime"] = str(short_lifetime)

    metadata["heights"] = [float(h) for h in fp_data.height.values]
    # Do we also need to save all the variables we have available in this footprints?
    metadata["variables"] = list(fp_data.data_vars)

    # if model_params is not None:
    #     metadata["model_parameters"] = model_params

    # TODO: Decide if to remove this as may not be the right thing for data we're outputting.
    # Set the attributes of this Dataset
    fp_data.attrs = {"author": "OpenGHG Cloud", "processed": str(timestamp_now())}

    # This might seem longwinded now but will help when we want to read
    # more than one footprints at a time
    # TODO - remove this once assign_attributes has been refactored
    key = "_".join((site, domain, model, inlet))

    footprint_data: defaultdict[str, dict[str, dict | Dataset]] = defaultdict(dict)
    footprint_data[key]["data"] = fp_data
    footprint_data[key]["metadata"] = metadata

    return footprint_data


# Adding "flexpart" as an alias for "paris"
parse_flexpart = parse_paris
