import logging
from pathlib import Path
from collections import defaultdict
from typing import DefaultDict, Dict, Literal, List, Optional, Union
import xarray as xr
from xarray import Dataset
from openghg.util import species_lifetime, timestamp_now
from openghg.store import infer_date_range, update_zero_dim

logger = logging.getLogger("openghg.standardise.footprint")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_acrg_org(
    filepath: Path,
    site: str,
    domain: str,
    model: str,
    inlet: str,
    metmodel: Optional[str] = None,
    species: Optional[str] = None,
    network: Optional[str] = None,
    period: Optional[Union[str, tuple]] = None,
    continuous: bool = True,
    high_spatial_resolution: bool = False,
    high_time_resolution: bool = False,
    short_lifetime: bool = False,
    chunks: Union[int, Dict, Literal["auto"], None] = None,
) -> Dict:
    """
    Read and parse input emissions data in original ACRG format.

    Args:
        filepath: Path of file to load
        site: Site name
        domain: Domain of footprints
        model: Model used to create footprint (e.g. NAME or FLEXPART)
        inlet: Height above ground level in metres. Format 'NUMUNIT' e.g. "10m"
        metmodel: Underlying meteorlogical model used (e.g. UKV)
        species: Species name. Only needed if footprint is for a specific species e.g. co2 (and not inert)
        network: Network name
        period: Period of measurements. Only needed if this can not be inferred from the time coords
        continuous: Whether time stamps have to be continuous.
        high_spatial_resolution : Indicate footprints include both a low and high spatial resolution.
        high_time_resolution: Indicate footprints are high time resolution (include H_back dimension)
                        Note this will be set to True automatically if species="co2" (Carbon Dioxide).
        short_lifetime: Indicate footprint is for a short-lived species. Needs species input.
                        Note this will be set to True if species has an associated lifetime.
        chunks: Chunk size to use when parsing NetCDF, useful for large datasets.
                        Passing "auto" will ask xarray to calculate a chunk size.
    Returns:
        dict: Dictionary of data
    """

    # Load this into memory
    fp_data = xr.open_dataset(filepath, chunks=chunks)

    if species == "co2":
        # Expect co2 data to have high time resolution
        if not high_time_resolution:
            logger.info("Updating high_time_resolution to True for co2 data")
            high_time_resolution = True

    if short_lifetime and not species:
        raise ValueError(
            "When indicating footprint is for short lived species, 'species' input must be included"
        )
    elif not short_lifetime and species:
        lifetime = species_lifetime(species)
        if lifetime is not None:
            # TODO: May want to add a check on length of lifetime here
            logger.info("Updating short_lifetime to True since species has an associated lifetime")
            short_lifetime = True

    # Need to read the metadata from the footprints and then store it
    # Do we need to chunk the footprints / will a Datasource store it correctly?
    metadata: Dict[str, Union[str, float, List[float]]] = {}

    metadata["data_type"] = "footprints"
    metadata["site"] = site
    metadata["domain"] = domain
    metadata["model"] = model

    # Include both inlet and height keywords for backwards compatability
    metadata["inlet"] = inlet
    metadata["height"] = inlet

    if species is not None:
        metadata["species"] = species

    if network is not None:
        metadata["network"] = network

    if metmodel is not None:
        metadata["metmodel"] = metmodel

    # Check if time has 0-dimensions and, if so, expand this so time is 1D
    if "time" in fp_data.coords:
        fp_data = update_zero_dim(fp_data, dim="time")

    fp_time = fp_data["time"]

    start_date, end_date, period_str = infer_date_range(
        fp_time, filepath=filepath, period=period, continuous=continuous
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

    metadata["high_time_resolution"] = high_time_resolution
    metadata["high_spatial_resolution"] = high_spatial_resolution
    metadata["short_lifetime"] = short_lifetime

    metadata["heights"] = [float(h) for h in fp_data.height.values]
    # Do we also need to save all the variables we have available in this footprints?
    metadata["variables"] = list(fp_data.data_vars)

    # if model_params is not None:
    #     metadata["model_parameters"] = model_params

    # Set the attributes of this Dataset
    fp_data.attrs = {"author": "OpenGHG Cloud", "processed": str(timestamp_now())}

    # This might seem longwinded now but will help when we want to read
    # more than one footprints at a time
    # TODO - remove this once assign_attributes has been refactored
    key = "_".join((site, domain, model, inlet))

    footprint_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
    footprint_data[key]["data"] = fp_data
    footprint_data[key]["metadata"] = metadata

    return footprint_data