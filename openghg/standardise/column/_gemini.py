from functools import partial
from pathlib import Path
from typing import cast
from collections.abc import MutableMapping
import numpy as np
import xarray as xr
import pandas as pd


import logging

logger = logging.getLogger("openghg.standardise.column._gemini")


def _preprocess(ds: xr.Dataset) -> xr.Dataset:
    """Preprocess the dataset by converting time to datetime and expanding dimensions if needed.

    Args:
        ds: dataset to preprocess
    Returns: xr,Dataset: preprocessed dataset
    """
    ds["time"] = pd.to_datetime(ds.time, unit="s")
    for var in ds.data_vars:
        if "time" not in ds[var].dims and var not in ["longitude", "latitude", "obs_height"]:
            ds[var] = ds[var].expand_dims(time=ds.time.values)
    return ds


def _filter_and_resample(ds: xr.Dataset, species: str, quality_filt: bool, resample: bool) -> xr.Dataset:
    """
    Filter (if quality_filt = True) the data keeping those for which "qual_flag" is equal to 1.
    Then resample the data on an hourly scale.
    Args:
        ds: dataset with column concentrations
        species: species name e.g. "ch4"
        resample: if True resamples the data at hourly scale.
    Returns:
        dataset resampled and filtered (if asked)
    """

    # Mask qual_flag == 1 and drop the other data
    ds=ds.compute()
    ds = ds.where(ds["qual_flag"] == 1, drop=True)

    # Drop NaN values along time and sort
    ds = ds.dropna("time").sortby("time")

    if ds[f"X{species.upper()}"].size == 0:
        raise ValueError("All the data have been filtered by quality flag and/or by `xr.Dataset.dropna()`.")

    if not resample:
        return ds

    output = ds.resample(time="h").mean(dim="time")
    output[f"x{species}_uncertainty"] = ds[f"sigma_X{species.upper()}"].resample(time="h").max(dim="time")
    output = output.dropna("time")
    return output


def parse_gemini(
    filepath: str | Path | list[str] | list[Path],
    species: str,
    domain: str | None = None,
    site: str | None = None,
    network: str | None = "GEMINI",
    platform: str = "column",
    chunks: dict | None = None,
    quality_filt: bool = True,
    resample: bool = True,
    **kwargs: str,
) -> dict:
    """
    Parse and extract data from netcdf provided by Neil Humpage and downloadable on JASMIN in /gws/nopw/j04/geminiuk/ (soon to be migrated..).
    NOTE: when this parser was written, data and network were new and no documentation was available.
    The data are so standardise on the same way than the TCCON data (see parser_tccon in _tccon.py and info therein).
    Assumed variables equivalencies with TCCON data (for ch4) are assumed as follow:
        - "xch4" in TCCON data => "XCH4"
        - "prior_ch4" in TCCON data ~> "ch4_apriori" (this one is dry, whereas the TCCON one is wet)
        - "prior_h2o" in TCCON data ~> "h2o_apriori" (this one is dry, whereas the TCCON one is wet)
        - "ak_xch4" in TCCON data => "XCH4_AK"
        - "integration_operator" in TCCON data -> no equivalence. Derived using the pressure weight method (see parser_tccon for more infos)
        - "prior_gravity" in TCCON data -> no equivalence. Derived using `gravity = surface_gravity * mean_earth_radius / (mean_earth_radius + height_masl)
        - "xch4_error" in TCCON data -> no equivalence. It has been asked to Neil.

    Args:
        filepath: Path of observation file
        species: Species name or synonym e.g. "ch4"
        domain: domain, e.g. europe, just ised for metadata.
        quality_filt: If True, filters data keeping data with qual_flag==1.
        resample: If True, resample data at hourly scale.
    Returns:
        Dict : Dictionary of source_name : data, metadata, attributes

    """

    if not isinstance(filepath, list):
        filepath = Path(filepath).expanduser().resolve()
    else:
        filepath = cast(
            list[str] | list[Path], sorted(filepath, key=lambda x: Path(str(x)).stem.split("_")[-1])
        )

    var_to_read = [
        f"X{species.upper()}",
        f"sigma_X{species.upper()}",
        f"X{species.upper()}_AK",
        f"{species}_apriori",
        "h2o_apriori",
        "pressure_grid",
        "height_grid",
        "qual_flag",
        "latitude",
        "longitude",
        "obs_height",
    ]

    # Here we assume that all the files have the same variables and that they are in the same order
    data = xr.open_mfdataset(
        filepath,
        combine="by_coords",
        preprocess=_preprocess,
        decode_times=False,
    )[var_to_read].chunk(chunks if chunks is not None else {})

    decode_times = pd.to_datetime(data.time.values, unit='s', origin='unix', utc=True)
    
    data = data.assign_coords(time=decode_times.values.astype('datetime64[ns]'))

    # Create metadata #
    attributes = cast(MutableMapping, data.attrs)

    attributes["file_start_date"] = str(data.time.values.min())
    attributes["file_end_date"] = str(data.time.values.max())

    if site is None:
        raise ValueError("The `site` parameter must be provided.")

    filepath_str = str(filepath) if not isinstance(filepath, list) else str(filepath[0])

    if site.lower() in filepath_str.lower():
        site_gemini_shortname = site
    else:
        raise ValueError(
            f"The site name: {site} provided does not match with the filepath: {filepath}. "
            "Please check the site name and the filepath."
        )

    attributes["species"] = species
    attributes["domain"] = domain
    attributes["site"] = site_gemini_shortname.upper()
    attributes["network"] = network
    attributes["platform"] = platform
    attributes["inlet"] = "column"
    attributes["calibration_scale"] = "unknown"

    attributes["data_owner"] = "Neil Humpage"
    attributes["data_owner_email"] = "nh58@leicester.ac.uk"

    attributes["longitude"] = f"{float(data.longitude.values):.3f}"
    attributes["latitude"] = f"{float(data.latitude.values):.3f}"
    # TODO: Add a check here that the site is really in the domain

    # Prepare data #
    # Align units
    for var in [f"X{species.upper()}", f"sigma_X{species.upper()}", f"{species}_apriori"]:
        if species == "ch4":
            if "ppm" in data[var].attrs["units"]:
                data[var] *= 1e3
                data[var].attrs["units"] = data[var].attrs["units"].replace("ppm", "ppb")
            elif "ppb" not in data[var].attrs["units"]:
                raise ValueError("The units are not those expected.")
        else:
            var_unit_check = [f"X{species.upper()}", f"{species}_apriori", f"sigma_X{species.upper()}"]
            unit_check = {data[v].attrs.get("units") for v in var_unit_check}
            if len(unit_check) != 1:
                raise ValueError(
                    f"'X{species.upper()}', '{species}_apriori' and 'sigma_X{species.upper()}' have different units, please update this part of code to correct that."
                )
            logger.warning(f"No unit conversion is implemented for {species}.")

    # Derive pressure thickness
    press = -data["pressure_grid"].diff(dim="altitude", n=1)
    press["altitude"] = np.arange(0, press.altitude.size)
    press0 = (
        data["pressure_grid"]
        .isel(altitude=-1)
        .expand_dims(
            {
                "altitude": [
                    press.altitude.size,
                ]
            }
        )
    )
    data["dpj"] = xr.merge([press0, press]).pressure_grid

    # Derive gravity
    mean_earth_radius = 6371.01e3  # meters
    surface_gravity = 9.80665
    data["gravity"] = surface_gravity * mean_earth_radius / (mean_earth_radius + data["height_grid"] * 1e3)

    # Derive pressure weight (hj), wet to dry conversion factor,
    # dry mole fraction of water (fdry_h2o) and prior dry xch4
    if "dry" not in data["h2o_apriori"].attrs["units"]:
        raise ValueError("Cannot determine if the data are dry..")
    M_dryH2O, M_dryAir = 18.0153, 28.9647
    data["hj"] = data["dpj"] / (
        data["gravity"] * M_dryAir * (1 + (data["h2o_apriori"] * M_dryH2O / M_dryAir))
    )

    data["integration_operator"] = data["hj"] / data["hj"].sum(dim="altitude")

    # Clean dataset
    data = data.drop_vars(["dpj", "hj", "gravity"])

    # Filter the data and resample to hourly
    data = _filter_and_resample(ds=data, species=species, quality_filt=quality_filt, resample=resample)

    # Rename variables
    data = data.rename(
        {
            "integration_operator": "pressure_weights",
            "pressure_grid": "pressure_levels",
            f"X{species.upper()}": f"x{species}",
            f"{species}_apriori": f"{species}_profile_apriori",
            f"X{species.upper()}_AK": f"x{species}_averaging_kernel",
            "altitude": "lev",
        }
    )

    data = data.drop_vars(["longitude", "latitude", "h2o_apriori", "obs_height", f"sigma_X{species.upper()}"])

    # Altitude
    data = data.rename({"height_grid": "altitude"})
    if data["altitude"].attrs["units"] == "km above sea level":
        data["altitude"] *= 1e3
        data["altitude"].attrs["units"] = "m"
        data["altitude"].attrs["description"] = "m above sea level"

    # Define metadata
    required_metadata = [
        "species",
        "domain",
        "inlet",
        "site",
        "network",
        "platform",
        "calibration_scale",
        "longitude",
        "latitude",
        "data_owner",
        "data_owner_email",
        "file_start_date",
        "file_end_date",
    ]
    metadata = {k: attributes[k] for k in required_metadata}

    # Prepare dict to return
    gas_data = {species: {"metadata": metadata, "data": data, "attributes": attributes}}

    return gas_data
