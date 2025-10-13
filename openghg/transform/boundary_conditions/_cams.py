import os
import logging
from pathlib import Path
import pathlib
from typing import Any, Callable, cast, Literal

import xarray as xr
import numpy as np
import pandas as pd

from openghg.util import find_domain, normalise_to_filepath_list, open_time_nc_fn, timestamp_now
from openghg.store import infer_date_range, update_zero_dim
from openghg.retrieve import get_footprint

logger = logging.getLogger("openghg.transform.boundary_conditions")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def interp1d_np(data: np.ndarray, x: np.ndarray, xi: np.ndarray, **kwargs: Any) -> np.ndarray:
    # np.interp returns a scalar if `xi` is a scalar, but we know `xi` is an array
    return cast(np.ndarray, np.interp(xi, x, data, **kwargs))


def xr_interp(
    data: xr.DataArray, dim: str, interp_vals: np.ndarray, coord: str | None = None
) -> xr.DataArray:
    """Iterpolate along given dim.

        If 'coord' is passed, then an alternate coordinate can be used, but the interpolation happens
        along 'dim'.

        This is useful if there is a "physical" coordinate, like altitude, which depends on lat and lon.
        So if data has dims: lat, level, and data.z is a coordinate with dims lat, level, which converts
        level to a height, dependant on lat, then setting dim='level' and coord='z' will allow interpolation
        with interp_vals that are on the same scale as 'z'.
    Note: `data[coord]` (or `data[dim]` if `coord` is `None`) must be sorted in increasing order. If not
    the interpolation will not fail, but the results will be meaningless. See the docs for `np.interp` for details.
        The function is based on https://tutorial.xarray.dev/advanced/apply_ufunc/example-interp.html
    """
    dim_coord = data[dim] if coord is None else data[coord]
    result: xr.DataArray = xr.apply_ufunc(
        interp1d_np,
        data,
        dim_coord,
        interp_vals,
        input_core_dims=[[dim], [dim], ["__newdim__"]],
        output_core_dims=[["__newdim__"]],
        exclude_dims=set((dim,)),
        vectorize=True,
        dask="parallelized",
        output_dtypes=[data.dtype],
    ).rename({"__newdim__": dim})
    result[dim] = interp_vals
    return result


def cams_to_domain(
    ds: xr.Dataset, domain: str, xr_interp_fn: Callable = xr_interp, get_footprint_kwargs: dict | None = None
) -> xr.Dataset:
    """
    Interpolate CAMS data on another grid (should be one commonly used in openghg)
    Args:
        ds: datset to
        xr_interp_fn: interpolation function to use
        get_footprint_kwargs: kwargs passed to get_footprint from openghg_retrieve to find the footprint data from which the grid will be taken.
            If None, will use function find_domain from openghg.util to get lat/lon and set the height as np.linspace(500.0, 19500.0, 20, endpoint=True)
    Returns:
        interpolated Dataset with 4 variables: vmr_n/e/s/w
    """

    if get_footprint_kwargs:
        fp = get_footprint(**get_footprint_kwargs).data
        lat = fp["lat"].values
        lon = fp["lon"].values
        height = fp["height"].values
    else:
        lat, lon = find_domain(domain)
        height = np.linspace(500.0, 19500.0, 20, endpoint=True)

    lat_n = ds.lat.where(ds.lat > lat.max()).min()
    lat_s = ds.lat.where(ds.lat < lat.min()).max()
    lon_e = ds.lon.where(ds.lon > lon.max()).min()
    lon_w = ds.lon.where(ds.lon < lon.min()).max()

    z = 0.5 * (ds.altitude.isel(hlevel=slice(0, -1)).values + ds.altitude.isel(hlevel=slice(1, None)).values)
    ds = ds.assign_coords(
        {"z": (tuple([dim if dim != "hlevel" else "level" for dim in ds.altitude.dims]), z)}
    )

    north = ds[["species", "z"]].sel(lat=lat_n, lon=slice(lon_w, lon_e)).drop_vars("lat")
    south = ds[["species", "z"]].sel(lat=lat_s, lon=slice(lon_w, lon_e)).drop_vars("lat")
    east = ds[["species", "z"]].sel(lon=lon_e, lat=slice(lat_s, lat_n)).drop_vars("lon")
    west = ds[["species", "z"]].sel(lon=lon_w, lat=slice(lat_s, lat_n)).drop_vars("lon")

    data_vars = {
        "vmr_n": xr_interp_fn(north.species, "level", height, "z").interp(lon=lon).astype("float32"),
        "vmr_e": xr_interp_fn(east.species, "level", height, "z").interp(lat=lat).astype("float32"),
        "vmr_s": xr_interp_fn(south.species, "level", height, "z").interp(lon=lon).astype("float32"),
        "vmr_w": xr_interp_fn(west.species, "level", height, "z").interp(lat=lat).astype("float32"),
    }
    return xr.Dataset(data_vars)


def get_resample_args(xr_time: xr.DataArray, species: str, period: str) -> dict:
    """
    Infer the arguments that will be used for the time resampling of the dataset
    Args:
        xr_time: time coordinates of the dataset to resample
        species: species of the dataset (necessary to set the 'closed' parameter that will be used for resampling)
        period: targeted period. Can be "3h", "daily", "monthly", "yearly" or a pandas aliases
    Returns:
        dict with 2 keys : "time" and "closed; whose values are the one passed to xr.Dataset().resample
    """
    org_freq_str = xr.infer_freq(xr_time)
    org_freq = pd.to_timedelta(org_freq_str if org_freq_str[0].isdecimal() else "1" + org_freq_str)
    thres_dict = {
        "3h": pd.Timedelta(3, "h"),
        "daily": pd.Timedelta(1, "D"),
        "monthly": pd.Timedelta(31, "D"),
        "yearly": pd.Timedelta(365, "D"),
    }

    if org_freq > thres_dict.get(period, period):
        raise ValueError("Original time resolution is coarser than targeted one.")

    alias_period = {"daily": "D", "monthly": "MS", "yearly": "YS"}

    closed: Literal["left", "right"] = "right" if species == "n2o" else "left"
    return {"time": alias_period.get(period, period), "closed": closed}


def calc_altitude_from_pressure(ds: xr.Dataset) -> xr.Dataset:
    """
    Calculate altitude of dataset levels. Specific to CAMS N2O files.
    Args:
        ds: dataset (CAMS N2O files), contains ap, bp, and Psurf variables
    Returns:
        ds: dataset whith new variable altitude (in meter)
    """
    pressure = ds.ap + ds.bp * ds.Psurf
    scale_height = 7.64e3  # in metres
    ds["altitude"] = -scale_height * np.log(pressure / ds.Psurf)
    return ds


def get_gridsize(ds: xr.Dataset) -> str:
    """
    Calculate mean grid cell size of dataset
    Args:
        ds: dataset containing lon and lat coordinates
    Returns
        string of the form "{lat_grid} x {lon_grid}"
    """
    lat_grid = (ds["lat"].values[1:] - ds["lat"].values[:-1]).mean()
    lon_grid = (ds["lon"].values[1:] - ds["lon"].values[:-1]).mean()
    return f"{lat_grid} x {lon_grid}"


def _check_and_set_params(
    filepath: list[Path],
    cams_version: str | None = None,
    species: str | None = None,
    input_observations: str | None = None,
) -> tuple[str, str, str]:
    """
    Extract fron the file names informations on species, cams version, input observations used in the files.
    Check that they correspond to parameters cams_version, species and input_observations (if given) and format them to be saved as attributes and metadata.
    Args:
        filepath: list of filepath that will be standardised
        cams_version: version of cams product that is used
        secies: species of interest
        input_observations: type of input observations (e.g. "surface_satellite_dm") that is used
    Returns:
        tuple containing 1. a string aggregating the cams versions used, 2. the species and 3. a string aggregating the input observations used.
    """
    cams_version_check, species_check, input_observations_check = [], [], []

    for file in filepath:
        file_keywords = file.name.split("_")
        if file_keywords[0] != "cams73" or file_keywords[3] != "conc" and file_keywords[-1][-3:] != ".nc":
            raise ValueError(
                "Filenames not in a proper format: expected something like cams73_*_*_conc_*.nc. Please don't alter the names from the unzipped CAMS files."
            )

        if species and species.lower() != file_keywords[2]:
            raise ValueError(
                f"Input species is {species} but species detected in filename is {file_keywords[2]}."
            )
        species_check.append(file_keywords[2])

        if cams_version and cams_version not in ["mix", file_keywords[1]]:
            raise ValueError(
                f"Input cams_version is {cams_version} but cams_version detected in {file} is {file_keywords[1]}."
            )
        cams_version_check.append(file_keywords[1])

        if input_observations and input_observations not in ["mix", ("_").join(file_keywords[3:-1])]:
            raise ValueError(
                f"Input input_observations is {input_observations} but input_observations detected in {file} is {('_').join(file_keywords[3:-1])}."
            )
        input_observations_check.append(("_").join(file_keywords[3:-1]))

    if len(set(species_check)) != 1:
        raise ValueError("Multiple species detected. Please standardise them separately")
    species = species_check[0]

    cams_version_check = list(set(cams_version_check))
    if cams_version == "mix":
        logger.warning(f"cams_version used: {cams_version_check}.")
    elif len(cams_version_check) != 1:
        raise ValueError(
            f"Multiple cams versions detected: {cams_version_check}. If you want to mix the cams versions, set cams_version='mix'."
        )
    else:
        cams_version = cams_version_check[0]

    input_observations_check = list(set(input_observations_check))
    if input_observations == "mix":
        logger.warning(f"input_observations used: {input_observations_check}.")
    elif len(input_observations_check) != 1:
        raise ValueError(
            f"Multiple input observations detected: {input_observations_check}. If you want to mix the input observations, set input_observations='mix'."
        )
    else:
        input_observations = input_observations_check[0]

    return cams_version, species, input_observations


def make_metadata(ds: xr.Dataset, period: str, continuous: bool, **kwargs: Any) -> dict:
    """
    Create metadata dictionnary for standardisation.
    Args:
        ds: processed boundary conditions data
        filepath: (List of) Path of boundary conditions fil
        period: period at which at resample and store the data
        continuous: whether time stamps have to be continuous
        **kwargs: other parameters to put as metadata
    Returns:
        metadata: dict containing metadata
    """
    metadata = {}
    metadata.update(ds.attrs)
    metadata.update(kwargs)
    metadata["processed"] = str(timestamp_now())

    # If filepath is a single file, the naming scheme of this file can be used
    # as one factor to try and determine the period.
    # If multiple files, this input isn't needed.

    start_date, end_date, period_str = infer_date_range(
        ds.time, filepath=None, period=period, continuous=continuous
    )

    metadata["start_date"] = str(start_date)
    metadata["end_date"] = str(end_date)

    metadata["max_longitude"] = round(float(ds["lon"].max()), 5)
    metadata["min_longitude"] = round(float(ds["lon"].min()), 5)
    metadata["max_latitude"] = round(float(ds["lat"].max()), 5)
    metadata["min_latitude"] = round(float(ds["lat"].min()), 5)
    metadata["min_height"] = round(float(ds["level"].min()), 5)
    metadata["max_height"] = round(float(ds["level"].max()), 5)

    metadata["time_period"] = period_str

    return metadata


def parse_cams(
    bc_input: str,
    domain: str,
    datapath: pathlib.Path,
    species: str | None = None,
    period: str | None = None,
    cams_version: str | None = None,
    input_observations: str | None = None,
    get_footprint_kwargs: dict | None = None,
    continuous: bool = True,
    chunks: dict | None = None,
) -> dict:
    """
    Parses the boundary conditions directly from the cams raw files and adds data and metadata.
    Args
        bc_input: Input used to create boundary conditions. For example:
            - a model name and version and period such as "cams_v24r1_daily"
            - a description such as "cams_uniform_mixedversion_daily" (uniform values based on CAMS average from a mix of version at daily resolution)
            Advice is to always put cams to state the model, as well as info on the cams version used and period, even though this will be put in the metadata.
        domain: Region for boundary conditions
        filepath: (List of) Path of boundary conditions file
        species: Species name
        period: period at which at resample and store the data
        cams_version: cams version to use. Put 'mix' if you want to use files from multiple cams versions.
        input_observations: input observations used to make the cams file (e.g. "surface_satellite_dm"). Put 'mix' if you want to use files from multiple input observations.
        get_footprint_kwargs: arguments passed to openghg.retrieve.get_footprint to get the grid that that will be used to store the data
        continuous: whether time stamps have to be continuous
        make_climatology: If True climatologies will be created. Not implemented yet.
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
                for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
                See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
                To disable chunking pass in an empty dictionary.
    Returns:
        Dict: Dictionary of "species_bc_input_domain" : data, metadata, attributes
    """

    xr_open_fn, filepath = open_time_nc_fn(datapath)

    filepath_list = normalise_to_filepath_list(datapath)
    cams_version, species, input_observations = _check_and_set_params(
        filepath_list, cams_version, species, input_observations
    )

    with xr_open_fn(filepath).chunk(chunks) as ds:
        # Be sure that data are sorted in ascending order (not the case for n2o latitude)
        ds = ds.sortby(list(ds.dims))

        # Resample data
        if period:
            resample_args = get_resample_args(ds.time, species, period)
            ds = ds.resample(**resample_args).mean()
        else:
            period = f"original frequency ({str(xr.infer_freq(ds.time))})"

        # Calc pressure if species is "n2o"
        if species.lower() == "n2o":
            ds = calc_altitude_from_pressure(ds)

        # Rename variables
        ds = ds.rename({"latitude": "lat", "longitude": "lon", species.upper(): "species"})

        # Interpolate vmrn/s/e/w variables
        bc_data = cams_to_domain(ds, "EUROPE", get_footprint_kwargs=get_footprint_kwargs)

        # Create time dimension if not present
        if "time" in bc_data.coords:
            bc_data = update_zero_dim(bc_data, dim="time")

    # Add new attributes
    add_attrs = dict(
        title=f"ECMWF CAMS {species} volume mixing ratios at domain edges",
        CAMS_resolution=get_gridsize(ds),
        author=os.getenv("USER"),
        date_created=str(timestamp_now()),
        files_used=(", ".join([file.name for file in filepath_list])),  # type: ignore
        CAMS_version=cams_version,
        CAMS_input_observations=input_observations,
    )

    bc_data.attrs.update(add_attrs)

    # create metadata
    metadata = make_metadata(
        bc_data,
        period,
        continuous,
        species=species,
        domain=domain,
        bc_input=bc_input,
    )

    key = "_".join((species, bc_input, domain))

    boundary_conditions_data: dict[str, dict] = {key: {}}
    boundary_conditions_data[key]["data"] = bc_data.rename({"level": "height"})
    boundary_conditions_data[key]["metadata"] = metadata

    return boundary_conditions_data
