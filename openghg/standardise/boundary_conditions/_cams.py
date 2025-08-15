import os
import logging
from pathlib import Path

import xarray as xr
import numpy as np

from openghg.util import timestamp_now, open_time_nc_fn
from openghg.store import infer_date_range, update_zero_dim

logger = logging.getLogger("openghg.standardise.boundary_conditions")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_cams(
    bc_input: str,
    domain: str,
    filepath: str | Path | list[str | Path],
    species: str | None = None,
    period: str | None = None,
    cams_version: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    input_observations: str | None = None,
    get_footprint_kwargs: dict | None = None,
    continuous: bool = True,
    make_climatology: bool = False,
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
        filepath: Path of boundary conditions file
        species: Species name
        period: period at which at resample and store the data
        cams_version: cams version to use. Put 'mix' if you want to use files from multiple cams versions.
        start_date: starting date to which restrict the filelist provided
        end_date: ending date to which restrict the filelist provided
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

    # Select files and set paremeters
    filepath_p = _select_files_from_dates(filepath, start_date, end_date)
    cams_version, species, input_observations = _check_and_set_params(
        filepath_p, cams_version, species, input_observations
    )

    # Open CAMS data
    xr_open_fn, filepath_p = open_time_nc_fn(filepath_p)  # type:ignore

    with xr_open_fn(filepath_p).chunk(chunks) as ds:
        # Reorder dataset
        ds = _reorder_dataset(ds)

        # Check time resolution
        org_period = (ds.time.values[1:] - ds.time.values[:-1]).max()
        thres_dict = {
            "3h": np.timedelta64(3, "h"),
            "daily": np.timedelta64(1, "D"),
            "monthly": np.timedelta64(31, "D"),
            "yearly": np.timedelta64(365, "D"),
        }
        if period and org_period > thres_dict[period]:
            raise ValueError("Original time resolution is coarser than targeted one.")

        # create climatology if required
        if make_climatology:
            raise ValueError("options not moved from ACRG code yet. Don't hesitate to do if you need it :)")
            # cams_seasonal = climatology.monthly_cycle(cams_ds)
            # cams_ds       = climatology.add_full_time(cams_seasonal, start = start, end = end)

        # Calculate altitude from pressure for n2o
        if species.lower() == "n2o":
            ds["pressure"] = ds.ap + ds.bp * ds.Psurf
            scale_height = 7.64e3  # in metres
            ds["altitude"] = -scale_height * np.log(ds.pressure / ds.Psurf)

        # get default variable names
        ds = ds.rename(
            {
                species.upper(): "species",
                "latitude": "lat",
                "longitude": "lon",
            }
        )
        ds = ds[["species", "altitude"]]

        lat_grid = (ds["lat"][1:] - ds["lat"][:-1]).mean().values
        lon_grid = (ds["lon"][1:] - ds["lon"][:-1]).mean().values
        gridsize = f"{lat_grid} x {lon_grid}"

        # Resample to target resolution
        alias_period = {"daily": "D", "monthly": "MS", "yearly": "YS"}
        closed = "right" if species =="n2o" else "left"
        ds = ds.resample(time=alias_period.get(period,period),
                         closed = closed).mean()

        # Convert altitude coordinates from hlevel to level
        if species.lower() == "co2":
            raise ValueError(
                "convertCAMSaltitude from ACRG has not be transcripted here for co2. Don't hesitate to do."
            )
        z = 0.5 * (
            ds["altitude"].isel(hlevel=slice(0, -1)).values
            + ds["altitude"].isel(hlevel=slice(1, None)).values
        )
        z_dims = tuple([dim if dim != "hlevel" else "level" for dim in ds["altitude"].dims])
        ds = ds.assign(**{"z": (z_dims, z.data)})

        # find the correct unit conversion between mol/mol and species specific parts-per- units
        unit_converter = {"ppt": 1e-12, "ppb": 1e-9, "ppm": 1e-6, "1e-9 mol mol-1": 1e-9}
        old_unit = ds["species"].attrs["units"]
        conversion = float(unit_converter.get(old_unit, old_unit))
        ds["species"].values *= conversion
        ds["species"].attrs["units"] = 1

        # Get coords from reference footprints
        if get_footprint_kwargs:
            from openghg.retrieve import get_footprint

            fp = get_footprint(**get_footprint_kwargs).data
            fp_lat = fp["lat"].values
            fp_lon = fp["lon"].values
            fp_height = fp["height"].values
        else:
            raise ValueError(
                "For now get_footprint_kwargs is not optionnal. Might do something with openghg_defs.domain_info_file"
            )

        # Select the gridcells closest to the edges of the  domain and make sure outside of fp
        lat_n = ds["lat"].where(ds["lat"] > max(fp_lat)).min()
        lat_s = ds["lat"].where(ds["lat"] < min(fp_lat)).max()
        lon_e = ds["lon"].where(ds["lon"] > max(fp_lon)).min()
        lon_w = ds["lon"].where(ds["lon"] < min(fp_lon)).max()

        # Select the boundary data
        north = ds.sel(lat=lat_n, lon=slice(lon_w, lon_e)).drop_vars("lat")
        south = ds.sel(lat=lat_s, lon=slice(lon_w, lon_e)).drop_vars("lat")
        east = ds.sel(lon=lon_e, lat=slice(lat_s, lat_n)).drop_vars("lon")
        west = ds.sel(lon=lon_w, lat=slice(lat_s, lat_n)).drop_vars("lon")

        # Interp on footprint grid
        vmr_n = _interp_dim(_interp_dim(north, fp_height, "level"), fp_lon, "lon").rename(
            {"species": "vmr_n"}
        )
        vmr_s = _interp_dim(_interp_dim(south, fp_height, "level"), fp_lon, "lon").rename(
            {"species": "vmr_s"}
        )
        vmr_e = _interp_dim(_interp_dim(east, fp_height, "level"), fp_lat, "lat").rename({"species": "vmr_e"})
        vmr_w = _interp_dim(_interp_dim(west, fp_height, "level"), fp_lat, "lat").rename({"species": "vmr_w"})

        bc_data = xr.merge([vmr_n, vmr_s, vmr_e, vmr_w])

        bc_data.attrs["title"] = f"ECMWF CAMS {species} volume mixing ratios at domain edges"
        bc_data.attrs["CAMS_resolution"] = gridsize
        bc_data.attrs["author"] = os.getenv("USER")
        bc_data.attrs["date_created"] = str(timestamp_now())
        bc_data.attrs["files_used"] = (
            ", ".join([file.name for file in filepath_p]) if len(filepath_p) == 1 else filepath_p[0].name
        )
        bc_data.attrs["CAMS_version"] = cams_version
        bc_data.attrs["CAMS_input_observations"] = input_observations

    attrs = {}
    for key, value in bc_data.attrs.items():
        try:
            attrs[key] = value.item()
        except AttributeError:
            attrs[key] = value

    metadata = {}
    metadata.update(attrs)

    metadata["species"] = species
    metadata["domain"] = domain
    metadata["bc_input"] = bc_input
    metadata["processed"] = str(timestamp_now())

    # Check if time has 0-dimensions and, if so, expand this so time is 1D
    if "time" in bc_data.coords:
        bc_data = update_zero_dim(bc_data, dim="time")

    bc_time = bc_data["time"]

    # If filepath is a single file, the naming scheme of this file can be used
    # as one factor to try and determine the period.
    # If multiple files, this input isn't needed.
    if len(filepath_p) == 1:
        input_filepath = filepath_p[0]
    else:
        input_filepath = None

    start_date, end_date, period_str = infer_date_range(
        bc_time, filepath=input_filepath, period=period, continuous=continuous
    )

    metadata["start_date"] = str(start_date)
    metadata["end_date"] = str(end_date)

    metadata["max_longitude"] = round(float(bc_data["lon"].max()), 5)
    metadata["min_longitude"] = round(float(bc_data["lon"].min()), 5)
    metadata["max_latitude"] = round(float(bc_data["lat"].max()), 5)
    metadata["min_latitude"] = round(float(bc_data["lat"].min()), 5)
    metadata["min_height"] = round(float(bc_data["height"].min()), 5)
    metadata["max_height"] = round(float(bc_data["height"].max()), 5)

    metadata["time_period"] = period_str

    key = "_".join((species, bc_input, domain))

    boundary_conditions_data: dict[str, dict] = {key: {}}
    boundary_conditions_data[key]["data"] = bc_data
    boundary_conditions_data[key]["metadata"] = metadata

    return boundary_conditions_data


def _reorder_dataset(ds: xr.Dataset) -> xr.Dataset:
    """
    Sort in acending order the dims of dataset when they are in descending order
    Args
        ds: dataset to sort
    Returns
        dataset sorted
    """
    for dim in ds.dims:
        if ds[dim].isel({dim: -1}) < ds[dim].isel({dim: 0}):
            ds = ds.isel({dim: slice(None, None, -1)})
    return ds


def _interp_dim(ds: xr.Dataset, fp_xx: np.ndarray, dim_to_interp: str) -> xr.Dataset:
    """
    Interpolate variable "species" of dataset along "dim_to_interp" using "fp_xx" as new coordinates.
    Args:
        ds: dataset to interpolate. Only the variable "species" will be interpolate.
        fp_xx: coordinates to interpolate on. Usually a coordinate array from a footprint.
        dim_to_interp: dimension name in the dataset. If "level", the variable "z" will be used as coordinates instead of the level coordinates.
    Returns
        Interpolated dataset
    """
    dims = list(ds["species"].dims)
    other_dims = [dim for dim in dims if dim != dim_to_interp]

    ds_to_concat = list()
    for x in ds[other_dims[0]]:
        ds_inter = list()
        for y in ds[other_dims[1]]:
            tmp = ds.sel({other_dims[0]: x, other_dims[1]: y})
            if dim_to_interp == "level":
                tmp["level"] = tmp["z"]

            fill_value = tuple(
                [tmp["species"].isel({dim_to_interp: 0}), tmp["species"].isel({dim_to_interp: -1})]
            )
            res = tmp["species"].interp(
                {dim_to_interp: fp_xx},
                method="linear",
                kwargs={"fill_value": fill_value, "bounds_error": False},
            )
            ds_inter.append(res)

        ds_to_concat.append(xr.concat(ds_inter, dim=other_dims[1]).to_dataset())

    output = xr.concat(ds_to_concat, dim=other_dims[0])

    if dim_to_interp == "level":
        output = output.rename({"level": "height"})

    return output


def _select_files_from_dates(
    filepath: str | Path | list[str | Path], start_date: str | None = None, end_date: str | None = None
) -> list[Path]:
    """
    Convert filepath to a list of path and retain only the one between start_date and end_date, based on the filenmaes.
    Args:
        filepath: (list of) filepath(s) to use
        start_date: starting date to which restrict the filelist provided
        end_date: ending date to which restrict the filelist provided
    Returns:
        List of selected files paths.
    """
    if not isinstance(filepath, list):
        filepath = [
            filepath,
        ]

    new_filepath = []

    for file in filepath:
        file_date = Path(file).name.split("_")[-1][:-3]
        if start_date and (int(file_date) < int(start_date.replace("-", "")[:6])):
            continue
        if end_date and (int(file_date) > int(end_date.replace("-", "")[:6])):
            continue
        new_filepath.append(Path(file))

    if len(new_filepath) == 0:
        raise ValueError(f"No files found bewteen dates {start_date} and {end_date}.")

    new_filepath.sort()

    return new_filepath


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
