from pathlib import Path
from typing import Callable, Dict, Literal, Optional, Union

import numpy as np
from openghg.transform import regrid_2d
import pandas as pd
import xarray as xr


chunk_type = Union[int, Dict, Literal["auto"]]


def _parse_generic(
    filepath: Path,
    species: str,
    source: str,
    domain: str,
    data_type: str,
    raw_file_hook: Callable[[Path], xr.Dataset],
    database: Optional[str] = None,
    database_version: Optional[str] = None,
    model: Optional[str] = None,
    high_time_resolution: Optional[bool] = False,
    period: Optional[Union[str, tuple]] = None,
    continuous: bool = True,
) -> Dict:
    """
    Read and parse input emissions data already in OpenGHG format.

    Args:
        filepath: Path to data file
        chunks: Chunk size to use when parsing NetCDF, useful for large datasets.
        Passing "auto" will ask xarray to calculate a chunk size.
    Returns:
        dict: Dictionary of data
    """
    from openghg.standardise.meta import assign_flux_attributes
    from openghg.store import infer_date_range, update_zero_dim
    from openghg.util import timestamp_now

    em_data = raw_file_hook(filepath)

    # Some attributes are numpy types we can't serialise to JSON so convert them
    # to their native types here
    attrs = {}
    for key, value in em_data.attrs.items():
        print(key, value, type(value))
        try:
            attrs[key] = value.item()
        except AttributeError:
            attrs[key] = value
        except ValueError:
            if isinstance(value, np.ndarray):
                attrs[key] = value.tolist()
            else:
                raise

    author_name = "OpenGHG Cloud"
    em_data.attrs["author"] = author_name

    metadata = {}
    metadata.update(attrs)

    metadata["species"] = species
    metadata["domain"] = domain
    metadata["source"] = source

    optional_keywords = {
        "database": database,
        "database_version": database_version,
        "model": model,
    }
    for key, value in optional_keywords.items():
        if value is not None:
            metadata[key] = value

    metadata["author"] = author_name
    metadata["data_type"] = data_type
    metadata["processed"] = str(timestamp_now())
    metadata["data_type"] = "emissions"
    metadata["source_format"] = "openghg"

    # As emissions files handle things slightly differently we need to check the time values
    # more carefully.
    # e.g. a flux / emissions file could contain e.g. monthly data and be labelled as 2012 but
    # contain 12 time points labelled as 2012-01-01, 2012-02-01, etc.

    # Check if time has 0-dimensions and, if so, expand this so time is 1D
    if "time" in em_data.coords:
        em_data = update_zero_dim(em_data, dim="time")

    em_time = em_data["time"]

    start_date, end_date, period_str = infer_date_range(
        em_time, filepath=filepath, period=period, continuous=continuous
    )

    metadata["start_date"] = str(start_date)
    metadata["end_date"] = str(end_date)

    metadata["max_longitude"] = round(float(em_data["lon"].max()), 5)
    metadata["min_longitude"] = round(float(em_data["lon"].min()), 5)
    metadata["max_latitude"] = round(float(em_data["lat"].max()), 5)
    metadata["min_latitude"] = round(float(em_data["lat"].min()), 5)

    metadata["time_resolution"] = "high" if high_time_resolution else "standard"
    metadata["time_period"] = period_str

    key = "_".join((species, source, domain))

    emissions_data: Dict[str, dict] = {}
    emissions_data[key] = {}
    emissions_data[key]["data"] = em_data
    emissions_data[key]["metadata"] = metadata

    emissions_data = assign_flux_attributes(emissions_data)

    return emissions_data


def parse_openghg(
    filepath: Path,
    species: str,
    source: str,
    domain: str,
    data_type: str,
    database: Optional[str] = None,
    database_version: Optional[str] = None,
    model: Optional[str] = None,
    high_time_resolution: Optional[bool] = False,
    period: Optional[Union[str, tuple]] = None,
    chunks: Union[int, Dict, Literal["auto"], None] = None,
    continuous: bool = True,
) -> Dict:
    """
    Read and parse input emissions data already in OpenGHG format.

    Args:
        filepath: Path to data file
        chunks: Chunk size to use when parsing NetCDF, useful for large datasets.
        Passing "auto" will ask xarray to calculate a chunk size.
    Returns:
        dict: Dictionary of data
    """

    def raw_file_hook(filepath: Path) -> xr.Dataset:
        return xr.open_dataset(filepath, chunks=chunks)

    return _parse_generic(
        filepath=filepath,
        species=species,
        source=source,
        domain=domain,
        raw_file_hook=raw_file_hook,
        data_type=data_type,
        database=database,
        database_version=database_version,
        model=model,
        high_time_resolution=high_time_resolution,
        period=period,
        continuous=continuous,
    )


def parse_edgar(
    filepath: Path,
    species: str,
    source: str,
    domain: str,
    data_type: str,
    database: Optional[str] = None,
    database_version: Optional[str] = None,
    model: Optional[str] = None,
    high_time_resolution: Optional[bool] = False,
    period: Optional[Union[str, tuple]] = None,
    chunks: Union[int, Dict, Literal["auto"], None] = None,
    continuous: bool = True,
    year: Optional[str] = None,
    flux_data_var: str = "fluxes",
) -> Dict:
    """
    Read and parse input emissions data downloaded from EDGAR in netCDF format.

    The expected units are kg / (m^2 s). For v8.0, these are the "flx" files from
    the EDGAR website (not the "emi" files).

    Args:
        filepath: Path to data file
        chunks: Chunk size to use when parsing NetCDF, useful for large datasets.
            Passing "auto" will ask xarray to calculate a chunk size.
    Returns:
        dict: Dictionary of data
    """
    import re
    from openghg.standardise.meta import define_species_label
    from openghg.util import molar_mass

    def raw_file_hook(filepath: Path) -> xr.Dataset:
        raw_data = xr.open_dataset(filepath, chunks=chunks)
        regridded_data = regrid_2d(
            raw_data.fluxes, in_data_var=flux_data_var, domain=domain
        )

        nonlocal year  # need to specify that we want `year` from `parse_edgar`
        if year is None:
            match = re.search(r"[0-9]{4}", filepath.name)
            if match:
                year = match.group(0)
            else:
                raise ValueError(
                    "Could not infer year, please specify the year explicitly."
                )

        regridded_data = regridded_data.expand_dims(
            {"time": [pd.to_datetime(year)]}, axis=2
        )

        # convert kg/m^2/s to mol/m^2/s
        kg_to_g = 1e3
        species_name = define_species_label(species, filepath)[0]
        species_molar_mass = molar_mass(species_name)
        regridded_data[flux_data_var] = (
            regridded_data[flux_data_var] * kg_to_g / species_molar_mass
        )
        regridded_data[flux_data_var].attrs["units"] = "mol/m2/s"

        return regridded_data

    return _parse_generic(
        filepath=filepath,
        species=species,
        source=source,
        domain=domain,
        raw_file_hook=raw_file_hook,
        data_type=data_type,
        database=database,
        database_version=database_version,
        model=model,
        high_time_resolution=high_time_resolution,
        period=period,
        continuous=continuous,
    )
