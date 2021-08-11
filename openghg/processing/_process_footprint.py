"""
This hopes to recreate the functionality of the ACRG repo function
footprints_data_merge
"""
from pandas import Timestamp
from xarray import Dataset, DataArray
from typing import List, Optional, Tuple, Union, Dict
from openghg.dataobjects import FootprintData

__all__ = ["single_site_footprint", "footprints_data_merge"]


def single_site_footprint(
    site: str,
    height: str,
    network: str,
    domain: str,
    species: str,
    start_date: Union[str, Timestamp],
    end_date: Union[str, Timestamp],
    resample_to: str = "coarsest",
    site_modifier: Optional[str] = None,
    platform: Optional[str] = None,
    instrument: Optional[str] = None,
) -> Dataset:
    """Creates a Dataset for a single site's measurement data and footprints

    Args:
        site: Site name
        height: Height of inlet in metres
        network: Network name
        resample_to: Resample the data to a given time dataset.
        Valid options are ["obs", "footprint", "coarsen"].
            - "obs" resamples the footprint to the observation time series data
            - "footprint" resamples to to the footprint time series
            - "coarsest" resamples to the data with the coarsest time resolution
        site_modifier: The name of the site given in the footprint.
                       This is useful for example if the same site footprints are run with a different met and
                       they are named slightly differently from the obs file. E.g.
                       site="DJI", site_modifier = "DJI-SAM" - station called DJI, footprint site called DJI-SAM
        platform: Observation platform used to decide whether to resample
        instrument:
        species: Species type
    Returns:
        xarray.Dataset
    """
    from openghg.processing import get_obs_surface, recombine_datasets, search
    from openghg.util import timestamp_tzaware

    start_date = timestamp_tzaware(start_date)
    end_date = timestamp_tzaware(end_date)

    resample_to = resample_to.lower()
    resample_choices = ("obs", "footprint", "coarsest")
    if resample_to not in resample_choices:
        raise ValueError(f"Invalid resample choice {resample_to} past, please select from one of {resample_choices}")

    # As we're not processing any satellite data yet just set tolerance to None
    tolerance = None
    platform = None

    # Here we want to use get_obs_surface
    obs_results = get_obs_surface(
        site=site, inlet=height, start_date=start_date, end_date=end_date, species=species, instrument=instrument
    )

    obs_data = obs_results.data

    # Save the observation data units
    try:
        units: Union[float, None] = float(obs_data.mf.attrs["units"])
    except KeyError:
        units = None
    except AttributeError:
        raise AttributeError("Unable to read mf attribute from observation data.")

    footprint_site = site
    # If the site for the footprint has a different name pass that in
    if site_modifier:
        footprint_site = site_modifier

    # Get the footprint data
    footprint_results: Dict = search(
        site=footprint_site, domain=domain, height=height, start_date=start_date, end_date=end_date, data_type="footprint"
    )  # type: ignore

    try:
        fp_site_key = list(footprint_results.keys())[0]
    except IndexError:
        raise ValueError(f"Unable to find any footprint data for {site} at a height of {height} in the {network} network.")

    footprint_keys = footprint_results[fp_site_key]["keys"]
    footprint_data = recombine_datasets(keys=footprint_keys, sort=False)

    # Align the two Datasets
    aligned_obs, aligned_footprint = align_datasets(
        obs_data=obs_data, footprint_data=footprint_data, platform=platform, resample_to=resample_to
    )

    combined_dataset = combine_datasets(dataset_A=aligned_obs, dataset_B=aligned_footprint, tolerance=tolerance)

    # Transpose to keep time in the last dimension position in case it has been moved in resample
    combined_dataset = combined_dataset.transpose(..., "time")

    if units is not None:
        combined_dataset.update({"fp": (combined_dataset.fp.dims, (combined_dataset.fp / units))})
        # if HiTRes:
        #     combined_dataset.update({"fp_HiTRes": (combined_dataset.fp_HiTRes.dims, (combined_dataset.fp_HiTRes / units))})

    return combined_dataset


def footprints_data_merge(
    site: str,
    height: str,
    network: str,
    domain: str,
    species: str,
    start_date: Union[str, Timestamp],
    end_date: Union[str, Timestamp],
    resample_to: str = "coarsest",
    site_modifier: Optional[str] = None,
    platform: Optional[str] = None,
    instrument: Optional[str] = None,
    load_flux: Optional[bool] = True,
    flux_sources: Optional[Union[str, List]] = None,
    load_bc: Optional[bool] = True,
    calc_timeseries: Optional[bool] = True,
    calc_bc: Optional[bool] = True,
    time_resolution: Optional[str] = "standard",
) -> FootprintData:
    """
    TODO - Should this be renamed?

    Args:
        site: Three letter site code
        height: Height of inlet in metres
        network: Network name
        domain: Domain name
        start_date: Start date
        end_date: End date
        resample_to: Overrides resampling to coarsest time resolution, can be one of ["coarsest", "footprint", "obs"]
        site_modifier: The name of the site given in the footprint.
                This is useful for example if the same site footprints are run with a different met and
                they are named slightly differently from the obs file. E.g.
                site="DJI", site_modifier = "DJI-SAM" - station called DJI, footprint site called DJI-SAM
        platform: Observation platform used to decide whether to resample
        instrument: Instrument name
        species: Species name
        load_flux: Load flux
        flux_sources: Flux source names
        load_bc: Load boundary conditions (not currently implemented)
        calc_timeseries: Calculate timeseries data (not currently implemented)
        calc_bc: Calculate boundary conditions (not currently implemented)
        time_resolution: One of ["standard", "high"]
    Returns:
        dict: Dictionary footprint data objects
    """
    # First get the site data
    combined_dataset = single_site_footprint(
        site=site,
        height=height,
        network=network,
        domain=domain,
        start_date=start_date,
        end_date=end_date,
        resample_to=resample_to,
        site_modifier=site_modifier,
        platform=platform,
        instrument=instrument,
        species=species,
    )

    # So here we iterate over the emissions types and get the fluxes
    flux_dict = {}
    if load_flux:
        if flux_sources is None:
            raise ValueError("If you want to load flux you must pass a flux source")

        flux_dict["standard"] = get_flux(
            species=species,
            domain=domain,
            sources=flux_sources,
            time_resolution=time_resolution,
            start_date=start_date,
            end_date=end_date,
        )

        if time_resolution == "high":
            flux_dict["high_time_res"] = get_flux(
                species=species,
                domain=domain,
                sources=flux_sources,
                time_resolution=time_resolution,
                start_date=start_date,
                end_date=end_date,
            )

    # Calculate model time series, if required
    if calc_timeseries:
        combined_dataset = add_timeseries(combined_dataset, flux_dict)

    return FootprintData(
        data=combined_dataset, metadata={}, flux=flux_dict, bc={}, species=species, scales="scale", units="units"
    )


def combine_datasets(dataset_A: Dataset, dataset_B: Dataset, method: str = "ffill", tolerance: Optional[float] = None) -> Dataset:
    """Merges two datasets and re-indexes to the first dataset.

        If "fp" variable is found within the combined dataset,
        the "time" values where the "lat", "lon" dimensions didn't match are removed.

    Args:
        dataset_A: First dataset to merge
        dataset_B: Second dataset to merge
        method: One of None, nearest, ffill, bfill.
                See xarray.DataArray.reindex_like for list of options and meaning.
                Defaults to ffill (forward fill)
        tolerance: Maximum allowed tolerance between matches.
    Returns:
        xarray.Dataset: Combined dataset indexed to dataset_A
    """
    import numpy as np

    if indexes_match(dataset_A, dataset_B):
        dataset_B_temp = dataset_B
    else:
        dataset_B_temp = dataset_B.reindex_like(other=dataset_A, method=method, tolerance=tolerance)  # type: ignore

    merged_ds = dataset_A.merge(other=dataset_B_temp)

    if "fp" in merged_ds:
        if all(k in merged_ds.fp.dims for k in ("lat", "long")):
            flag = np.where(np.isfinite(merged_ds.fp.mean(dim=["lat", "lon"]).values))
            merged_ds = merged_ds[dict(time=flag[0])]

    return merged_ds


def indexes_match(dataset_A: Dataset, dataset_B: Dataset) -> bool:
    """Check if two datasets need to be reindexed_like for combine_datasets

    Args:
        dataset_A: First dataset to check
        dataset_B: Second dataset to check
    Returns:
        bool: True if indexes match, else False
    """
    import numpy as np

    common_indices = (key for key in dataset_A.indexes.keys() if key in dataset_B.indexes.keys())

    for index in common_indices:
        if not len(dataset_A.indexes[index]) == len(dataset_B.indexes[index]):
            return False

        # Check number of values that are not close (testing for equality with floating point)
        if index == "time":
            # For time override the default to have ~ second precision
            rtol = 1e-10
        else:
            rtol = 1e-5

        index_diff = np.sum(
            ~np.isclose(dataset_A.indexes[index].values.astype(float), dataset_B.indexes[index].values.astype(float), rtol=rtol)
        )

        if not index_diff == 0:
            return False

    return True


def align_datasets(
    obs_data: Dataset,
    footprint_data: Dataset,
    resample_to: Optional[str] = "coarsest",
    platform: Optional[str] = None,
) -> Tuple[Dataset, Dataset]:
    """Slice and resample two datasets to align along time

    This slices the date to the smallest time frame
    spanned by both the footprint and obs, then resamples the data
    using the mean to the one with coarsest median resolution
    starting from the sliced start date.

    Args:
        obs_data: Observations Dataset
        footprint_data: Footprint Dataset
        resample_to: Overrides resampling to coarsest time resolution, can be one of ["coarsest", "footprint", "obs"]
        platform: Observation platform used to decide whether to resample
    Returns:
        tuple: Two xarray.Dataset with aligned time dimensions
    """
    import numpy as np
    from pandas import Timedelta

    if platform is not None:
        platform = platform.lower()
        # Do not apply resampling for "satellite" (but have re-included "flask" for now)
        if platform == "satellite":
            return obs_data, footprint_data

    # Get the period of measurements in time
    obs_attributes = obs_data.attrs
    if "averaged_period" in obs_attributes:
        obs_data_period_s = int(obs_attributes["averaged_period"])
    elif "sampling_period" in obs_attributes:
        obs_data_period_s = int(obs_attributes["sampling_period"])
    else:
        # Attempt to derive sampling period from frequency of data
        obs_data_period_s = np.nanmedian((obs_data.time.data[1:] - obs_data.time.data[0:-1]) / 1e9).astype("int64")

        obs_data_period_s_min = np.diff(obs_data.time.data).min() / 1e9
        obs_data_period_s_max = np.diff(obs_data.time.data).max() / 1e9

        # Check if the periods differ by more than 1 second
        if np.isclose(obs_data_period_s_min, obs_data_period_s_max, 1):
            raise ValueError("Sample period can be not be derived from observations")

    obs_data_timeperiod = Timedelta(seconds=obs_data_period_s)

    # Derive the footprint period from the frequency of the data
    footprint_data_period_ns = np.nanmedian((footprint_data.time.data[1:] - footprint_data.time.data[0:-1]).astype("int64"))
    footprint_data_timeperiod = Timedelta(footprint_data_period_ns, unit="ns")

    # Here we want timezone naive Timestamps
    obs_startdate = Timestamp(obs_data.time[0].values)
    obs_enddate = Timestamp(obs_data.time[-1].values)
    footprint_startdate = Timestamp(footprint_data.time[0].values)
    footprint_enddate = Timestamp(footprint_data.time[-1].values)

    start_date = max(obs_startdate, footprint_startdate)
    end_date = min(obs_enddate, footprint_enddate)

    # Subtract half a second to ensure lower range covered
    start_slice = start_date - Timedelta("0.5s")
    # Add half a second to ensure upper range covered
    end_slice = end_date + Timedelta("0.5s")

    obs_data = obs_data.sel(time=slice(start_slice, end_slice))
    footprint_data = footprint_data.sel(time=slice(start_slice, end_slice))

    # Only non satellite datasets with different periods need to be resampled
    timeperiod_diff_s = np.abs(obs_data_timeperiod - footprint_data_timeperiod).total_seconds()
    tolerance = 1e-9  # seconds
    if timeperiod_diff_s >= tolerance:
        base = start_date.hour + start_date.minute / 60.0 + start_date.second / 3600.0

        if (obs_data_timeperiod >= footprint_data_timeperiod) or resample_to == "obs":

            resample_period = str(round(obs_data_timeperiod / np.timedelta64(1, "h"), 5)) + "H"

            footprint_data = footprint_data.resample(indexer={"time": resample_period}, base=base).mean()

        elif obs_data_timeperiod < footprint_data_timeperiod or resample_to == "footprint":

            resample_period = str(round(footprint_data_timeperiod / np.timedelta64(1, "h"), 5)) + "H"

            obs_data = obs_data.resample(indexer={"time": resample_period}, base=base).mean()

    return obs_data, footprint_data


def get_flux(
    species: Union[str, List[str]],
    sources: Union[str, List[str]],
    domain: Union[str, List[str]],
    start_date: Optional[Timestamp] = None,
    end_date: Optional[Timestamp] = None,
    time_resolution: Optional[str] = "standard",
) -> Dataset:
    """
    The flux function reads in all flux files for the domain and species as an xarray Dataset.
    Note that at present ALL flux data is read in per species per domain or by emissions name.
    To be consistent with the footprints, fluxes should be in mol/m2/s.

    Args:
        species: Species name
        sources: Source name
        domain: Domain e.g. EUROPE
        start_date: Start date
        end_date: End date
        time_resolution: One of ["standard", "high"]
    Returns:
        xarray.Dataset: combined dataset of all matching flux files
    """
    from openghg.processing import search, recombine_datasets
    from openghg.util import timestamp_epoch, timestamp_now

    if start_date is None:
        start_date = timestamp_epoch()
    if end_date is None:
        end_date = timestamp_now()

    results: Dict = search(
        species=species,
        source=sources,
        domain=domain,
        time_resolution=time_resolution,
        start_date=start_date,
        end_date=end_date,
        data_type="emissions",
    )  # type: ignore

    # TODO - more than one emissions file
    try:
        em_key = list(results.keys())[0]
    except IndexError:
        raise ValueError(f"Unable to find any footprint data for {domain} for {species}.")

    data_keys = results[em_key]["keys"]
    em_ds = recombine_datasets(keys=data_keys, sort=False)

    # Check for level coordinate. If one level, assume surface and drop
    if "lev" in em_ds.coords:
        if len(em_ds.lev) > 1:
            raise ValueError("Error: More than one flux level")

        return em_ds.drop_vars(names="lev")

    return em_ds


def add_timeseries(combined_dataset: Dataset, flux_dict: Dict) -> Dataset:
    """
    Add timeseries mole fraction values in footprint_data_merge

    Args:
        combined_dataset [Dataset]:
            output created during footprint_data_merge
        flux_dict [dict]:
            Dictionary containing flux datasets
    """
    # TODO: Extend to include multiple sources
    # TODO: Add ability to merge high time resolution footprints (e.g. species as co2)
    for key, flux_ds in flux_dict.items():
        if key != "high_time_res":
            flux_reindex = flux_ds.reindex_like(combined_dataset, "ffill")
            combined_dataset["mf_mod"] = DataArray(
                (combined_dataset.fp * flux_reindex.flux).sum(["lat", "lon"]), coords={"time": combined_dataset.time}
            )
        else:
            print("Unable to create modelled mole fraction for high time resolution datasets yet.")

    return combined_dataset
