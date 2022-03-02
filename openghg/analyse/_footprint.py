"""
This hopes to recreate the functionality of the ACRG repo function
footprints_data_merge
"""
from pandas import Timestamp
from xarray import Dataset, DataArray
from typing import Optional, Tuple, Union, Dict, Any
from openghg.dataobjects import FootprintData

# from openghg.dataobjects import FluxData


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
        Valid options are ["obs", "footprints", "coarsen"].
            - "obs" resamples the footprints to the observation time series data
            - "footprints" resamples to to the footprints time series
            - "coarsest" resamples to the data with the coarsest time resolution
        site_modifier: The name of the site given in the footprints.
                       This is useful for example if the same site footprints are run with a different met and
                       they are named slightly differently from the obs file. E.g.
                       site="DJI", site_modifier = "DJI-SAM" - station called DJI, footprints site called DJI-SAM
        platform: Observation platform used to decide whether to resample
        instrument:
        species: Species type
    Returns:
        xarray.Dataset
    """
    from openghg.retrieve import get_obs_surface, get_footprint
    from openghg.util import timestamp_tzaware

    start_date = timestamp_tzaware(start_date)
    end_date = timestamp_tzaware(end_date)

    resample_to = resample_to.lower()
    resample_choices = ("obs", "footprints", "coarsest")
    if resample_to not in resample_choices:
        raise ValueError(
            f"Invalid resample choice {resample_to} past, please select from one of {resample_choices}"
        )

    # As we're not retrieve any satellite data yet just set tolerance to None
    tolerance = None
    platform = None

    # Here we want to use get_obs_surface
    obs_results = get_obs_surface(
        site=site,
        inlet=height,
        start_date=start_date,
        end_date=end_date,
        species=species,
        instrument=instrument,
    )

    obs_data = obs_results.data

    # Save the observation data units
    try:
        units: Union[float, None] = float(obs_data.mf.attrs["units"])
    except KeyError:
        units = None
    except AttributeError:
        raise AttributeError("Unable to read mf attribute from observation data.")

    # If the site for the footprints has a different name, pass that in
    if site_modifier:
        footprint_site = site_modifier
    else:
        footprint_site = site

    # Try to find appropriate footprints file first with and then without species name
    try:
        footprint = get_footprint(
            site=footprint_site,
            domain=domain,
            height=height,
            start_date=start_date,
            end_date=end_date,
            species=species,
        )
    except ValueError:
        footprint = get_footprint(
            site=footprint_site,
            domain=domain,
            height=height,
            start_date=start_date,
            end_date=end_date,
        )

    # TODO: Add checks for particular species e.g. co2 and short-lived species
    # which should have a specific footprints available rather than the generic one

    # Extract dataset
    footprint_data = footprint.data

    # Align the two Datasets
    aligned_obs, aligned_footprint = align_datasets(
        obs_data=obs_data,
        footprint_data=footprint_data,
        platform=platform,
        resample_to=resample_to,
    )

    combined_dataset = combine_datasets(
        dataset_A=aligned_obs, dataset_B=aligned_footprint, tolerance=tolerance
    )

    # Transpose to keep time in the last dimension position in case it has been moved in resample
    combined_dataset = combined_dataset.transpose(..., "time")

    if units is not None:
        combined_dataset["fp"].values = combined_dataset["fp"].values / units
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
    flux_source: Optional[str] = None,
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
        resample_to: Overrides resampling to coarsest time resolution, can be one of ["coarsest", "footprints", "obs"]
        site_modifier: The name of the site given in the footprints.
                This is useful for example if the same site footprints are run with a different met and
                they are named slightly differently from the obs file. E.g.
                site="DJI", site_modifier = "DJI-SAM" - station called DJI, footprints site called DJI-SAM
        platform: Observation platform used to decide whether to resample
        instrument: Instrument name
        species: Species name
        load_flux: Load flux
        flux_source: Flux source name
        load_bc: Load boundary conditions (not currently implemented)
        calc_timeseries: Calculate timeseries data (not currently implemented)
        calc_bc: Calculate boundary conditions (not currently implemented)
        time_resolution: One of ["standard", "high"]
    Returns:
        dict: Dictionary footprints data objects
    """
    from openghg.retrieve import get_flux
    from pandas import Timedelta

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
        if flux_source is None:
            raise ValueError("If you want to load flux you must pass a flux source")

        flux_dict["standard"] = get_flux(
            species=species,
            domain=domain,
            source=flux_source,
            time_resolution=time_resolution,
            start_date=start_date,
            end_date=end_date,
        ).data

        if time_resolution == "high":

            # TODO: Check appropriate date range and file formats for other species
            if species == "co2":
                max_h_back = str(combined_dataset["H_back"][-1].values) + "H"
                if isinstance(start_date, str):
                    start_date = Timestamp(start_date)

                start_date_hr = start_date - Timedelta(max_h_back)
            else:
                start_date_hr = start_date

            flux_dict["high_time_res"] = get_flux(
                species=species,
                domain=domain,
                source=flux_source,
                time_resolution=time_resolution,
                start_date=start_date_hr,
                end_date=end_date,
            ).data

    # Calculate model time series, if required
    if calc_timeseries:
        combined_dataset = add_timeseries(combined_dataset=combined_dataset, flux_dict=flux_dict)

    return FootprintData(
        data=combined_dataset,
        metadata={},
        flux=flux_dict,
        bc={},
        species=species,
        scales="scale",
        units="units",
    )


def combine_datasets(
    dataset_A: Dataset,
    dataset_B: Dataset,
    method: str = "ffill",
    tolerance: Optional[float] = None,
) -> Dataset:
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
            ~np.isclose(
                dataset_A.indexes[index].values.astype(float),
                dataset_B.indexes[index].values.astype(float),
                rtol=rtol,
            )
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
    spanned by both the footprints and obs, then resamples the data
    using the mean to the one with coarsest median resolution
    starting from the sliced start date.

    Args:
        obs_data: Observations Dataset
        footprint_data: Footprint Dataset
        resample_to: Overrides resampling to coarsest time resolution, can be one of ["coarsest", "footprints", "obs"]
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

    # Whether sampling period is present or we need to try to infer this
    infer_sampling_period = False
    # Get the period of measurements in time
    obs_attributes = obs_data.attrs
    if "averaged_period" in obs_attributes:
        obs_data_period_s = float(obs_attributes["averaged_period"])
    elif "sampling_period" in obs_attributes:
        sampling_period = obs_attributes["sampling_period"]
        if sampling_period == "NOT_SET":
            infer_sampling_period = True
        else:
            obs_data_period_s = float(sampling_period)
        obs_data_period_s = float(obs_attributes["sampling_period"])
    elif "sampling_period_estimate" in obs_attributes:
        estimate = obs_attributes["sampling_period_estimate"]
        print(f"WARNING: Using estimated sampling period of {estimate}s for observational data")
        obs_data_period_s = float(estimate)
    else:
        infer_sampling_period = True

    if infer_sampling_period:
        # Attempt to derive sampling period from frequency of data
        obs_data_period_s = np.nanmedian((obs_data.time.data[1:] - obs_data.time.data[0:-1]) / 1e9).astype(
            "float32"
        )

        obs_data_period_s_min = np.diff(obs_data.time.data).min() / 1e9
        obs_data_period_s_max = np.diff(obs_data.time.data).max() / 1e9

        # Check if the periods differ by more than 1 second
        if np.isclose(obs_data_period_s_min, obs_data_period_s_max, 1):
            raise ValueError("Sample period can be not be derived from observations")

    obs_data_timeperiod = Timedelta(seconds=obs_data_period_s)

    # Derive the footprints period from the frequency of the data
    footprint_data_period_ns = np.nanmedian(
        (footprint_data.time.data[1:] - footprint_data.time.data[0:-1]).astype("int64")
    )
    footprint_data_timeperiod = Timedelta(footprint_data_period_ns, unit="ns")

    # Here we want timezone naive Timestamps
    # Add sampling period to end date to make sure resample includes these values when matching
    obs_startdate = Timestamp(obs_data.time[0].values)
    obs_enddate = Timestamp(obs_data.time[-1].values) + Timedelta(obs_data_timeperiod, unit="seconds")
    footprint_startdate = Timestamp(footprint_data.time[0].values)
    footprint_enddate = Timestamp(footprint_data.time[-1].values) + Timedelta(
        footprint_data_timeperiod, unit="nanoseconds"
    )

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

        if resample_to == "coarsest":
            if obs_data_timeperiod >= footprint_data_timeperiod:
                resample_to = "obs"
            elif obs_data_timeperiod < footprint_data_timeperiod:
                resample_to = "footprints"

        if resample_to == "obs":

            resample_period = str(round(obs_data_timeperiod / np.timedelta64(1, "h"), 5)) + "H"

            footprint_data = footprint_data.resample(indexer={"time": resample_period}, base=base).mean()

        elif resample_to == "footprints":

            resample_period = str(round(footprint_data_timeperiod / np.timedelta64(1, "h"), 5)) + "H"

            obs_data = obs_data.resample(indexer={"time": resample_period}, base=base).mean()

    return obs_data, footprint_data


def add_timeseries(combined_dataset: Dataset, flux_dict: Dict[str, Dataset]) -> Dataset:
    """
    Add timeseries mole fraction values in footprint_data_merge

    Args:
        combined_dataset [Dataset]:
            output created during footprint_data_merge
        flux_dict [dict]:
            Dictionary containing flux datasets
    """
    # TODO: Extend to include multiple sources
    # TODO: Improve ability to merge high time resolution footprints (e.g. species as co2)
    # What do we expect flux_dict to look like?
    for key, flux_ds in flux_dict.items():
        if key == "high_time_res":
            mf_mod: DataArray = timeseries_HiTRes(combined_dataset, flux_ds)
            name = "mf_mod_high_res"
            # TODO: May want to reindex afterwards? But can be expensive operation.
        else:
            # flux_reindex = flux_ds.reindex_like(combined_dataset, 'ffill')
            # combined_dataset['mf_mod'] = DataArray((combined_dataset.fp * flux_reindex.flux).sum(["lat", "lon"]), coords={'time': combined_dataset.time})
            mf_mod = timeseries_integrated(combined_dataset, flux_ds)
            name = "mf_mod"

        combined_dataset[name] = DataArray(mf_mod, coords={"time": combined_dataset.time})

    return combined_dataset


def timeseries_integrated(combined_dataset: Dataset, flux_ds: Dataset) -> DataArray:
    """
    Calculate modelled mole fraction timeseries using integrated footprints data.

    Args:
        combined_dataset [Dataset]:
            output created during footprint_data_merge
        flux_ds [Dataset]:
            Dataset containing flux values

    Returns:
        DataArray :
            Modelled mole fraction timeseries, dimensions = (time)

    TODO: Also allow flux_mod to be returned as an option? Include flags if so.
    """
    flux_reindex = flux_ds.reindex_like(combined_dataset, "ffill")
    flux_mod: DataArray = combined_dataset.fp * flux_reindex.flux
    timeseries: DataArray = flux_mod.sum(["lat", "lon"])
    # combined_dataset['mf_mod'] = DataArray((combined_dataset.fp * flux_reindex.flux).sum(["lat", "lon"]), coords={'time': combined_dataset.time})

    return timeseries


def timeseries_HiTRes(
    combined_dataset: Dataset,
    flux_ds: Dataset,
    averaging: Optional[str] = None,
    output_TS: Optional[bool] = True,
    output_fpXflux: Optional[bool] = False,
) -> Any:
    """
     Calculate modelled mole fraction timeseries using high time resolution
     footprints data and emissions data.

    Args:
         combined_dataset:
             output created during footprint_data_merge. Expect dataset containing
             "fp_HiTRes" data variable with dimensions (lat, lon, time, H_back).
             Where H_back represents the hourly footprints related to the footprints
             time.
         flux_ds:
             Dataset containing flux values. Expect dataset containing "flux"
             data variable with dimensions (lat, lon, time).
         averaging:
             Time resolution to use to average the time dimension.
             Default = None
         output_TS:
             Whether to output the modelled mole fraction timeseries DataArray.
             Default = True
         output_fpXflux:
             Whether to output the modelled flux map DataArray used to create
             the timeseries.
             Default = False
     Returns:
         DataArray / DataArray :
             Modelled mole fraction timeseries, dimensions = (time)
             Modelled flux map, dimensions = (lat, lon, time)

         If one of output_TS and output_fpXflux are True:
             DataArray is returned for the respective output

         If both output_TS and output_fpXflux are both True:
             Both DataArrays are returned.

     TODO: Low frequency flux values may need to be selected from the month before
     (currently selecting the same month).
     TODO: Indexing for low frequency flux should be checked to make sure this
     allows for crossing over the end of the year.
     TODO: Currently using pure dask arrays (based on Hannah's original code)
     but would be good to update this to add more pre-indexing using xarray
     and/or use dask as part of datasets.
     TODO: May want to update this to not rely on indexing when selecting
     the appropriate flux values. At the moment this solution has been chosen
     because selecting on a dimension, rather than indexing, can be *very* slow
     depending on the operations performed beforehand on the Dataset (e.g.
     resample and reindex)
     TODO: This code currently resamples the frequency to be regular. This will
     have no effect if the time frequency was already regular but this may
     not be what we want and may want to add extra code to remove any NaNs, if
     they are introduced or to find a way to remove this requirement.
     TODO: mypy having trouble with different types options and incompatible types,
     included as Any for now.
    """
    import numpy as np
    import dask.array as da  # type: ignore
    from tqdm import tqdm
    from pandas import date_range
    from math import gcd

    fp_HiTRes = combined_dataset.fp_HiTRes

    # Calculate time resolution for both the flux and footprints data
    nanosecond_to_hour = 1 / (1e9 * 60.0 * 60.0)
    flux_res_H = int(flux_ds.time.diff(dim="time").values.mean() * nanosecond_to_hour)
    fp_res_time_H = int(fp_HiTRes.time.diff(dim="time").values.mean() * nanosecond_to_hour)

    fp_res_Hback_H = int(fp_HiTRes["H_back"].diff(dim="H_back").values.mean())

    # Define resolution on time dimension in number in hours
    if averaging:
        try:
            time_res_H = int(averaging)
            time_resolution = f"{time_res_H}H"
        except (ValueError, TypeError):
            time_res_H = int(averaging[0])
            time_resolution = averaging
    else:
        # If not specified derive from time from combined dataset
        time_res_H = fp_res_time_H
        time_resolution = f"{time_res_H}H"

    # Resample fp timeseries to match time resolution
    if fp_res_time_H != time_res_H:
        fp_HiTRes = fp_HiTRes.resample(time=time_resolution).ffill()

    # Define resolution on high frequency dimension in number of hours
    # At the moment this is matched to the Hback dimension
    time_hf_res_H = fp_res_Hback_H

    # Only allow for high frequency resolution < 24 hours
    if time_hf_res_H > 24:
        raise ValueError(f"High frequency resolution must be <= 24 hours. Current: {time_hf_res_H}H")
    elif 24 % time_hf_res_H != 0 or 24 % time_hf_res_H != 0.0:
        raise ValueError(
            f"High frequency resolution must exactly divide into 24 hours. Current: {time_hf_res_H}H"
        )

    # Find the greatest common denominator between time and high frequency resolutions.
    # This is needed to make sure suitable flux frequency is used to allow for indexing.
    # e.g. time: 1H; hf (high frequency): 2H, highest_res_H would be 1H
    # e.g. time: 2H; hf (high frequency): 3H, highest_res_H would be 1H
    highest_res_H = gcd(time_res_H, time_hf_res_H)
    highest_resolution = f"{highest_res_H}H"

    # create time array to loop through, with the required resolution
    # fp_HiTRes.time is the release time of particles into the model
    time_array = fp_HiTRes["time"]
    lat = fp_HiTRes["lat"]
    lon = fp_HiTRes["lon"]
    hback = fp_HiTRes["H_back"]

    ntime = len(time_array)
    nlat = len(lat)
    nlon = len(lon)
    # nh_back = len(hback)

    # Define maximum hour back
    max_h_back = hback.values[-1]

    # Define full range of dates to select from the flux input
    date_start = time_array[0]
    date_start_back = date_start - np.timedelta64(max_h_back, "h")
    date_end = time_array[-1] + np.timedelta64(1, "s")

    start = {
        dd: getattr(np.datetime64(time_array[0].values, "h").astype(object), dd) for dd in ["month", "year"]
    }

    # Create times for matching to the flux
    full_dates = date_range(
        date_start_back.values, date_end.values, freq=highest_resolution, closed="left"
    ).to_numpy()

    # Create low frequency flux data (monthly)
    flux_ds_low_freq = flux_ds.resample({"time": "1MS"}).mean().sel(time=slice(date_start_back, date_end))
    flux_ds_low_freq = flux_ds_low_freq.transpose(*("lat", "lon", "time"))

    # Select and align high frequency flux data
    flux_ds_high_freq = flux_ds.sel(time=slice(date_start_back, date_end))
    if flux_res_H <= 24:
        base = (
            date_start_back.dt.hour.data
            + date_start_back.dt.minute.data / 60.0
            + date_start_back.dt.second.data / 3600.0
        )
        if flux_res_H <= highest_res_H:
            # Downsample flux to match to footprints frequency
            flux_ds_high_freq = flux_ds_high_freq.resample({"time": highest_resolution}, base=base).mean()
        elif flux_res_H > highest_res_H:
            # Upsample flux to match footprints frequency and forward fill
            flux_ds_high_freq = flux_ds_high_freq.resample({"time": highest_resolution}, base=base).ffill()
        # Reindex to match to correct values
        flux_ds_high_freq = flux_ds_high_freq.reindex({"time": full_dates}, method="ffill")
    elif flux_res_H > 24:
        # If flux is not high frequency use the monthly averages instead.
        flux_ds_high_freq = flux_ds_low_freq

    # TODO: Add check to make sure time values are exactly aligned based on date range

    # Make sure the dimensions match the expected order for indexing
    fp_HiTRes = fp_HiTRes.transpose(*("lat", "lon", "time", "H_back"))
    flux_ds_high_freq = flux_ds_high_freq.transpose(*("lat", "lon", "time"))

    # Extract footprints array to use in numba loop
    fp_HiTRes = da.array(fp_HiTRes)

    # Set up a numpy array to calculate the product of the footprints (H matrix) with the fluxes
    if output_fpXflux:
        fpXflux = da.zeros((nlat, nlon, ntime))

    if output_TS:
        timeseries = da.zeros(ntime)

    # Iterate through the time coord to get the total mf at each time step using the H back coord
    # at each release time we disaggregate the particles backwards over the previous 24hrs
    # The final value then contains the 29-day integrated residual footprints
    print("Calculating modelled timeseries comparison:")
    iters = tqdm(time_array.values)
    for tt, time in enumerate(iters):

        # Get correct index for low resolution data based on start and current date
        current = {dd: getattr(np.datetime64(time, "h").astype(object), dd) for dd in ["month", "year"]}
        tt_low = current["month"] - start["month"] + 12 * (current["year"] - start["year"])

        # get 4 dimensional chunk of high time res footprints for this timestep
        # units : mol/mol/mol/m2/s
        # reverse the time coordinate to be chronological
        fp_time = fp_HiTRes[:, :, tt, ::-1]

        fp_high_freq = fp_time[:, :, 1:]
        fp_residual = fp_time[:, :, 0:1]  # First element (reversed) contains residual footprints

        # Extract flux data from dataset
        flux_high_freq = flux_ds_high_freq.flux
        flux_low_freq = flux_ds_low_freq.flux

        # Define high and low frequency fluxes based on inputs
        # Allow for variable frequency within 24 hours
        flux_low_freq = flux_low_freq[:, :, tt_low : tt_low + 1]
        if flux_res_H <= 24:
            # Define indices to correctly select matching date range from flux data
            # This will depend on the various frequencies of the inputs
            # At present, highest_res_H matches the flux frequency
            tt_start = tt * int(time_res_H / highest_res_H) + 1
            tt_end = tt_start + int(max_h_back / highest_res_H)
            selection = int(time_hf_res_H / highest_res_H)

            # Extract matching time range from whole flux array
            flux_high_freq = flux_high_freq[..., tt_start:tt_end]
            if selection > 1:
                # If flux frequency does not match to the high frequency (hf, H_back)
                # dimension, select entries which do. Reversed to make sure
                # entries matching to the correct times are selected
                flux_high_freq = flux_high_freq[..., ::-selection]
                flux_high_freq = flux_high_freq[..., ::-1]
        else:
            flux_high_freq = flux_high_freq[:, :, tt_low : tt_low + 1]

        # convert to array to use in numba loop
        flux_high_freq = da.array(flux_high_freq)
        flux_low_freq = da.array(flux_low_freq)

        # Multiply the HiTRes footprints with the HiTRes emissions to give mf
        # Multiply residual footprints by low frequency emissions data to give residual mf
        # flux units : mol/m2/s;       fp units : mol/mol/mol/m2/s
        # --> mol/mol/mol/m2/s * mol/m2/s === mol / mol
        fpXflux_time = flux_high_freq * fp_high_freq
        fpXflux_residual = flux_low_freq * fp_residual

        # append the residual emissions
        fpXflux_time = np.dstack((fpXflux_time, fpXflux_residual))

        if output_fpXflux:
            # Sum over time (H back) to give the total mf at this timestep
            fpXflux[:, :, tt] = fpXflux_time.sum(axis=2)

        if output_TS:
            # work out timeseries by summing over lat, lon (24 hrs)
            timeseries[tt] = fpXflux_time.sum()

    if output_fpXflux:
        fpXflux = DataArray(
            fpXflux,
            dims=("lat", "lon", "time"),
            coords={"lat": lat, "lon": lon, "time": time_array},
        )

    if output_TS:
        timeseries = DataArray(timeseries, dims=("time"), coords={"time": time_array})

    if output_fpXflux and output_TS:
        timeseries.compute()
        fpXflux.compute()
        return timeseries, fpXflux
    elif output_fpXflux:
        fpXflux.compute()
        return fpXflux
    elif output_TS:
        timeseries.compute()
        return timeseries

    return None
