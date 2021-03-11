"""
This hopes to recreate the functionality of the ACRG repo function
footprints_data_merge
"""
from pandas import Timestamp
from xarray import Dataset
from typing import Dict, List, Optional, Tuple, Union

__all__ = ["single_site_footprint"]


def single_site_footprint(
    site: str,
    height: str,
    network: str,
    domain: str,
    resample_data: bool,
    start_date: Union[str, Timestamp],
    end_date: Union[str, Timestamp],
    site_modifier: Optional[str] = None,
    platform: Optional[str] = None,
    instrument: Optional[str] = None,
    species: Optional[Union[str, List]] = None,
) -> Dataset:
    """Creates a Dataset for a single site's measurement data and footprints

    Args:
        site:
        height
        network
        resample_data
        site_modifier: The name of the site given in the footprint.
                       This is useful for example if the same site footprints are run with a different met and
                       they are named slightly differently from the obs file. E.g.
                       site="DJI", site_modifier = "DJI-SAM" - station called DJI, footprint site called DJI-SAM
        platform:
        instrument:
        species:
    Returns:
        xarray.Dataset
    """
    from openghg.processing import get_observations, recombine_datasets, search_footprints
    from openghg.util import timestamp_tzaware

    start_date = timestamp_tzaware(start_date)
    end_date = timestamp_tzaware(end_date)

    # As we're not processing any satellite data yet just set tolerance to None
    tolerance = None
    platform = None

    # Here we want to use get_observations
    obs_results = get_observations(
        site=site, inlet=height, start_date=start_date, end_date=end_date, species=species, instrument=instrument
    )

    if len(obs_results) > 1:
        raise ValueError("More than one ObsData object returned. Unable to continue.")

    try:
        obs_data = obs_results[0].data
        # TODO - remove this once further checks are in place within get_obs and here to combine the returned data - GJ - 2021-03-10
    except IndexError:
        raise IndexError("Unable to find observation data for the passed parameters.")

    # Save the observation data units
    try:
        units = float(obs_data.mf.attrs["units"])
    except KeyError:
        units = None
    except AttributeError:
        raise AttributeError("Unable to read mf attribute from observation data.")

    footprint_site = site
    # If the site for the footprint has a different name pass that in
    if site_modifier:
        footprint_site = site_modifier

    # Get the footprint data
    footprint_results = search_footprints(sites=footprint_site, domains=domain, inlet=height, start_date=start_date, end_date=end_date)

    try:
        fp_site_key = list(footprint_results.keys())[0]
    except IndexError:
        raise ValueError(f"Unable to find any footprint data for {site} at a height of {height} in the {network} network.")

    footprint_keys = footprint_results[fp_site_key]["keys"]
    footprint_data = recombine_datasets(data_keys=footprint_keys, sort=False)

    # Align the two Datasets
    aligned_obs, aligned_footprint = align_datasets(
        obs_data=obs_data, footprint_data=footprint_data, platform=platform, resample_to_obs_data=False
    )

    combined_dataset = combine_datasets(dataset_A=aligned_obs, dataset_B=aligned_footprint, tolerance=tolerance)

    # Transpose to keep time in the last dimension position in case it has been moved in resample
    combined_dataset = combined_dataset.transpose(..., "time")

    if units:
        combined_dataset.update({"fp": (combined_dataset.fp.dims, (combined_dataset.fp / units))})
        # if HiTRes:
        #     combined_dataset.update({"fp_HiTRes": (combined_dataset.fp_HiTRes.dims, old_div(combined_dataset.fp_HiTRes, units))})

    return combined_dataset


def footprints_data_merge(
    sites: Union[str, List[str]],
    domain: str,
    species: str,
    load_flux: Optional[bool] = True,
    load_bc: Optional[bool] = True,
):
    """ 
    TODO - Should this be renamed?

    Args:
        site: Site or list of sites to retrieve combined footprint data for
        domain: Footprint domain name(s).
        species: Species of measurements to retrieve.
        load_flux: True includes fluxes in output, False does not. Default True.
        load_bc: True includes boundary conditions in output, False does not. Default True.
    Returns:
        dict: Dictionary footprint data objects
    """

    raise NotImplementedError()

    if load_flux:
        flux_dict = {}
        basestring = (str, bytes)
        for source in list(emissions_name.keys()):

            if isinstance(emissions_name[source], basestring):
                flux_dict[source] = flux(
                    domain, emissions_name[source], start=flux_bc_start, end=flux_bc_end, flux_directory=flux_directory
                )

            elif isinstance(emissions_name[source], dict):
                if HiTRes == False:
                    print(
                        "HiTRes is set to False and a dictionary has been found as the emissions_name dictionary value\
                          for source %s. Either enter your emissions names as separate entries in the emissions_name\
                          dictionary or turn HiTRes to True to use the two emissions files together with HiTRes footprints."
                        % source
                    )
                    # return None
                else:
                    flux_dict[source] = flux_for_HiTRes(
                        domain, emissions_name[source], start=flux_bc_start, end=flux_bc_end, flux_directory=flux_directory
                    )

        fp_and_data[".flux"] = flux_dict

    # We we're going to need multiple dictionaries
    # results = {}
    # # We want to get a footprint and observations for each site
    # for site in sites:
    #     single_site_footprint(site=site, domain=domain, species=species)
        





    # Takes in a dictionary of dataframes

    # Checks they all have the same species

    # Checks they have an emissions file attached - we can create an emissions class for this

    # Iterates over the sites
    # For each site
    # Reads data for the site etc from acrg_site_info - what if sites have multiple networks?
    # If network isn't passed we'll just use the first network
    # Read platform from acrg_site_info

    # Gets the start and end dates - beginning of the month for both
    # Checks flux boundary condition start dates - leave this for now

    # Check the height - get the user to pass this in
    # Then it checks if it's a satellite else finds the height closest to the inlet

    # Then it retrievs the footprints - do this from FOOTPRINTS
    # def footprints() loads the footprints from NetCDF

    # Retrieves the units for this dataset

    # Does some satellite dependent checks

    # Then it aligns the datasets using align_datasets
    # So we need to have the mol/frac data AND the footprint data available at this point

    # Then it does a transpose to ensure time is in the last dimension position ?

    # Updates the units using a scaling factor

    # This creates a list of Datasets for each site and then combines them into one Dataset for each site

    # Then it does some flux and boundary condition work I'll leave for now

    # Then it reads the scales for each of the site datasets
    # Adds units if we have them

    # It adds these as . keys for some reason to be backwards compatible?
    # Don't think we need to do this

    # Returns a dictionary keyed by site


def combine_datasets(
    dataset_A: Dataset, dataset_B: Dataset, method: Optional[str] = "ffill", tolerance: Optional[str] = None
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
        dataset_B_temp = dataset_B.reindex_like(dataset_A, method, tolerance=tolerance)

    merged_ds = dataset_A.merge(dataset_B_temp)

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
    obs_data: Dataset, footprint_data: Dataset, platform: Optional[str] = None, resample_to_obs_data: Optional[bool] = False
) -> Tuple[Dataset, Dataset]:
    """Slice and resample two datasets to align along time

    This slices the date to the smallest time frame
    spanned by both the footprint and obs, then resamples the data
    using the mean to the one with coarsest median resolution
    starting from the sliced start date.

    Args:
        obs_data: Observations Dataset
        footprint_data: Footprint Dataset
        platform: Observation platform used to decide whether to resample
        resample_to_obs_data: Override resampling to coarser resolution and resample to obs_data regardless
    Returns:
        tuple: Two xarray.Dataset with aligned time dimensions
    """
    import numpy as np

    platform_skip_resample = ("satellite", "flask")

    if platform in platform_skip_resample:
        return obs_data, footprint_data

    obs_data_timeperiod = np.nanmedian((obs_data.time.data[1:] - obs_data.time.data[0:-1]).astype("int64"))
    footprint_data_timeperiod = np.nanmedian((footprint_data.time.data[1:] - footprint_data.time.data[0:-1]).astype("int64"))

    obs_startdate = obs_data.time[0]
    obs_enddate = obs_data.time[-1]
    footprint_startdate = footprint_data.time[0]
    footprint_enddate = footprint_data.time[-1]

    if int(obs_startdate.data) > int(footprint_startdate.data):
        start_date = obs_startdate
    else:
        start_date = footprint_startdate

    if int(obs_enddate.data) < int(footprint_enddate.data):
        end_date = obs_enddate
    else:
        end_date = footprint_enddate

    # Subtract half a second to ensure lower range covered
    start_s = str(np.round(start_date.data.astype(np.int64) - 5e8, -9).astype("datetime64[ns]"))
    # Add half a second to ensure upper range covered
    end_s = str(np.round(end_date.data.astype(np.int64) + 5e8, -9).astype("datetime64[ns]"))

    obs_data = obs_data.sel(time=slice(start_s, end_s))
    footprint_data = footprint_data.sel(time=slice(start_s, end_s))

    # only non satellite datasets with different periods need to be resampled
    if not np.isclose(obs_data_timeperiod, footprint_data_timeperiod):
        base = start_date.dt.hour.data + start_date.dt.minute.data / 60.0 + start_date.dt.second.data / 3600.0

        if (obs_data_timeperiod >= footprint_data_timeperiod) or resample_to_obs_data is True:

            resample_period = str(round(obs_data_timeperiod / 3600e9, 5)) + "H"

            footprint_data = footprint_data.resample(indexer={"time": resample_period}, base=base).mean()

        elif obs_data_timeperiod < footprint_data_timeperiod or resample_to_obs_data is False:

            resample_period = str(round(footprint_data_timeperiod / 3600e9, 5)) + "H"

            obs_data = obs_data.resample(indexer={"time": resample_period}, base=base).mean()

    return obs_data, footprint_data
