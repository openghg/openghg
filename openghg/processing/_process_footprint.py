"""
This hopes to recreate the functionality of the ACRG repo function
footprints_data_merge
"""
from pandas import Timestamp
from xarray import Dataset
from typing import Union

__all__ = ["single_site_footprint"]


def single_site_footprint(
    site: str, height: str, network: str, resample_data: bool, start_date: Union[str, Timestamp], end_date: Union[str, Timestamp]
) -> Dataset:
    """Creates a Dataset for a single site's measurement data and footprints

    Args:
        site:
        height
        network
        resample_data
    Returns:
        xarray.Dataset
    """
    from openghg.processing import search, recombine_sections, search_footprints
    from openghg.util import timestamp_tzaware

    start_date = timestamp_tzaware(start_date)
    end_date = timestamp_tzaware(end_date)

    # Get the measurement data
    measurement_results = search(locations=site, inlet=height, start_date=start_date, end_date=end_date)

    try:
        site_key = list(measurement_results.keys())[0]
    except IndexError:
        raise ValueError(f"Unable to find any measuremnt results for {site} at a height of {height} in the {network} network.")

    measurement_keys = measurement_results[site_key]["keys"]

    measurement_data = recombine_sections(data_keys=measurement_keys, sort=True)

    # Get the footprint data
    footprint_results = search_footprints(locations=site, inlet=height, start_date=start_date, end_date=end_date)

    try:
        fp_site_key = list(footprint_results.keys())[0]
    except IndexError:
        raise ValueError(f"Unable to find any footprints for {site} at a height of {height} in the {network} network.")

    footprint_keys = footprint_results[fp_site_key]["keys"]
    footprint_data = recombine_sections(data_keys=footprint_keys, sort=False)

    # As we're not processing any satellite data yet just set toleranc to None
    tolerance = None



    return measurement_data, footprint_data

    # Now need to test these two parts work


def footprints_data_merge():
    """This retrieves mol/frac data and footprints from the object store and combines them into a
    single xarray Dataset

    Returns:
        dict: Dictionary of merged Datasets
    """
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


def retrieve_footprints(site, others, HiTRes=False):
    """This retrieves the correct footprints from the object store"""

    if HiTRes:
        # get high time resolution footprints
        # OpenGHG doesn't currently differentiate between these and normal footprints
        pass


#
def combine_datasets(dsa, dsb, method="ffill", tolerance=None):
    """
    This is taken from the ACRG repo

    The combine_datasets function merges two datasets and re-indexes to the FIRST dataset.
    If "fp" variable is found within the combined dataset, the "time" values where the "lat","lon"
    dimensions didn't match are removed.

    Example:
        ds = combine_datasets(dsa, dsb)

    Args:
        dsa (xarray.Dataset) :
            First dataset to merge
        dsb (xarray.Dataset) :
            Second dataset to merge
        method (str, optional) :
            One of {None, ‘nearest’, ‘pad’/’ffill’, ‘backfill’/’bfill’}
            See xarray.DataArray.reindex_like for list of options and meaning.
            Default = "ffill" (forward fill)
        tolerance (int/float??) :
            Maximum allowed tolerance between matches.

    Returns:
        xarray.Dataset:
            Combined dataset indexed to dsa
    """
    import numpy as np

    # merge the two datasets within a tolerance and remove times that are NaN (i.e. when FPs don't exist)

    if not indexesMatch(dsa, dsb):
        dsb_temp = dsb.reindex_like(dsa, method, tolerance=tolerance)
    else:
        dsb_temp = dsb

    ds_temp = dsa.merge(dsb_temp)
    if "fp" in list(ds_temp.keys()):
        flag = np.where(np.isfinite(ds_temp.fp.mean(dim=["lat", "lon"]).values))
        ds_temp = ds_temp[dict(time=flag[0])]
    return ds_temp


def indexesMatch(dsa, dsb):
    """
    Check if two datasets need to be reindexed_like for combine_datasets

    Args:
        dsa (xarray.Dataset) :
            First dataset to check
        dsb (xarray.Dataset) :
            Second dataset to check

    Returns:
        boolean:
            True if indexes match, False if datasets must be reindexed
    """

    commonIndicies = [key for key in dsa.indexes.keys() if key in dsb.indexes.keys()]

    # test if each comon index is the same
    for index in commonIndicies:
        # first check lengths are the same to avoid error in second check
        if not len(dsa.indexes[index]) == len(dsb.indexes[index]):
            return False

        # check number of values that are not close (testing for equality with floating point)
        if index == "time":
            # for time iverride the default to have ~ second precision
            rtol = 1e-10
        else:
            rtol = 1e-5
        if (
            not np.sum(~np.isclose(dsa.indexes[index].values.astype(float), dsb.indexes[index].values.astype(float), rtol=rtol))
            == 0
        ):
            return False

    return True


def align_datasets(dataset_1, dataset_2, platform=None, resample_to_dataset_1=False):
    """
    Slice and resample two datasets to align along time

    Args:
        dataset_1, dataset_2 (xarray.Dataset) :
            Datasets with time dimension. It is assumed that dataset_1 is obs data and dataset_2 is footprint data

        platform (str) :
            obs platform used to decide whether to resample

        resample_to_dataset_1 (boolean) :
            Override resampling to coarser resolution and resample to dataset_1 regardless

    Returns:
        2 xarray.dataset with aligned time dimensions
    """
    import numpy as np

    platform_skip_resample = ("satellite", "flask")

    if platform in platform_skip_resample:
        return dataset_1, dataset_2

    # lw13938: 12/04/2018 - This should slice the date to the smallest time frame
    # spanned by both the footprint and obs, then resamples the data
    # using the mean to the one with coarsest median resolution
    # starting from the sliced start date.

    dataset_1_timeperiod = np.nanmedian((dataset_1.time.data[1:] - dataset_1.time.data[0:-1]).astype("int64"))
    dataset_2_timeperiod = np.nanmedian((dataset_2.time.data[1:] - dataset_2.time.data[0:-1]).astype("int64"))

    dataset_1_st = dataset_1.time[0]
    dataset_1_et = dataset_1.time[-1]
    dataset_2_st = dataset_2.time[0]
    dataset_2_et = dataset_2.time[-1]

    if int(dataset_1_st.data) > int(dataset_2_st.data):
        start_date = dataset_1_st
    else:
        start_date = dataset_2_st
    if int(dataset_1_et.data) < int(dataset_2_et.data):
        end_date = dataset_1_et
    else:
        end_date = dataset_2_et

    start_s = str(
        np.round(start_date.data.astype(np.int64) - 5e8, -9).astype("datetime64[ns]")
    )  # subtract half a second to ensure lower range covered
    end_s = str(
        np.round(end_date.data.astype(np.int64) + 5e8, -9).astype("datetime64[ns]")
    )  # add half a second to ensure upper range covered

    dataset_1 = dataset_1.sel(time=slice(start_s, end_s))
    dataset_2 = dataset_2.sel(time=slice(start_s, end_s))

    # only non satellite datasets with different periods need to be resampled
    if not np.isclose(dataset_1_timeperiod, dataset_2_timeperiod):
        base = start_date.dt.hour.data + start_date.dt.minute.data / 60.0 + start_date.dt.second.data / 3600.0
        if (dataset_1_timeperiod >= dataset_2_timeperiod) or (resample_to_dataset_1 == True):
            resample_period = (
                str(round(dataset_1_timeperiod / 3600e9, 5)) + "H"
            )  # rt17603: Added 24/07/2018 - stops pandas frequency error for too many dp.
            dataset_2 = dataset_2.resample(indexer={"time": resample_period}, base=base).mean()
        elif dataset_1_timeperiod < dataset_2_timeperiod or (resample_to_dataset_1 == False):
            resample_period = (
                str(round(dataset_2_timeperiod / 3600e9, 5)) + "H"
            )  # rt17603: Added 24/07/2018 - stops pandas frequency error for too many dp.
            dataset_1 = dataset_1.resample(indexer={"time": resample_period}, base=base).mean()

    return dataset_1, dataset_2