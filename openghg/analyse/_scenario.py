from pandas import Timestamp
from xarray import Dataset
from typing import Optional, Tuple, Union, List
from openghg.dataobjects import ObsData, FootprintData, FluxData
from openghg.retrieve import get_obs_surface, get_footprint, search

__all__ = ["ModelScenario", "combine_datasets"]


# TODO: Need to think about input into the class
# We can include a site, inlet, species input and this will be able to grab
# the observations and footprints but we should think about how we want
# to assign the emissions
# Could we print out a list of emissions options if present which matched
# the input criteria for species etc.

# TODO: Emissions also shouldn't need to match against a domain
# We should be able to grab global/bigger area emissions and cut that down 
# to whichever area out LPDM model covered.

# TODO: Add static methods for different ways of creating the class
# e.g. from_existing_data(), from_search(), empty() , ...


class ModelScenario():
    """
    """
    def __init__(self,
                 site: Optional[str] = None,
                 species: Optional[str] = None,
                 inlet: Optional[str] = None,
                 network: Optional[str] = None,
                 domain: Optional[str] = None,
                 model: Optional[str] = None,
                 metmodel: Optional[str] = None,
                 sources: Optional[str] = None,  # TODO: Allow this to be a list of str as well?
                 start_date: Optional[str] = None,   # TODO: Allow str or Timestamp
                 end_date: Optional[str] = None,   # TODO: Allow str or Timestamp
                 obs: Optional[ObsData] = None, 
                 footprint: Optional[FootprintData] = None, 
                 flux: Optional[FluxData] = None):
        """
        Allow a set of keywords to be specified or for the objects to be supplied
        directly.

        """

        self.add_obs(site = site,
                     species = species,
                     inlet = inlet,
                     network = network,
                     start_date = start_date,
                     end_date = end_date,
                     obs = obs)

        self.add_footprint(site = site,
                           inlet = inlet,
                           domain = domain,
                           model = model,
                           metmodel = metmodel,
                           start_date = start_date,
                           end_date = end_date,
                           species = species,
                           footprint = footprint)

        # TODO: Don't necessarily want to "add" flux... may want to rename?
        self.add_flux(species=species,
                      domain=domain,
                      sources=sources,
                      start_date = start_date,
                      end_date = end_date,
                      flux = flux)


    def add_obs(self,
                site: Optional[str] = None,
                species: Optional[str] = None,
                inlet: Optional[str] = None,
                network: Optional[str] = None,
                start_date: Optional[str] = None,   # TODO: Allow str or Timestamp
                end_date: Optional[str] = None,   # TODO: Allow str or Timestamp
                obs: Optional[ObsData] = None):

        # TODO: Work out how we want to cache calculations
        # Make sure cache is empty to update obs
        # self.obs_cache = None

        # Search for obs data based on keywords
        if site and not obs:
            # search for obs based on suitable keywords - site, species, inlet
            obs_params = {"site": site, 
                          "species": species,
                          "inlet": inlet,
                          "network": network,
                          "start_date": start_date,
                          "end_date": end_date}

            try:
                obs = get_obs_surface(**obs_params)
            except ValueError:
                print("Unable to add observation data based on keywords supplied.")
                print(f"Inputs - site: {site}, species: {species}, inlet: {inlet}")
                obs_search = search(site=site, species=species, inlet=inlet)
                print(obs_search)
                # TODO: If we can determine how many results are returned from search
                # we can use this to give better information about why no data has
                # been found for these inputs.
        
        self.obs = obs

    def add_footprint(self,
                      site: Optional[str] = None,
                      inlet: Optional[str] = None,
                      domain: Optional[str] = None,
                      model: Optional[str] = None,
                      metmodel: Optional[str] = None,
                      start_date: Optional[str] = None,   # TODO: Allow str or Timestamp
                      end_date: Optional[str] = None,   # TODO: Allow str or Timestamp
                      species: Optional[str] = None,
                      footprint: Optional[FootprintData] = None):

        # Search for footprint data based on keywords
        # - site, domain, inlet (can extract from obs), model, metmodel
        if site and not footprint:

            if not inlet and self.obs:
                # TODO: Add case to deal with "multiple" inlets
                inlet = self.obs.metadata["inlet"]

            footprint_params = {"site": site, 
                                "height": inlet,
                                "domain": domain,
                                "model": model,  # Not currently used in get_footprint - should be added
                                # "metmodel": metmodel,  # Should be added to inputs for get_footprint() 
                                "start_date": start_date,
                                "end_date": end_date}

            try:
                # Check if there is a species specific footprint
                footprint = get_footprint(species=species, **footprint_params)
            except ValueError:
                try:
                    # If not, look for generic footprint
                    footprint = get_footprint(**footprint_params)
                except ValueError:
                    print("Unable to add footprint data based on keywords supplied.")
                    print("Inputs - \n")
                    print(f" site: {site}\n inlet: {inlet}\n domain: {domain}\n model: {model}\n metmodel: {metmodel}\n (species: {species})")
                    # TODO: Add footprint search? What's the syntax?
                    # TODO: If we can determine how many results are returned from search
                    # we can use this to give better information about why no data has
                    # been found for these inputs.
        
        self.footprint = footprint

    def add_flux(self,
                 species: Optional[str] = None,
                 domain: Optional[str] = None,
                 sources: Optional[str] = None,  # TODO: Allow this to be a list of str as well?
                 start_date: Optional[str] = None,   # TODO: Allow str or Timestamp
                 end_date: Optional[str] = None,   # TODO: Allow str or Timestamp
                 flux: Optional[FluxData] = None):

        # TODO: DECIDE HOW BEST TO SEARCH FOR AND ADD FLUX HERE!

        # Search for flux data based on keywords (but don't necessarily add..?)
        if species and not flux:

            pass
            # search for flux based on suitable keywords
            # BUT don't add automatically to the model scenario just print out 
            # options with details of how to add flux - decide on those details!

        self.flux = flux


    def check_data_is_present(self, need=["obs", "footprint"]):
        """
        Check whether correct data types have been included. This should
        be used by functions to check whether they can perform the requested 
        operation with the data types available.

        Args:
            need (list) : Names of objects needed for the function being called.
            Should be one or more of "obs", "footprint", "flux"
        
        Returns:
            None

            Raises ValueError is necessary data is missing.
        """
        missing = []
        for attr in need:
            value = getattr(self, attr)
            if value is None:
                missing.append(attr)
                
                print(f"Must have {attr} data linked to this ModelScenario to run this function")
                print(f"Add this using by setting the {attr} input, for example: ")
                print("  ModelScenario.{attr} = {attr.capitalize()}Data")
        
        if missing:            
            raise ValueError(f"Missing necessary {' and '.join(missing)} data.")

    def align_obs_footprint(self, 
                            resample_to: Optional[str] = "coarsest",
                            platform: Optional[str] = None) -> Tuple:
        """
        Slice and resample obs and footprint data to align along time

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

        self.check_data_is_present(need=["obs", "footprint"])

        obs_data = self.obs.data
        footprint_data = self.footprint.data

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
            obs_data_period_s = np.nanmedian((obs_data.time.data[1:] - obs_data.time.data[0:-1]) / 1e9).astype(
                "int64"
            )

            obs_data_period_s_min = np.diff(obs_data.time.data).min() / 1e9
            obs_data_period_s_max = np.diff(obs_data.time.data).max() / 1e9

            # Check if the periods differ by more than 1 second
            if np.isclose(obs_data_period_s_min, obs_data_period_s_max, 1):
                raise ValueError("Sample period can be not be derived from observations")

        obs_data_timeperiod = Timedelta(seconds=obs_data_period_s)

        # Derive the footprint period from the frequency of the data
        footprint_data_period_ns = np.nanmedian(
            (footprint_data.time.data[1:] - footprint_data.time.data[0:-1]).astype("int64")
        )
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

    def combine_obs_footprint(self,
                                resample_to: Optional[str] = "obs",
                                platform: Optional[str] = None,
                                species: Optional[Union[str, List]] = None,
                            ) -> Dataset:
        """
        """
        self.check_data_is_present(need=["obs", "footprint"])

        resample_to = resample_to.lower()
        resample_choices = ("obs", "footprint", "coarsest")
        if resample_to not in resample_choices:
            raise ValueError(f"Invalid resample choice {resample_to} past, please select from one of {resample_choices}")

        # As we're not processing any satellite data yet just set tolerance to None
        tolerance = None
        platform = None

        # TODO: Use species input to inform us on how to combine these datasets.
        # Can extract from self keywords or an external input?

        # Align and merge the observation and footprint Datasets
        aligned_obs, aligned_footprint = self.align_obs_footprint(resample_to=resample_to, platform=platform)
        combined_dataset = combine_datasets(dataset_A=aligned_obs, dataset_B=aligned_footprint, tolerance=tolerance)

        # Transpose to keep time in the last dimension position in case it has been moved in resample
        combined_dataset = combined_dataset.transpose(..., "time")

        # TODO: Add units into combined_dataset
        # # Save the observation data units
        # try:
        #     units = float(obs_data.mf.attrs["units"])
        # except KeyError:
        #     units = None
        # except AttributeError:
        #     raise AttributeError("Unable to read mf attribute from observation data.")

        # if units:
        #     combined_dataset.update({"fp": (combined_dataset.fp.dims, (combined_dataset.fp / units))})
        #     # if HiTRes:
        #     #     combined_dataset.update({"fp_HiTRes": (combined_dataset.fp_HiTRes.dims, (combined_dataset.fp_HiTRes / units))})

        return combined_dataset

    # TODO: Write calc_modelled_obs function to create a forward model combining the footprint and emissions inputs
    # TODO: Will want to allow this to be resampled / reindexed to the obs values even though they are not used here
    # but should allow this to a seperate function if obs isn't present.

    # def calc_modelled_obs(self, ):

    #     if self.footprint is None or self.flux is None:
    #         raise ValueError("Footprint and flux must be both be specified to calculate the modelled observation")
        

def _indexes_match(dataset_A: Dataset, dataset_B: Dataset) -> bool:
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


def combine_datasets(
    dataset_A: Dataset, dataset_B: Dataset, method: Optional[str] = "ffill", tolerance: Optional[str] = None) -> Dataset:
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

    if _indexes_match(dataset_A, dataset_B):
        dataset_B_temp = dataset_B
    else:
        dataset_B_temp = dataset_B.reindex_like(dataset_A, method, tolerance=tolerance)

    merged_ds = dataset_A.merge(dataset_B_temp)

    if "fp" in merged_ds:
        if all(k in merged_ds.fp.dims for k in ("lat", "long")):
            flag = np.where(np.isfinite(merged_ds.fp.mean(dim=["lat", "lon"]).values))
            merged_ds = merged_ds[dict(time=flag[0])]

    return merged_ds

# Blueprint from Issue #42 created for this

# class LPDM():

#     def __init__(species, domain, flux, bc):
#         self.species=species
#         self.domain=domain
#         #etc...

#         self.flux=get_flux(species, domain, flux)
#         self.bc=get_bc(species, domain, bc)

#         # dictionary of obs datasets + footprints?
#         self.obs = {}
#         self.footprints = {}

#     def obs_get(self, site, average=None):
#         self.obs["site"]=get_observations(self.species, site)

#     def obs_footprints_merge(self.obs, site, etc.):
#         obs_resampled, fp_resampled = footprints_data_merge(self.obs, site, kwargs_see_above)
#         self.obs["site"] = obs_resampled
#         self.footprints["site"] = fp_resampled

#     def model_mf(self, site):
#         # this is obviously not to be taken literally:
#         return self.footprints["site"] * self.flux + self.bc


# if __name__ == "__main__":
#     import LPDM # Need a better class name?

#     mod = LPDM(species="CH4", domain="EUROPE", flux=["ch4_source1", "ch4_source2", "ch4_source3"],
#                         bc="CAMS")

#     for site in sites:
#         mod.obs_get(site, average="1H") # Need to be careful about averaging here and/or in fp/data merge step 
#         mod.obs_footprints_merge(site, model="NAME-UKV", average="1D") # 1D average of combined fp/data dataset

#         # Optional
#         mod.get_site_met(site, met="ECMWF")

#     # output some obs
#     ds = mod.obs_out(site)
#     mod.obs_plot(site)

#     # plot some model output
#     mod.footprint_plot(site)

#     # predicted mole fraction
#     mf_mod = mod.model_mf(site, source="ch4_source1")