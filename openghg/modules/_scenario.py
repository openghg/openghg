from pandas import Timestamp
from xarray import Dataset
from typing import Optional, Tuple, Union, List

__all__ = ["indexes_match", "ModelScenario"]

class ModelScenario():
    """
    """
    def __init__(self, obs=None, footprint=None, flux=None):
        self.obs = obs
        self.footprint = footprint
        self.flux = flux

        # Set keywords based on obs, footprint and flux input or otherwise
        # species, domain, ...

    def combine_datasets(self, 
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

    def check_data(self, need=["obs", "footprint"]):
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
            raise ValueError(f"Missing necessary {' and '.join(missing)} data")

    def align_obs_footprint(self, 
                            resample_to: Optional[str] = "coarsest",
                            platform: Optional[str] = None) -> Tuple:
        """

        """
        import numpy as np
        from pandas import Timedelta

        self.check_data(need=["obs", "footprint"])

        obs_data = self.obs.data
        footprint_data = self.footprint.data

        if platform is not None:
            platform = platform.lower()
            if platform in ("satellite", "flask"):
                return obs_data, footprint_data

        # This gives us the period in ns
        obs_data_period_ns = np.nanmedian((obs_data.time.data[1:] - obs_data.time.data[0:-1]).astype("int64"))
        footprint_data_period_ns = np.nanmedian((footprint_data.time.data[1:] - footprint_data.time.data[0:-1]).astype("int64"))

        obs_data_timeperiod = Timedelta(obs_data_period_ns, unit="ns")
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
        if not np.isclose(obs_data_period_ns, footprint_data_period_ns):
            base = start_date.hour + start_date.minute / 60.0 + start_date.second / 3600.0
            print("resample_to", resample_to)
            if resample_to == "coarsest":
                if obs_data_timeperiod >= footprint_data_timeperiod:
                    resample_to = "obs"
                elif obs_data_timeperiod < footprint_data_timeperiod:
                    resample_to = "footprint"

            if resample_to == "obs":

                resample_period = str(round(obs_data_timeperiod / np.timedelta64(1, "h"), 5)) + "H"
                footprint_data = footprint_data.resample(indexer={"time": resample_period}, base=base).mean()

            elif resample_to == "footprint":

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
        self.check_data(need=["obs", "footprint"])

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
        combined_dataset = self.combine_datasets(dataset_A=aligned_obs, dataset_B=aligned_footprint, tolerance=tolerance)

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