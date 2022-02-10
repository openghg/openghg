from xml.sax import parseString
from pandas import Timestamp
from xarray import Dataset, DataArray
from typing import Optional, Tuple, Union, List, Dict, Any
from openghg.dataobjects import ObsData, FootprintData, FluxData
from openghg.retrieve import get_obs_surface, get_footprint, get_flux, search

__all__ = ["ModelScenario", "combine_datasets", "stack_datasets"]

# TODO: Consider how to handle averaging
# TODO: Consider how to handle sources as well as storing and using multiple fluxes
# TODO: How does the high/low resolution for fluxes work with this?

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

# TODO: Incorporate boundary conditions when possible


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
                 sources: Optional[Union[str, List]] = None,
                 start_date: Optional[Union[str, Timestamp]] = None,
                 end_date: Optional[Union[str, Timestamp]] = None,
                 obs: Optional[ObsData] = None, 
                 footprint: Optional[FootprintData] = None, 
                 flux: Optional[Union[FluxData, Dict[str, FluxData]]] = None):
        """
        Create a ModelScenario instance based on a set of keywords to be 
        or directly supplied objects. This can be created as an empty class to be 
        populated.

        The keywords are related to observation, footprint and flux data
        which may be available within the object store. The combination of these supplied
        will be used to extract the relevant data. Related keywords are as follows:
         - Observation data: site, species, inlet, network, start_date, end_data
         - Footprint data: site, inlet, domain, model, metmodel, species, start_date, end_date
         - Flux data: species, sources, domain, start_date, end_date

        Args:
            site : Site code e.g. "TAC"
            species : Species code e.g. "ch4"
            inlet : Inlet value e.g. "10m"
            network : Network name e.g. "AGAGE"
            domain : Domain name e.g. "EUROPE"
            model : Model name used in creation of footprint e.g. "NAME"
            metmodel : Name of met model used in creation of footprint e.g. "UKV"
            sources : Emissions sources
            start_date : Start of date range to use. Note for flux this may not be applied
            end_date : End of date range to use. Note for flux this may not be applied
            obs : Supply ObsData object directly (e.g. from get_obs...() functions)
            footprint : Supply FootprintData object directly (e.g. from get_footprint() function)
            flux : Supply FluxData object directly (e.g. from get_flux() function)

        Returns:
            None

            Sets up instance of class with associated values.

        TODO: For obs, footprint, flux should we also allow Dataset input and turn
        these into the appropriate class?
        """

        # Add observation data (directly or through keywords)
        self.add_obs(site = site,
                     species = species,
                     inlet = inlet,
                     network = network,
                     start_date = start_date,
                     end_date = end_date,
                     obs = obs)

        # TODO: Add updates to inputs here if obs has been specified directly if useful?

        # Add footprint data (directly or through keywords)
        self.add_footprint(site = site,
                           inlet = inlet,
                           domain = domain,
                           model = model,
                           metmodel = metmodel,
                           start_date = start_date,
                           end_date = end_date,
                           species = species,
                           footprint = footprint)

        # Add flux data (directly or through keywords)
        self.add_flux(species=species,
                      domain=domain,
                      sources=sources,
                      start_date = start_date,
                      end_date = end_date,
                      flux = flux)

        # Initialise attributes used for caching
        self.scenario = None
        self.modelled_obs = None

        # TODO: Check species, site etc. values align between inputs?
        # TODO: May want to add class additional attributes for e.g. site, species etc.

    # TODO: May want to find a clever functional way to combine aspects of add_obs, add_footprint, add_flux together

    def _get_data(self,
                  keywords: Union[List[Dict[str, str]], Dict[str, str]],
                  input_type: str):
        """
        Use appropriate get function to search for data in object store.
        """

        get_functions = {"obs_surface": get_obs_surface,
                         "footprint": get_footprint,
                         "flux": get_flux}

        # TODO: Add/write footprint and flux search? What's the syntax?
        search_functions = {"obs_surface": search}

        get_fn = get_functions[input_type]
        search_fn = search_functions.get(input_type)

        if isinstance(keywords, Dict):
            keywords = [keywords]
        
        num_checks = len(keywords)
        for i, keyword_set in enumerate(keywords):
            try:
                 data = get_fn(**keyword_set)
            except (ValueError, AttributeError):
                num = i + 1
                if num == num_checks:
                    print(f"Unable to add {input_type} data based on keywords supplied.")
                    print(" Inputs - \n")
                    for key, value in keyword_set.items():
                        print(f" {key}: {value}\n")
                    if search_fn is not None:
                        data_search = search_fn(**keyword_set)
                        print("---- Search results ---")
                        print(data_search)
                        # TODO: If we can determine how many results are returned from search
                        # we can use this to give better information about why no data has
                        # been found for these inputs.
                data = None
            else:
                print(f"Adding {input_type} to model scenario")
                break

        return data


    def add_obs(self,
                site: Optional[str] = None,
                species: Optional[str] = None,
                inlet: Optional[str] = None,
                network: Optional[str] = None,
                start_date: Optional[Union[str, Timestamp]] = None,
                end_date: Optional[Union[str, Timestamp]] = None,
                obs: Optional[ObsData] = None):
        """
        Add observation data based on keywords or direct ObsData object.
        """
        from openghg.util import clean_string

        # Search for obs data based on keywords
        if site and obs is None:
            site = clean_string(site)
            # search for obs based on suitable keywords - site, species, inlet
            obs_keywords = {"site": site, 
                            "species": species,
                            "inlet": inlet,
                            "network": network,
                            "start_date": start_date,
                            "end_date": end_date}

            obs = self._get_data(obs_keywords, input_type="obs_surface")

        self.obs = obs

        # Add keywords to class for convenience
        if self.obs is not None:
            self.site = self.obs.metadata["site"]
            self.species = self.obs.metadata["species"]


    def add_footprint(self,
                      site: Optional[str] = None,
                      inlet: Optional[str] = None,
                      domain: Optional[str] = None,
                      model: Optional[str] = None,
                      metmodel: Optional[str] = None,
                      start_date: Optional[Union[str, Timestamp]] = None,
                      end_date: Optional[Union[str, Timestamp]] = None,
                      species: Optional[str] = None,
                      footprint: Optional[FootprintData] = None):
        """
        Add footprint data based on keywords or direct FootprintData object.
        """
        from openghg.util import clean_string

        # Search for footprint data based on keywords
        # - site, domain, inlet (can extract from obs), model, metmodel
        if site and footprint is None:
            site = clean_string(site)
            if not inlet and self.obs:
                # TODO: Add case to deal with "multiple" inlets
                inlet = self.obs.metadata["inlet"]

            footprint_keywords_1 = {"site": site, 
                                    "height": inlet,
                                    "domain": domain,
                                    "model": model,  # Not currently used in get_footprint - should be added
                                    # "metmodel": metmodel,  # Should be added to inputs for get_footprint() 
                                    "start_date": start_date,
                                    "end_date": end_date,
                                    "species": species}
            
            footprint_keywords_2 = footprint_keywords_1.copy()
            footprint_keywords_2.pop("species")

            footprint_keywords = [footprint_keywords_1, footprint_keywords_2]

            footprint = self._get_data(footprint_keywords, input_type="footprint")
        
        self.footprint = footprint

        if self.footprint is not None and not hasattr(self, "site"):
            self.site = self.footprint.metadata["site"]


    def add_flux(self,
                 species: Optional[str] = None,
                 domain: Optional[str] = None,
                 sources: Optional[Union[str, List]] = None,
                 start_date: Optional[Union[str, Timestamp]] = None,
                 end_date: Optional[Union[str, Timestamp]] = None,
                 flux: Optional[Union[FluxData, Dict[str, FluxData]]] = None) -> Dict[str, FluxData]:
        """
        Add flux data based on keywords or direct FluxData object.
        """
        # Search for flux data based on keywords (but don't necessarily add..?)
        if species and flux is None:
            if sources is None or isinstance(sources, str):
                sources = [sources]
            
            flux = {}
            for source in sources:
                # TODO: Make this into a    dictionary for each source
                flux_keywords_1 = {"species": species,
                                   "source": source,
                                   "domain": domain,
                                   "start_date": start_date,
                                   "end_date": end_date}
                
                flux_keywords_2 = flux_keywords_1.copy()
                flux_keywords_2.pop("start_date")
                flux_keywords_2.pop("end_date")

                flux_keywords = [flux_keywords_1, flux_keywords_2]

                # TODO: Add something to allow for e.g. global domain or no domain

                flux_source = self._get_data(flux_keywords, input_type="flux")
            
            flux[source] = flux_source

        elif flux is not None:
            if not isinstance(flux, dict):
                if sources is None:
                    source = "source"
                elif isinstance(sources, list):
                    source = sources[0]
                else:
                    source = sources
                flux = {source: flux}

        # TODO: Make this so flux.anthro can be called etc. - link in some way
        self.flux = flux

        # Is there any metadata associated with FluxData??
        if self.flux is not None and not hasattr(self, "species"):
            flux_values = list(self.flux.values())
            flux_1 = flux_values[0]
            self.species = flux_1.metadata["species"]


    def _check_data_is_present(self, need=["obs", "footprint"]):
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
                print(f"Include this by using the add function, with appropriate inputs:")
                print("  ModelScenario.add_{attr}(...)")
        
        if missing:            
            raise ValueError(f"Missing necessary {' and '.join(missing)} data.")


    def _get_platform(self) -> Union[str, None]:
        """
        Find the platform for a site, if present.
        
        This will access the "acrg_site_info.json" file to find this information.
        """
        from openghg.util import load_json

        try:
            site = self.site
            site_upper = site.upper()
        except AttributeError:
            return None
        else:
            site_info = load_json(filename="acrg_site_info.json")
            platform = site_info[site_upper].get("platform")
            return platform


    def _align_obs_footprint(self,
                             resample_to: Optional[str] = "coarsest",
                             platform: Optional[str] = None) -> Tuple:
        """
        Slice and resample obs and footprint data to align along time

        This slices the date to the smallest time frame
        spanned by both the footprint and obs, using the sliced start date
        The time dimension is resampled based on the resample_to input using the mean.
        The resample_to options are:
         - "coarsest" - resample to the coarsest resolution between obs and footprints
         - "obs" - resample to observation data frequency
         - "footprint" - resample to footprint data frequency
         - a valid resample period e.g. "2H"

        Args:
            resample_to: Resample option to use: either data based or using a valid pandas resample period.
            platform: Observation platform used to decide whether to resample
        
        Returns:
            tuple: Two xarray.Dataset with aligned time dimensions
        """
        import numpy as np
        from pandas import Timedelta

        self._check_data_is_present(need=["obs", "footprint"])

        obs_data = self.obs.data
        footprint_data = self.footprint.data

        resample_keyword_choices = ("obs", "footprint", "coarsest")
        if resample_to in resample_keyword_choices:
            resample_to = resample_to.lower()

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
        footprint_enddate = Timestamp(footprint_data.time[-1].values) + Timedelta(footprint_data_timeperiod, unit="nanoseconds")

        start_date = max(obs_startdate, footprint_startdate)
        end_date = min(obs_enddate, footprint_enddate)

        # Subtract half a second to ensure lower range covered
        start_slice = start_date - Timedelta("0.5s")
        # Add half a second to ensure upper range covered
        end_slice = end_date + Timedelta("0.5s")

        obs_data = obs_data.sel(time=slice(start_slice, end_slice))
        footprint_data = footprint_data.sel(time=slice(start_slice, end_slice))

        # Check whether resample has been requested by specifying a specific period rather than a keyword
        if resample_to in resample_keyword_choices:
            force_resample = False
        else:
            force_resample = True

        # Only non satellite datasets with different periods need to be resampled
        timeperiod_diff_s = np.abs(obs_data_timeperiod - footprint_data_timeperiod).total_seconds()
        tolerance = 1e-9  # seconds

        if timeperiod_diff_s >= tolerance or force_resample:
            base = start_date.hour + start_date.minute / 60.0 + start_date.second / 3600.0

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

            else:
                resample_period = resample_to
                footprint_data = footprint_data.resample(indexer={"time": resample_period}, base=base).mean()
                obs_data = obs_data.resample(indexer={"time": resample_period}, base=base).mean()

        return obs_data, footprint_data


    def combine_obs_footprint(self,
                              resample_to: Optional[str] = "obs",
                              platform: Optional[str] = None,
                              cache: Optional[bool] = "True") -> Dataset:
        """
        Combine observation and footprint data so these are on the same time
        axis. This will both slice and resample the data to align this axis.

        - Data is slices to smallest timeframe spanned by both footprint and obs
        - Data is resampled according to resample_to input and using the mean
        - Data is combined into one dataset

        Args:
            resample_to: Resample option to use for averaging: 
                          - either one of ["coarsest", "obs", "footprint"] to match to the datasets
                          - or using a valid pandas resample period e.g. "2H".
                         Default = "coarsest".
            platform: Observation platform used to decide whether to resample
            cache: Cache this data after calculation. Default = True.

        Returns:
            xarray.Dataset: Combined dataset aligned along the time dimension

            If cache is True:
                This data will be also be cached as the ModelScenario.scenario attribute.
        """

        self._check_data_is_present(need=["obs", "footprint"])

        # As we're not processing any satellite data yet just set tolerance to None
        tolerance = None
        if platform is None:
            platform = self._get_platform()

        # Align and merge the observation and footprint Datasets
        aligned_obs, aligned_footprint = self._align_obs_footprint(resample_to=resample_to, platform=platform)
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

        combined_dataset.attrs["resample_to"] = resample_to

        if cache:
            self.scenario = combined_dataset

        return combined_dataset


    def combine_flux_sources(self, sources: Optional[Union[str, List]] = None) -> Dataset:
        """
        Combine together flux sources on the time dimension. This will align to
        the time of the highest frequency flux source both for time range and frequency.

        Args:
            sources : Names of sources to combine. Should already be attached to ModelScenario.

        Returns:
            Dataset: All flux sources stacked on the time dimension.
        """
        self._check_data_is_present(need=["flux"])

        time_dim = "time"
        flux_dict = self.flux

        if sources is None:
            sources = flux_dict.keys()
        elif isinstance(sources, str):
            sources = [sources]

        flux_datasets = [flux_dict[source].data for source in sources]

        if len(sources) == 1:
            return flux_datasets[0]

        try:
            flux_stacked = stack_datasets(flux_datasets, dim=time_dim, method="ffill")
        except ValueError:
            sources = flux_dict.keys()
            raise ValueError(f"Unable to combine flux data for sources: {list(sources)}")

        return flux_stacked


    def _check_footprint_resample(self, resample_to: str) -> FootprintData:
        '''
        Check whether footprint needs resampling based on resample_to input.
        '''
        if resample_to in ("coarsest", "obs", "footprint"):
            return self.footprint.data
        else:
            footprint_data = self.footprint.data
            time = footprint_data["time"].values
            start_date = Timestamp(time[0])
            base = start_date.hour + start_date.minute / 60.0 + start_date.second / 3600.0
            footprint_data = footprint_data.resample(indexer={"time": resample_to}, base=base).mean()
            return footprint_data


    def calc_modelled_obs(self,
                          sources: Optional[Union[str, List]] = None,
                          resample_to: Optional[str] = "coarsest",
                          platform: Optional[str] = None,
                          cache: Optional[bool] = True,
                          recalculate: Optional[bool] = False) -> DataArray:
        """
        Calculate the modelled observation points based on site footprint and fluxes.

        The time points returned are dependent on the resample_to option chosen.
        If obs data is also linked to the ModelScenario instance, this will be used
        to derive the time points where appropriate.
        
        Args:
            sources: NOT IMPLEMENTED YET. Sources to use for flux. All will be used if not specified.
            resample_to: Resample option to use for averaging: 
                          - either one of ["coarsest", "obs", "footprint"] to match to the datasets
                          - or using a valid pandas resample period e.g. "2H".
                         Default = "coarsest".
            platform: Observation platform used to decide whether to resample e.g. "site", "satellite".
            cache: Cache this data after calculation. Default = True.
            recalculate: Make sure to recalculate this data rather than return from cache. Default = False.

        Returns:
            xarray.DataArray: Modelled observation values along the time axis

            If cache is True:
                This data will also be cached as the ModelScenario.modelled_obs attribute.
                The associated scenario data will be cached as the ModelScenario.scenario attribute.
        """

        self._check_data_is_present(need=["footprint", "flux"])

        # Check if cached modelled observations exist
        if self.modelled_obs is None:
            # Check if observations are present and use these for resampling
            if self.obs is not None:
                self.combine_obs_footprint(resample_to, platform=platform, cache=True)
            else:
                self.scenario = self._check_footprint_resample(resample_to)
        else:
            if self.obs is not None:
                # Check previous resample_to input for cached data
                prev_resample_to = self.scenario.attrs.get("resample_to")

                # Check if this previous resample period matches input value
                # - if not (or explicit recalculation requested), recreate scenario
                # - if so return cached modelled observations
                if prev_resample_to != resample_to or recalculate:
                    self.combine_obs_footprint(resample_to, platform=platform, cache=True)
                else:
                    return self.modelled_obs
            elif recalculate:
                # Recalculate based on footprint data if obs not present
                self.scenario = self._check_footprint_resample(resample_to)
            else:
                # Return cached modelled observations if explicit recalculation not requested
                return self.modelled_obs

        # Check species and use high time resolution steps if this is carbon dioxide
        if self.species == "co2":
            modelled_obs = self._calc_modelled_obs_HiTRes(sources=sources, output_TS=True, output_fpXflux=False)
            name = "mf_mod_high_res"
        else:
            modelled_obs = self._calc_modelled_obs_integrated(sources=sources, output_TS=True, output_fpXflux=False)
            name = "mf_mod"

        # Cache output from calculations
        if cache:
            print("Caching calculated data")
            self.modelled_obs = modelled_obs
            self.scenario[name] = modelled_obs
        else:
            self.modelled_obs = None  # Make sure this is reset and not cached
            self.scenario = None  # Reset this to None after calculation completed

        return modelled_obs


    def _calc_modelled_obs_integrated(self,
                                      sources: Optional[Union[str, List]] = None,
                                      output_TS: Optional[bool] = True,
                                      output_fpXflux: Optional[bool] = False,
                                      ) -> Any:
        """
        Calculate modelled mole fraction timeseries using integrated footprints data.

        Returns:
            DataArray / DataArray :
                Modelled mole fraction timeseries, dimensions = (time)
                Modelled flux map, dimensions = (lat, lon, time)

            If one of output_TS and output_fpXflux are True:
                DataArray is returned for the respective output

            If both output_TS and output_fpXflux are both True:
                Both DataArrays are returned.
        """

        scenario = self.scenario
        flux = self.combine_flux_sources(sources)

        flux = flux.reindex_like(scenario, "ffill")
        flux_modelled: DataArray = scenario["fp"] * flux["flux"]
        timeseries: DataArray = flux_modelled.sum(["lat", "lon"])

        if output_TS and output_fpXflux:
            return timeseries, flux_modelled
        elif output_TS:
            return timeseries
        elif output_fpXflux:
            return flux_modelled


    def _calc_modelled_obs_HiTRes(self, 
                                  sources: Optional[Union[str, List]] = None,
                                  averaging: Optional[str] = None,
                                  output_TS: Optional[bool] = True,
                                  output_fpXflux: Optional[bool] = False,
                                  ) -> Any:
        """
        Calculate modelled mole fraction timeseries using high time resolution
        footprints data and emissions data. This is appropriate for time variable
        species reliant on high time resolution footprints such as carbon dioxide (co2).

        Args:
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

        # TODO: Need to work out how this fits in with high time resolution method
        # Do we need to flag low resolution to use a different method? natural / anthro for example

        fp_HiTRes = self.scenario.fp_HiTRes
        flux_ds = self.combine_flux_sources(sources)

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


def stack_datasets(datasets: List[Dataset], 
                   dim: str = "time",
                   method: str = "ffill") -> Dataset:
    """
    Stacks multiple datasets based on the input dimension. By default this is time.
    
    At the moment, the two datasets must have identical coordinate values and 
    variable names for these to be stacked.
    
    Args:
        datasets : List of input datasets
        dim : Name of dimension to stack along. Default = "time"
        method: Method to use when aligning the datasets. Default = "ffill"
    
    Returns:
        Dataset : Stacked dataset

    TODO: Could update this to only allow DataArrays to be included to reduce the phase
    space here.
    TODO: Try this out with different inputs to check this is sensibly robust
    """

    if len(datasets) == 1:
        dataset = datasets[0]
        return dataset

    data_frequency = (ds[dim].diff(dim=dim).mean() for ds in datasets)
    index_highest_freq = min(range(len(data_frequency)), key=data_frequency.__getitem__)
    data_highest_freq = datasets[index_highest_freq]
    coords_to_match = data_highest_freq[dim]

    for i, data in enumerate(datasets):
        data_match = data.reindex({dim: coords_to_match}, method=method)
        if i == 0:
            data_stacked = data_match
            data_stacked.attrs = {}
        else:
            data_stacked += data_match

    return data_stacked


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