"""The ModelScenario class allows users to collate related data sources and calculate
modelled output based on this data. The types of data currently included are:
 - Timeseries observation data (ObsData)
 - Fixed domain sensitivity maps known as footprints (FootprintData)
 - Fixed domain flux maps (FluxData) - multiple maps can be included and
 referenced by source name
 - Fixed domain vertical curtains at each boundary (BoundaryConditionsData)

A ModelScenario instance can be created by searching the object store manually
and providing these outputs:
>>> obs = get_obs_surface(site, species, inlet, ...)
>>> footprint = get_footprint(site, domain, inlet, ...)
>>> flux = get_flux(species, source, domain, ...)
>>> bc = get_bc(species, domain, bc_input, ...)
>>> model = ModelScenario(obs=obs, footprint=footprint, flux=flux, bc=bc)

A ModelScenario instance can also be created using keywords to search the object store:
>>> model = ModelScenario(site,
                          species,
                          inlet,
                          network,
                          domain,
                          sources=sources,
                          bc_input=bc_input,
                          start_date=start_date,
                          end_date=end_date)

A ModelScenario instance can also be initialised and then populated after creation:
>>> model = ModelScenario()
>>> model.add_obs(obs=obs)
>>> model.add_footprint(site, inlet, domain, ...)
>>> model.add_flux(species, domain, sources, ...)
>>> model.add_bc(species, domain, bc_input, ...)

Once created, methods can be called on ModelScenario which will combine these
data sources and cache the outputs (if requested) to make for quicker calculation.

>>> modelled_obs = model.calc_modelled_obs()
>>> modelled_baseline = model.calc_modelled_baseline()
>>> combined_data = model.footprints_data_merge()

If some input types needed for these operations are missing, the user will be alerted
on which data types are missing.
"""

import logging
from typing import Any, Union, cast
from collections.abc import Sequence

import numpy as np
import pandas as pd
from pandas import Timestamp
from xarray import DataArray, Dataset

from openghg.analyse._modelled_obs import fp_x_flux_integrated, fp_x_flux_time_resolved
from openghg.dataobjects import BoundaryConditionsData, FluxData, FootprintData, ObsData, ObsColumnData
from openghg.retrieve import (
    get_obs_surface,
    get_obs_column,
    get_bc,
    get_flux,
    get_footprint,
    search_surface,
    search_bc,
    search_flux,
    search_footprints,
    search_column,
)
from openghg.util import synonyms, clean_string, format_inlet, verify_site_with_satellite, define_platform
from openghg.types import SearchError, ReindexMethod
from ._alignment import combine_datasets, resample_obs_and_other
from ._utils import check_units, match_dataset_dims, stack_datasets


__all__ = ["ModelScenario"]


# TODO: Really with the emissions, they shouldn't need to match against a domain
# We should be able to grab global/bigger area emissions and cut that down
# to whichever area our LPDM model covers.

# TODO: Add static methods for different ways of creating the class
# e.g. from_existing_data(), from_search(), empty() , ...

ParamType = Union[list[dict[str, str | int | None]], dict[str, str | int | None]]


logger = logging.getLogger("openghg.analyse")
logger.setLevel(logging.INFO)  # Have to set level for logger as well as handler


class ModelScenario:
    """This class stores together observation data with ancillary data and allows
    operations to be performed combining these inputs.
    """

    def __bool__(self) -> bool:
        return (
            bool(self.obs)
            or bool(self.obs_column)
            or bool(self.footprint)
            or bool(self.fluxes)
            or bool(self.bc)
        )

    def __init__(
        self,
        site: str | None = None,
        satellite: str | None = None,
        species: str | None = None,
        inlet: str | None = None,
        height: str | None = None,
        network: str | None = None,
        domain: str | None = None,
        platform: str | None = None,
        max_level: int | None = None,
        obs_region: str | None = None,
        selection: str | None = None,
        model: str | None = None,
        met_model: str | None = None,
        fp_inlet: str | list | None = None,
        source: str | None = None,
        sources: str | Sequence | None = None,
        bc_input: str | None = None,
        start_date: str | Timestamp | None = None,
        end_date: str | Timestamp | None = None,
        obs: ObsData | None = None,
        obs_column: ObsColumnData | None = None,
        footprint: FootprintData | None = None,
        flux: FluxData | dict[str, FluxData] | None = None,
        bc: BoundaryConditionsData | None = None,
        store: str | None = None,
    ):
        """Create a ModelScenario instance based on a set of keywords to be
        or directly supplied objects. This can be created as an empty class to be
        populated.

        The keywords are related to observation, footprint and flux data
        which may be available within the object store. The combination of these supplied
        will be used to extract the relevant data. Related keywords are as follows:
         - Observation data: site, species, inlet, network, start_date, end_data
         - Footprint data: site, inlet, domain, model, met_model, species, start_date, end_date
         - Flux data: species, sources, domain, start_date, end_date

        Args:
        site: Site code e.g. "TAC".
        satellite: Satellite name e.g "GOSAT".
        species: Species code e.g. "ch4".
        inlet: Inlet value e.g. "10m".
        height: Alias for inlet.
        network: Network name e.g. "AGAGE".
        domain: Domain name e.g. "EUROPE".
        platform: Platform name e.g "satellite", "column-insitu".
        max_level: Maximum level for processing.
        obs_region: The geographic region covered by the data ("BRAZIL", "INDIA", "UK").
        selection: For satellite only, identifier for any data selection which has been
            performed on satellite data. This can be based on any form of filtering, binning etc.
            but should be unique compared to other selections made e.g. "land", "glint", "upperlimit".
            If not specified, domain will be used.
        model: Model name used in creation of footprint e.g. "NAME".
        met_model: Name of met model used in creation of footprint e.g. "UKV".
        fp_inlet: Specify footprint release height options if this doesn't match the site value.
        source: "anthro" (for "TOTAL"), source name from file otherwise.
        sources: Emissions sources.
        bc_input: Input keyword for boundary conditions e.g. "mozart" or "cams".
        start_date: Start of date range to use. Note for flux this may not be applied.
        end_date: End of date range to use. Note for flux this may not be applied.
        obs: Supply ObsData object directly (e.g. from get_obs...() functions).
        obs_column: Supply ObsColumnData object directly.
        footprint: Supply FootprintData object directly (e.g. from get_footprint() function).
        flux: Supply FluxData object directly (e.g. from get_flux() function).
        bc: Supply BoundaryConditionsData object directly.
        store: Name of object store to retrieve data from.

        Returns:
            None

            Sets up instance of class with associated values.

        TODO: For obs, footprint, flux should we also allow Dataset input and turn
        these into the appropriate class?
        """
        self.obs: ObsData | ObsColumnData | None = None
        self.obs_column: ObsColumnData | None = None
        self.footprint: FootprintData | None = None
        self.fluxes: dict[str, FluxData] | None = None
        self.bc: BoundaryConditionsData | None = None

        if species is not None:
            species = synonyms(species)

        accepted_column_data_types = define_platform(data_type="column")

        self.platform: str | None = platform
        if satellite is not None and platform is None:
            logger.info("You passed a satellite but no platform. Updating the platform to 'satellite'")
            self.platform = "satellite"

        # For ObsColumn data processing
        if platform in accepted_column_data_types:
            # Add observation column data (directly or through keywords, column or satellite)
            if max_level is None:
                raise AttributeError(
                    f"If you are using column-based data (i.e. platform is {accepted_column_data_types}), you need to pass max_level"
                )
            self.add_obs_column(
                site=site,
                satellite=satellite,
                species=species,
                max_level=max_level,
                obs_region=obs_region,
                selection=selection,
                obs_column=obs_column,
                store=store,
            )
        else:
            # Add observation data (directly or through keywords)
            self.add_obs(
                site=site,
                species=species,
                inlet=inlet,
                height=height,
                network=network,
                start_date=start_date,
                end_date=end_date,
                obs=obs,
                store=store,
            )

        # Make sure obs data is present, make sure inputs match to metadata
        if self.obs is not None:
            obs_metadata = self.obs.metadata
            if "satellite" in obs_metadata:
                satellite = obs_metadata["satellite"]
            else:
                site = obs_metadata["site"]
                inlet = obs_metadata["inlet"]
            species = obs_metadata["species"]
            logger.info("Updating any inputs based on observation data")
            logger.info(f"site: {site}, species: {species}, inlet: {inlet}")

        # Add footprint data (directly or through keywords)
        self.add_footprint(
            site=site,
            inlet=inlet,
            satellite=satellite,
            height=height,
            domain=domain,
            model=model,
            met_model=met_model,
            fp_inlet=fp_inlet,
            start_date=start_date,
            end_date=end_date,
            species=species,
            footprint=footprint,
            store=store,
        )

        # Add flux data (directly or through keywords)
        self.add_flux(
            species=species,
            domain=domain,
            source=source,
            sources=sources,
            start_date=start_date,
            end_date=end_date,
            flux=flux,
            store=store,
        )

        # Add boundary conditions (directly or through keywords)
        self.add_bc(
            species=species,
            bc_input=bc_input,
            domain=domain,
            start_date=start_date,
            end_date=end_date,
            bc=bc,
            store=store,
        )

        # Initialise attributes used for caching
        self.scenario: Dataset | None = None
        self.modelled_obs: DataArray | None = None
        self.modelled_baseline: DataArray | None = None
        self.flux_stacked: Dataset | None = None

        # TODO: Check species, site etc. values align between inputs?

    def _get_data(self, keywords: ParamType, data_type: str) -> Any:
        """Use appropriate get function to search for data in object store."""
        get_functions = {
            "obs_surface": get_obs_surface,
            "footprint": get_footprint,
            "flux": get_flux,
            "boundary_conditions": get_bc,
            "obs_column": get_obs_column,
        }

        search_functions = {
            "obs_surface": search_surface,
            "footprint": search_footprints,
            "flux": search_flux,
            "boundary_conditions": search_bc,
            "obs_column": search_column,
        }

        get_fn = get_functions[data_type]
        search_fn = search_functions.get(data_type)

        if isinstance(keywords, dict):
            keywords = [keywords]

        data = None
        num_checks = len(keywords)
        for i, keyword_set in enumerate(keywords):
            try:
                data = get_fn(**keyword_set)  # type:ignore
            except (SearchError, AttributeError):
                num = i + 1
                if num == num_checks:
                    logger.warning(f"Unable to add {data_type} data based on keywords supplied.")
                    logger.warning(" Inputs -")
                    for key, value in keyword_set.items():
                        logger.info(f" {key}: {value}")
                    if search_fn is not None:
                        data_search = search_fn(**keyword_set)  # type:ignore
                        logger.info("---- Search results ---")
                        logger.info(f"Number of results returned: {len(data_search)}")
                        logger.info(data_search.results)
                    print("\n")
                data = None
            else:
                logger.info(f"Adding {data_type} to model scenario")
                break

        return data

    def add_obs(
        self,
        site: str | None = None,
        species: str | None = None,
        inlet: str | None = None,
        height: str | None = None,
        network: str | None = None,
        start_date: str | Timestamp | None = None,
        end_date: str | Timestamp | None = None,
        obs: ObsData | None = None,
        store: str | None = None,
    ) -> None:
        """Add observation data based on keywords or direct ObsData object."""

        # Search for obs data based on keywords
        if site is not None and obs is None:
            site = clean_string(site)

            if height is not None and inlet is None:
                inlet = height
            inlet = clean_string(inlet)
            inlet = format_inlet(inlet)

            # search for obs based on suitable keywords - site, species, inlet
            obs_keywords = {
                "site": site,
                "species": species,
                "inlet": inlet,
                "network": network,
                "start_date": start_date,
                "end_date": end_date,
                "store": store,
            }

            obs = self._get_data(obs_keywords, data_type="obs_surface")

        self.obs = obs

        # Add keywords to class for convenience
        if self.obs is not None:
            self.site = self.obs.metadata["site"]
            self.species = self.obs.metadata["species"]
            self.inlet = self.obs.metadata["inlet"]

    def add_obs_column(
        self,
        site: str | None = None,
        satellite: str | None = None,
        max_level: int | None = None,
        species: str | None = None,
        platform: str | None = None,
        obs_region: str | None = None,
        domain: str | None = None,
        selection: str | None = None,
        network: str | None = None,
        obs_column: ObsColumnData | None = None,
        store: str | None = None,
    ) -> None:
        """Add column data based on keywords or direct ObsColumnData object."""

        # Search for obs data based on keywords
        if site is not None and obs_column is None and satellite is None:
            site = clean_string(site)
        elif obs_column is None:
            verify_site_with_satellite(
                site=site, satellite=satellite, obs_region=obs_region, selection=selection
            )

            # search for obs based on suitable keywords - site, species, inlet
            obs_column_keywords = {
                "site": site,
                "satellite": satellite,
                "max_level": max_level,
                "species": species,
                "network": network,
                "store": store,
                "platform": platform,
                "obs_region": obs_region,
                "domain": domain,
                "selection": selection,
            }

            obs_column = self._get_data(obs_column_keywords, data_type="obs_column")

        # Updating obs to be obs_column
        self.obs = obs_column

        # Add keywords to class for convenience
        if self.obs_column is not None:
            if self.site is not None:
                self.site = self.obs_column.metadata["site"]
            else:
                self.satellite = self.obs_column.metadata["satellite"]

            self.species = self.obs_column.metadata["species"]

    def add_footprint(
        self,
        site: str | None = None,
        inlet: str | None = None,
        height: str | None = None,
        domain: str | None = None,
        model: str | None = None,
        satellite: str | None = None,
        obs_region: str | None = None,
        met_model: str | None = None,
        start_date: str | Timestamp | None = None,
        end_date: str | Timestamp | None = None,
        species: str | None = None,
        fp_inlet: str | list | None = None,
        network: str | None = None,
        footprint: FootprintData | None = None,
        store: str | None = None,
    ) -> None:
        """Add footprint data based on keywords or direct FootprintData object."""
        from openghg.util import (
            species_lifetime,
            extract_height_name,
        )

        # Search for footprint data based on keywords
        # - site, domain, inlet (can extract from obs / height_name), model, met_model

        if footprint is None and (site is not None or satellite is not None):
            if site is not None:
                site = clean_string(site)

            if satellite is not None and fp_inlet is None:
                fp_inlet = "column"

            if site is not None and fp_inlet is None:
                # use obs network if we're trying to infer the height from site info
                if self.obs is not None:
                    network = network or self.obs.metadata.get("network")

                if site is not None:
                    height_name = extract_height_name(site, network, inlet)
                if height_name is not None:
                    fp_inlet = height_name
                    logger.info(
                        f"Using height_name option(s) for footprint inlet: {fp_inlet}."
                        f"\nInferred from site={site}, network={network}, inlet={inlet}"
                    )

            if fp_inlet is None:
                if inlet is None and self.obs is not None:
                    fp_inlet = self.obs.metadata["inlet"]
                    logger.info(f"Using observations' inlet height for footprints: {fp_inlet}")
                elif inlet is None and height is not None:
                    fp_inlet = clean_string(height)
                else:
                    fp_inlet = clean_string(inlet)

            # TODO: Add case to deal with "multiple" inlets
            # In this case would need to find different footprints for different inlet values
            if fp_inlet == "multiple":
                raise ValueError(
                    "Unable to deal with multiple inlets yet:\n Please change date range or specify a specific inlet"
                )

            if isinstance(fp_inlet, list):
                fp_inlet_options = fp_inlet
            else:
                fp_inlet_options = [fp_inlet]

            fp_inlet_options = [format_inlet(value) for value in fp_inlet_options]

            footprint_keyword_options = []
            for fp_inlet_option in fp_inlet_options:
                footprint_keywords = {
                    "site": site,
                    "height": fp_inlet_option,
                    "satellite": satellite,
                    "obs_region": obs_region,
                    "inlet": fp_inlet_option,
                    "domain": domain,
                    "model": model,
                    # "met_model": met_model,  # Should be added to inputs for get_footprint()
                    "start_date": start_date,
                    "end_date": end_date,
                    "store": store,
                }

                # Check whether general inert footprint should be extracted (suitable for long-lived species)
                # or species specific footprint
                #  - needed for short-lived species (includes additional parameters for age of particles)
                #  - needed for carbon dioxide (include high time resolution footprint)
                species_lifetime_value = species_lifetime(species)
                if species_lifetime_value is not None or species == "co2":
                    footprint_keywords["species"] = species

                footprint_keyword_options.append(footprint_keywords)

            footprint = self._get_data(footprint_keyword_options, data_type="footprint")

        self.footprint = footprint

        if self.footprint is not None:
            if "satellite" in self.footprint.metadata and self.footprint.metadata["satellite"] is not None:
                self.satellite = self.footprint.metadata["satellite"]
            else:
                fp_inlet = self.footprint.metadata["inlet"]
                self.fp_inlet = fp_inlet
                if not hasattr(self, "site"):
                    self.site = self.footprint.metadata["site"]

    def add_flux(
        self,
        species: str | None = None,
        domain: str | None = None,
        source: str | None = None,
        sources: str | Sequence | None = None,
        start_date: str | Timestamp | None = None,
        end_date: str | Timestamp | None = None,
        flux: FluxData | dict[str, FluxData] | None = None,
        store: str | None = None,
    ) -> None:
        """Add flux data based on keywords or direct FluxData object.
        Can add flux datasets for multiple sources.
        """
        if self.fluxes is not None:
            # Check current species in any flux data
            if species is not None:
                current_flux_1 = list(self.fluxes.values())[0]
                current_species = current_flux_1.metadata["species"]
                if species != current_species:
                    raise ValueError(
                        f"New data must match current species {current_species} in ModelScenario. Input value: {species}"
                    )

        if species is not None and flux is None:
            flux = {}

            if sources is None and source is not None:
                sources = [source]
            elif sources is None or isinstance(sources, str):
                sources = [sources]

            for name in sources:
                flux_keywords_1 = {"species": species, "source": name, "domain": domain, "store": store}

                # For CO2 we need additional emissions data before a start_date to
                # match to high time resolution footprints.
                # For now, just extract all data
                if species == "co2":
                    flux_keywords = [flux_keywords_1]
                else:
                    flux_keywords_2 = flux_keywords_1.copy()

                    flux_keywords_1["start_date"] = start_date
                    flux_keywords_1["end_date"] = end_date

                    flux_keywords = [flux_keywords_1, flux_keywords_2]

                # TODO: Add something to allow for e.g. global domain or no domain

                flux_source = self._get_data(flux_keywords, data_type="flux")
                # TODO: May need to update this check if flux_source is empty FluxData() object

                if flux_source is not None:
                    # try to infer source name from FluxData metadata
                    if name is None and len(sources) == 1:
                        try:
                            name = flux_source.metadata["source"]
                        except KeyError:
                            raise ValueError(
                                "Flux 'source' could not be inferred from metadata/attributes. Please specify the source(s) of the flux."
                            )
                    flux[name] = flux_source

        elif flux is not None:
            if not isinstance(flux, dict):
                try:
                    name = flux.metadata["source"]
                except KeyError:
                    raise ValueError(
                        "Flux 'source' could not be inferred from `flux` metadata/attributes. Please specify the source(s) of the flux in the `FluxData` metadata.."
                    )
                else:
                    flux = {name: flux}

        # TODO: Make this so flux.anthro can be called etc. - link in some way
        if self.fluxes is not None:
            if flux:
                self.fluxes.update(flux)
        # Flux can be None or empty dict.
        elif flux:
            self.fluxes = flux

        if self.fluxes is not None:
            if not hasattr(self, "species"):
                flux_values = list(self.fluxes.values())

                flux_1 = flux_values[0]
                self.species = flux_1.metadata["species"]

            self.flux_sources = list(self.fluxes.keys())

    def add_bc(
        self,
        species: str | None = None,
        bc_input: str | None = None,
        domain: str | None = None,
        start_date: str | Timestamp | None = None,
        end_date: str | Timestamp | None = None,
        bc: BoundaryConditionsData | None = None,
        store: str | None = None,
    ) -> None:
        """Add boundary conditions data based on keywords or direct BoundaryConditionsData object."""
        # Search for boundary conditions data based on keywords
        # - domain, species, bc_input
        if domain is not None and bc is None:
            bc_keywords = {
                "species": species,
                "domain": domain,
                "bc_input": bc_input,
                "start_date": start_date,
                "end_date": end_date,
                "store": store,
            }

            bc = self._get_data(bc_keywords, data_type="boundary_conditions")

        self.bc = bc

    def _check_data_is_present(self, need: str | Sequence | None = None) -> None:
        """Check whether correct data types have been included. This should
        be used by functions to check whether they can perform the requested
        operation with the data types available.

        Args:
            need (list) : Names of objects needed for the function being called.
            Should be one or more of "obs", "footprint", "fluxes" (or "flux")

        Returns:
            None

            Raises ValueError is necessary data is missing.
        """
        if need is None:
            need = ["obs", "footprint"]
        elif isinstance(need, str):
            need = [need]

        need = ["fluxes" if value == "flux" else value for value in need]  # Make sure attributes match
        missing = []
        for attr in need:
            value = getattr(self, attr)
            if value is None:
                missing.append(attr)

                logger.error(f"Must have {attr} data linked to this ModelScenario to run this function")
                logger.error("Include this by using the add function, with appropriate inputs:")
                logger.error("  ModelScenario.add_{attr}(...)")

        if missing:
            raise ValueError(f"Missing necessary {' and '.join(missing)} data.")

    def _get_platform(self) -> str | None:
        """Find the platform for a site, if present.

        To find this information this will look within:
            - obs metadata
            - the "site_info.json" file from openghg_defs dependency.
        """
        from openghg.util import get_platform_from_info, not_set_metadata_values

        if self.obs is not None:
            metadata = self.obs.metadata
            platform: str | None = metadata.get("platform")

            # Check for values which indicate this has not been specified
            not_set_values = not_set_metadata_values()
            if platform in not_set_values:
                platform = None

        if hasattr(self, "site") and self.site is not None:
            platform_from_site = get_platform_from_info(self.site)
            if platform is None:
                logger.info(f"Platform of '{platform}' for site '{self.site}' extracted from site_info.json")
                platform = platform_from_site
            elif platform_from_site is not None:
                if platform != platform_from_site:
                    logger.warning(
                        f"Platform from metadata and site_info details do not match ({platform}, {platform_from_site}). Using platform value from metadata."
                    )

        return platform

    def _resample_obs_footprint(self, resample_to: str | None = "coarsest") -> tuple:
        """Slice and resample obs and footprint data to align along time

        This slices the date to the smallest time frame
        spanned by both the footprint and obs, using the sliced start date
        The time dimension is resampled based on the resample_to input using the mean.
        The resample_to options are:
         - "coarsest" - resample to the coarsest resolution between obs and footprints
         - "obs" - resample to observation data frequency
         - "footprint" - resample to footprint data frequency
         - a valid resample period e.g. "2H"
         - None to not resample and just return original values

        Args:
            resample_to: Resample option to use: either data based or using a valid pandas resample period.

        Returns:
            tuple: Two xarray.Dataset with aligned time dimensions
        """
        # Check data is present (not None) and cast to correct type
        self._check_data_is_present(need=["obs", "footprint"])
        obs = cast(ObsData, self.obs)
        footprint = cast(FootprintData, self.footprint)

        obs_data = obs.data
        footprint_data = footprint.data

        if resample_to is None:
            return obs_data, footprint_data

        if resample_to == "footprint":
            resample_to = "other"

        return resample_obs_and_other(obs_data, footprint_data, resample_to=resample_to)

    def combine_obs_footprint(
        self,
        resample_to: str | None = "coarsest",
        platform: str | None = None,
        cache: bool = True,
        recalculate: bool = False,
    ) -> Dataset:
        """Combine observation and footprint data so these are on the same time
        axis. This will both slice and resample the data to align this axis.

        - Data is slices to smallest timeframe spanned by both footprint and obs
        - Data is resampled according to resample_to input and using the mean
        - Data is combined into one dataset

        Args:
            resample_to: Resample option to use for averaging:
                          - either one of ["coarsest", "obs", "footprint"] to match to the datasets
                          - or using a valid pandas resample period e.g. "2H".
                          - None to not resample and to just "ffill" footprint to obs
                         Default = "coarsest".
            platform: Observation platform used to decide on resample and alignment steps.
                If this is not supplied, function will attempt to extract this value from
                from the metadata, then the openghg_defs site_info.json details for the site.
            cache: Cache this data after calculation. Default = True.

        Returns:
            xarray.Dataset: Combined dataset aligned along the time dimension

            If cache is True:
                This data will be also be cached as the ModelScenario.scenario attribute.
        """
        self._check_data_is_present(need=["obs", "footprint"])
        obs = cast(ObsData, self.obs)
        footprint = cast(FootprintData, self.footprint)

        # Return any matching cached data
        if self.scenario is not None and not recalculate:
            if self.scenario.attrs["resample_to"] == resample_to:
                return self.scenario

        # Extract platform
        if platform is None:
            platform = self._get_platform()

        # Set default reindex method and tolerance values
        merge_method: ReindexMethod = "ffill"
        tolerance = None

        # If platform is specified, update resample_to, reindex and tolerance values where appropriate
        if platform is not None:
            platform = platform.lower()
            if platform == "satellite":
                # Update reindex method and tolerance for satellite
                resample_to = None
                merge_method = "nearest"
                tolerance = pd.Timedelta("1ms")
                logger.info(
                    f"Platform '{platform}' has been used to determine the resample and alignment stategy (no resampling, alignment as {merge_method} with {tolerance} tolerance."
                )
            elif "flask" in platform:
                # TODO: Iss #253. Update this to be smarter about averaging irregular, flask data
                # May need to access different method than resample
                logger.info(
                    f"Platform '{platform}' has been used to determine the resample stategy (no resampling)."
                )
                resample_to = None
            else:
                logger.warning(
                    f"Platform '{platform}' not used when determining resample and merge strategy."
                )

        # Resample, align and merge the observation and footprint Datasets
        resampled_obs, resampled_footprint = self._resample_obs_footprint(resample_to=resample_to)
        combined_dataset = combine_datasets(
            dataset_A=resampled_obs, dataset_B=resampled_footprint, tolerance=tolerance, method=merge_method
        )

        # Transpose to keep time in the last dimension position in case it has been moved in resample
        combined_dataset = combined_dataset.transpose(..., "time")

        # Save the observation data units
        try:
            mf = obs.data["mf"]
            units: float | None = float(mf.attrs["units"])
        except (ValueError, KeyError):
            units = None
        except AttributeError:
            raise AttributeError("Unable to read mf attribute from observation data.")

        if units is not None:
            combined_dataset.update({"fp": (combined_dataset.fp.dims, (combined_dataset["fp"].data / units))})
            if self.species == "co2":
                combined_dataset.update(
                    {
                        "fp_HiTRes": (
                            combined_dataset.fp_HiTRes.dims,
                            (combined_dataset.fp_HiTRes.data / units),
                        )
                    }
                )

        attributes = {}
        attributes_obs = obs.data.attrs
        attributes_footprint = footprint.data.attrs
        attributes.update(attributes_footprint)
        attributes.update(attributes_obs)

        attributes["resample_to"] = str(resample_to)

        combined_dataset.attrs.update(attributes)

        if cache:
            self.scenario = combined_dataset

        return combined_dataset

    def _clean_sources_input(self, sources: str | list | None = None) -> list:
        """Check sources input and make sure this is a list. If None, this will extract
        all sources from self.fluxes.
        """
        self._check_data_is_present(need=["fluxes"])
        flux_dict = cast(dict[str, FluxData], self.fluxes)

        if sources is None:
            sources = list(flux_dict.keys())
        elif isinstance(sources, str):
            sources = [sources]

        return sources

    def combine_flux_sources(
        self, sources: str | list | None = None, cache: bool = True, recalculate: bool = False
    ) -> Dataset:
        """Combine together flux sources on the time dimension. This will align to
        the time of the highest frequency flux source both for time range and frequency.

        Args:
            sources : Names of sources to combine. Should already be attached to ModelScenario.
            cache : Cache this data after calculation. Default = True

        Returns:
            Dataset: All flux sources stacked on the time dimension.
        """
        self._check_data_is_present(need=["fluxes"])
        flux_dict = cast(dict[str, FluxData], self.fluxes)

        time_dim = "time"

        sources = self._clean_sources_input(sources)
        sources_str = ", ".join(sources)

        # Return any matching cached data
        if self.flux_stacked is not None and not recalculate:
            if self.flux_stacked.attrs["sources"] == sources_str:
                return self.flux_stacked

        flux_datasets = [flux_dict[source].data for source in sources]

        if len(sources) == 1:
            return flux_datasets[0]

        # Make sure other dimensions than time are aligned between flux datasets
        # - expects values to be closely aligned so only allows for a small floating point tolerance
        dims = list(flux_datasets[0].dims)
        dims.remove("time")
        flux_datasets = match_dataset_dims(flux_datasets, dims=dims)

        try:
            flux_stacked = stack_datasets(flux_datasets, dim=time_dim, method="ffill")
        except ValueError:
            raise ValueError(f"Unable to combine flux data for sources: {sources_str}")

        if cache:
            flux_stacked.attrs["sources"] = sources_str
            self.flux_stacked = flux_stacked

        return flux_stacked

    def _check_footprint_resample(self, resample_to: str | None) -> Dataset:
        """Check whether footprint needs resampling based on resample_to input.
        Ignores resample_to keywords of ("coarsest", "obs", "footprint") as this is
        for comparison with observation data but uses pandas frequencies to resample.
        """
        footprint = cast(FootprintData, self.footprint)

        if resample_to in ("coarsest", "obs", "footprint") or resample_to is None:
            return footprint.data
        else:
            footprint_data = footprint.data
            time = footprint_data["time"].values
            start_date = Timestamp(time[0])
            offset = pd.Timedelta(
                hours=start_date.hour + start_date.minute / 60.0 + start_date.second / 3600.0
            )
            offset = cast(pd.Timedelta, offset)  # mypy thinks this could be NaT
            footprint_data = footprint_data.resample(indexer={"time": resample_to}, offset=offset).mean()
            return footprint_data

    def _param_setup(
        self,
        param: str = "modelled_obs",
        resample_to: str | None = "coarsest",
        platform: str | None = None,
        recalculate: bool = False,
    ) -> bool:
        """Decide if calculation is needed for input parameter and set up
        underlying parameters accordingly. This will populate the
        self.scenario attribute if not already present or if this needs
        to be recalculated.

        Args:
            param : Name of the parameter being calculated.
                    Should be one of "modelled_obs", "modelled_baseline"
            resample_to: Resample option to use for averaging:
                          - either one of ["coarsest", "obs", "footprint"] to match to the datasets
                          - or using a valid pandas resample period e.g. "2H".
                          - None to not resample and to just "ffill" footprint to obs
                         Default = "coarsest".
            platform: Observation platform used to decide whether to resample e.g. "satellite", "insitu", "flask".
            cache: Cache this data after calculation. Default = True.
            recalculate: Make sure to recalculate this data rather than return from cache. Default = False.

        Returns:
            bool: True if param should be calculated, False otherwise

            Populates details of ModelScenario.scenario to use in calculation.
        """
        try:
            parameter = getattr(self, param)
        except AttributeError:
            raise ValueError(f"Did not recognise input for {param}")

        # Check if cached modelled observations exist
        # if self.modelled_obs is None or recalculate:
        if parameter is None or recalculate:
            # Check if observations are present and use these for resampling
            if self.obs is not None or self.obs_column is not None:
                self.combine_obs_footprint(
                    resample_to, platform=platform, recalculate=recalculate, cache=True
                )
            else:
                self.scenario = self._check_footprint_resample(resample_to)
        elif self.obs is not None:
            # Check previous resample_to input for cached data
            # prev_resample_to = self.modelled_obs.attrs.get("resample_to")
            prev_resample_to = parameter.attrs.get("resample_to")

            # Check if this previous resample period matches input value
            # - if not (or explicit recalculation requested), recreate scenario
            # - if so return cached modelled observations
            if prev_resample_to != str(resample_to) or recalculate:
                self.combine_obs_footprint(resample_to, platform=platform, cache=True)
            else:
                # return self.modelled_obs
                return False
        elif recalculate:
            # Recalculate based on footprint data if obs not present
            self.scenario = self._check_footprint_resample(resample_to)

        # TODO: Add check for matching sources and recalculate otherwise
        else:
            # Return cached modelled observations if explicit recalculation not requested
            # return self.modelled_obs
            return False

        return True

    def calc_modelled_obs(
        self,
        sources: str | list | None = None,
        resample_to: str | None = "coarsest",
        platform: str | None = None,
        cache: bool = True,
        recalculate: bool = False,
    ) -> DataArray:
        """Calculate the modelled observation points based on site footprint and fluxes.

        The time points returned are dependent on the resample_to option chosen.
        If obs data is also linked to the ModelScenario instance, this will be used
        to derive the time points where appropriate.

        Args:
            sources: Sources to use for flux. All will be used and stacked if not specified.
            resample_to: Resample option to use for averaging:
                          - either one of ["coarsest", "obs", "footprint"] to match to the datasets
                          - or using a valid pandas resample period e.g. "2H".
                          - None to not resample and to just "ffill" footprint to obs
                         Default = "coarsest".
            platform: Observation platform used to decide whether to resample e.g. "satellite", "insitu", "flask"
            cache: Cache this data after calculation. Default = True.
            recalculate: Make sure to recalculate this data rather than return from cache. Default = False.

        Returns:
            xarray.DataArray: Modelled observation values along the time axis

            If cache is True:
                This data will also be cached as the ModelScenario.modelled_obs attribute.
                The associated scenario data will be cached as the ModelScenario.scenario attribute.
        """
        self._check_data_is_present(need=["footprint", "fluxes"])

        param_calculate = self._param_setup(
            param="modelled_obs", resample_to=resample_to, platform=platform, recalculate=recalculate
        )

        if not param_calculate:
            modelled_obs = cast(DataArray, self.modelled_obs)
            return modelled_obs

        # Check species and use high time resolution steps if this is carbon dioxide
        if self.species == "co2":
            modelled_obs = self._calc_modelled_obs_HiTRes(
                sources=sources, output_TS=True, output_fpXflux=False
            )
            name = "mf_mod_high_res"
        else:
            modelled_obs = self._calc_modelled_obs_integrated(
                sources=sources, output_TS=True, output_fpXflux=False
            )
            name = "mf_mod"

        modelled_obs.attrs["resample_to"] = str(resample_to)
        modelled_obs = modelled_obs.rename(name)

        # Cache output from calculations
        if cache:
            logger.info("Caching calculated data")
            self.modelled_obs = modelled_obs
            # self.scenario[name] = modelled_obs
        else:
            self.modelled_obs = None  # Make sure this is reset and not cached
            self.scenario = None  # Reset this to None after calculation completed

        return modelled_obs

    def _calc_modelled_obs_integrated(
        self,
        sources: str | list | None = None,
        output_TS: bool = True,
        output_fpXflux: bool = False,
    ) -> Any:
        """Calculate modelled mole fraction timeseries using integrated footprints data.

        Args:
            sources : Flux sources to use for the calculation. By default this will use all available sources.
            output_TS : Whether to output the modelled mole fraction timeseries DataArray.
                       Default = True
            output_fpXflux : Whether to output the modelled flux map DataArray used to create
                            the timeseries. Default = False

        Returns:
            DataArray / DataArray :
                Modelled mole fraction timeseries, dimensions = (time)
                Modelled flux map, dimensions = (lat, lon, time)

            If one of output_TS and output_fpXflux are True:
                DataArray is returned for the respective output

            If both output_TS and output_fpXflux are both True:
                Both DataArrays are returned.
        """
        if self.scenario is None:
            raise ValueError("Combined data must have been defined before calling this function.")

        scenario = self.scenario
        flux = self.combine_flux_sources(sources)
        flux_modelled = fp_x_flux_integrated(scenario, flux)
        timeseries: DataArray = flux_modelled.sum(["lat", "lon"])

        # TODO: Add details about units to output

        if output_TS and output_fpXflux:
            return timeseries, flux_modelled
        elif output_TS:
            return timeseries
        elif output_fpXflux:
            return flux_modelled

    def _calc_modelled_obs_HiTRes(
        self,
        sources: str | list | None = None,
        averaging: str | None = None,
        output_TS: bool = True,
        output_fpXflux: bool = False,
    ) -> Any:
        """Calculate modelled mole fraction timeseries using high time resolution
        footprints data and emissions data. This is appropriate for time variable
        species reliant on high time resolution footprints such as carbon dioxide (co2).

        Args:
            sources : Flux sources to use for the calculation. By default this will use all available sources.
            averaging : Time resolution to use to average the time dimension. Default = None
            output_TS : Whether to output the modelled mole fraction timeseries DataArray.
                       Default = True
            output_fpXflux : Whether to output the modelled flux map DataArray used to create
                            the timeseries. Default = False

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

        # TODO: Need to work out how this fits in with high time resolution method
        # Do we need to flag low resolution to use a different method? natural / anthro for example

        if self.scenario is None:
            raise ValueError("Combined data must have been defined before calling this function.")

        fp_HiTRes = self.scenario.fp_HiTRes
        flux_ds = self.combine_flux_sources(sources)

        fpXflux = fp_x_flux_time_resolved(fp_HiTRes, flux_ds, averaging=averaging)

        if output_TS:
            timeseries = fpXflux.sum(["lat", "lon"])

        # TODO: Add details about units to output

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

    def calc_modelled_baseline(
        self,
        resample_to: str | None = "coarsest",
        platform: str | None = None,
        output_units: float = 1e-9,
        cache: bool = True,
        recalculate: bool = False,
    ) -> DataArray:
        """Calculate the modelled baseline points based on site footprint and boundary conditions.
        Boundary conditions are multipled by any loss (exp(-t/lifetime)) for the species.

        The time points returned are dependent on the resample_to option chosen.
        If obs data is also linked to the ModelScenario instance, this will be used
        to derive the time points where appropriate.

        Args:
            resample_to: Resample option to use for averaging:
                          - either one of ["coarsest", "obs", "footprint"] to match to the datasets
                          - or using a valid pandas resample period e.g. "2H".
                          - None to not resample and to just "ffill" footprint to obs
                         Default = "coarsest".
            platform: Observation platform used to decide whether to resample e.g. "satellite", "insitu", "flask"
            cache: Cache this data after calculation. Default = True.
            recalculate: Make sure to recalculate this data rather than return from cache. Default = False.

        Returns:
            xarray.DataArray: Modelled baselined values along the time axis

            If cache is True:
                This data will also be cached as the ModelScenario.modelled_baseline attribute.
                The associated scenario data will be cached as the ModelScenario.scenario attribute.
        """
        from openghg.util import check_lifetime_monthly, species_lifetime, time_offset

        self._check_data_is_present(need=["footprint", "bc"])
        bc = cast(BoundaryConditionsData, self.bc)

        param_calculate = self._param_setup(
            param="modelled_baseline", resample_to=resample_to, platform=platform, recalculate=recalculate
        )

        if not param_calculate:
            modelled_baseline = cast(DataArray, self.modelled_baseline)
            return modelled_baseline

        scenario = cast(Dataset, self.scenario)
        bc_data = bc.data

        bc_data = bc_data.reindex_like(scenario, "ffill")

        lifetime_value = species_lifetime(self.species)
        check_monthly = check_lifetime_monthly(lifetime_value)

        if check_monthly:
            lifetime_monthly = cast(list[str] | None, lifetime_value)
            lifetime: str | None = None
        else:
            lifetime_monthly = None
            lifetime = cast(str | None, lifetime_value)

        if lifetime is not None:
            short_lifetime = True
            lt_time_delta = time_offset(period=lifetime)
            lifetime_hrs: float | np.ndarray = lt_time_delta.total_seconds() / 3600.0
        elif lifetime_monthly:
            short_lifetime = True
            lifetime_monthly_hrs = []
            for lt in lifetime_monthly:
                lt_time_delta = time_offset(period=lt)
                lt_hrs = lt_time_delta.total_seconds() / 3600.0
                lifetime_monthly_hrs.append(lt_hrs)

            # calculate the lifetime_hrs associated with each time point in scenario data
            # this is because lifetime can be a list of monthly values
            time_month = scenario["time"].dt.month
            lifetime_hrs = np.array([lifetime_monthly_hrs[item - 1] for item in time_month.values])
        else:
            short_lifetime = False

        # Include loss condition if lifetime of species is specified
        if short_lifetime:
            expected_vars = (
                "mean_age_particles_n",
                "mean_age_particles_e",
                "mean_age_particles_s",
                "mean_age_particles_w",
            )
            for var in expected_vars:
                if var not in scenario.data_vars:
                    raise ValueError(
                        f"Unable to calculate baseline for short-lived species {self.species} without species specific footprint."
                    )

            # Ignoring type below -  - problem with xarray patching np.exp to return DataArray rather than ndarray
            loss_n: DataArray | float = np.exp(-1 * scenario["mean_age_particles_n"] / lifetime_hrs).rename(  # type: ignore
                "loss_n"
            )
            loss_e: DataArray | float = np.exp(-1 * scenario["mean_age_particles_e"] / lifetime_hrs).rename(  # type: ignore
                "loss_e"
            )
            loss_s: DataArray | float = np.exp(-1 * scenario["mean_age_particles_s"] / lifetime_hrs).rename(  # type: ignore
                "loss_s"
            )
            loss_w: DataArray | float = np.exp(-1 * scenario["mean_age_particles_w"] / lifetime_hrs).rename(  # type: ignore
                "loss_w"
            )

        else:
            loss_n = 1.0
            loss_e = 1.0
            loss_s = 1.0
            loss_w = 1.0

        # Check and extract units as float, if present.
        units_default = 1.0
        units_n = check_units(bc_data["vmr_n"], default=units_default)
        units_e = check_units(bc_data["vmr_e"], default=units_default)
        units_s = check_units(bc_data["vmr_s"], default=units_default)
        units_w = check_units(bc_data["vmr_w"], default=units_default)

        modelled_baseline = (
            (scenario["particle_locations_n"] * bc_data["vmr_n"] * loss_n * units_n / output_units).sum(
                ["height", "lon"]
            )
            + (scenario["particle_locations_e"] * bc_data["vmr_e"] * loss_e * units_e / output_units).sum(
                ["height", "lat"]
            )
            + (scenario["particle_locations_s"] * bc_data["vmr_s"] * loss_s * units_s / output_units).sum(
                ["height", "lon"]
            )
            + (scenario["particle_locations_w"] * bc_data["vmr_w"] * loss_w * units_w / output_units).sum(
                ["height", "lat"]
            )
        )

        modelled_baseline.attrs["resample_to"] = str(resample_to)
        modelled_baseline.attrs["units"] = output_units
        modelled_baseline = modelled_baseline.rename("bc_mod")

        # Cache output from calculations
        if cache:
            logger.info("Caching calculated data")
            self.modelled_baseline = modelled_baseline
            # self.scenario[name] = modelled_obs
        else:
            self.modelled_baseline = None  # Make sure this is reset and not cached
            self.scenario = None  # Reset this to None after calculation completed

        return modelled_baseline

    # def _calc_modelled_baseline_long_lived():
    #     pass

    # def _calc_modelled_baseline_short_lived():
    #     pass

    def footprints_data_merge(
        self,
        resample_to: str | None = "coarsest",
        platform: str | None = None,
        calc_timeseries: bool = True,
        sources: str | list | None = None,
        calc_bc: bool = True,
        cache: bool = True,
        recalculate: bool = False,
    ) -> Dataset:
        """Produce combined object containing aligned footprint and observation data.
        Can also include modelled timeseries data derived from flux.

        Args:
            resample_to: Resample option to use for averaging:
                          - either one of ["coarsest", "obs", "footprint"] to match to the datasets
                          - or using a valid pandas resample period e.g. "2H".
                          - None to not resample and to just "ffill" footprint to obs
                         Default = "coarsest".
            platform: Observation platform used to decide whether to resample.
            calc_timeseries: Calculate modelled timeseries based on flux sources.
            sources: Sources to use for flux if calc_timseries is True.
                     All will be used and stacked if not specified.
            calc_baseline: Calculate modelled baseline.
            cache: Cache this data after calculation. Default = True.
            recalculate: Make sure to recalculate this data rather than return from cache. Default = False.

        Returns:
            xarray.Dataset: Combined dataset containing footprint and observation data
        """
        combined_dataset = self.combine_obs_footprint(
            resample_to=resample_to, platform=platform, cache=cache, recalculate=recalculate
        )

        if calc_timeseries:
            modelled_obs = self.calc_modelled_obs(
                resample_to=resample_to,
                sources=sources,
                platform=platform,
                cache=cache,
                recalculate=recalculate,
            )

            name = modelled_obs.name
            combined_dataset = combined_dataset.assign({name: modelled_obs})

        if calc_bc:
            if self.bc is not None:
                modelled_baseline = self.calc_modelled_baseline(
                    resample_to=resample_to,
                    platform=platform,
                    cache=cache,
                    recalculate=recalculate,
                )
                name = modelled_baseline.name
                combined_dataset = combined_dataset.assign({name: modelled_baseline})
            else:
                logger.warning(
                    "Unable to calculate baseline data. Add boundary conditions using ModelScenarion.add_bc(...) to do this."
                )

        if cache:
            self.scenario = combined_dataset

        return combined_dataset

    def plot_timeseries(self) -> Any:
        """Plot the observation timeseries data.

        Returns:
            Plotly Figure

            Interactive plotly graph created with observations
        """
        self._check_data_is_present(need="obs")

        obs = cast(ObsData, self.obs)

        fig = obs.plot_timeseries()  # Calling method on ObsData class

        return fig

    def plot_comparison(
        self,
        baseline: str | None = "boundary_conditions",
        sources: str | list | None = None,
        resample_to: str | None = "coarsest",
        platform: str | None = None,
        cache: bool = True,
        recalculate: bool = False,
    ) -> Any:
        """Plot comparison between observation and modelled timeseries data.

        Args:
            baseline: Add baseline to data. One of:
                          - "boundary_conditions" - Uses added boundary conditions to calculate modelled baseline
                          - "percentile" - Calculates the 1% value across the whole time period
                          - None - don't add a baseline and only plot the modelled observations
            sources: Sources to use for flux. All will be used and stacked if not specified.
            resample_to: Resample option to use for averaging:
                          - either one of ["coarsest", "obs", "footprint"] to match to the datasets
                          - or using a valid pandas resample period e.g. "2H".
                          - None to not resample and to just "ffill" footprint to obs
                         Default = "coarsest".
            platform: Observation platform used to decide whether to resample e.g. "satellite", "insitu", "flask"
            cache: Cache this data after calculation. Default = True.
            recalculate: Make sure to recalculate this data rather than return from cache. Default = False.

        Returns:
            Plotly Figure

            Interactive plotly graph created with observation and modelled observation data.
        """
        # Only import plotly when we need to - not needed if not plotting.
        import plotly.graph_objects as go

        self._check_data_is_present(need=["obs", "footprint", "flux"])
        obs = cast(ObsData, self.obs)

        fig = obs.plot_timeseries()

        modelled_obs = self.calc_modelled_obs(
            sources=sources, resample_to=resample_to, platform=platform, cache=cache, recalculate=recalculate
        )
        x_data = modelled_obs["time"]
        y_data = modelled_obs.data

        species = self.species
        if sources is None:
            sources = self.flux_sources
        elif isinstance(sources, str):
            sources = [sources]

        if sources is not None:
            source_str = ", ".join(sources)
            label = f"Modelled {species.upper()}: {source_str}"
        else:
            label = f"Modelled {species.upper()}"

        # TODO: Check modelled_obs units and ensure these match to modelled_baseline
        # - currently modelled_baseline outputs in 1e-9 (ppb) by default.

        if baseline == "boundary_conditions":
            if self.bc is not None:
                modelled_baseline = self.calc_modelled_baseline(
                    resample_to=resample_to, platform=platform, cache=cache, recalculate=recalculate
                )
                y_baseline = modelled_baseline.data
                y_data = y_data + y_baseline
            else:
                logger.warning("Unable to calculate baseline from boundary conditions")
        elif baseline == "percentile":
            mf = obs.data["mf"]
            y_baseline = mf.quantile(1.0, dim="time").values
            y_data = y_data + y_baseline

        fig.add_trace(go.Scatter(x=x_data, y=y_data, mode="lines", name=label))

        return fig


# def footprints_data_merge(data: Union[dict, ObsData],
#                           domain: str,
#                           met_model: Optional[str] = None,
#                           load_flux: bool = True,
#                           load_bc: bool = True,
#                           calc_timeseries: bool = True,
#                           calc_bc: bool = True,
#                           HiTRes: bool = False,
#                           site_modifier: Dict[str, str] = {},
#                           height: Optional[str] = None,
#                           emissions_name: Optional[str] = None,
#                           fp_directory: Optional[Union[Path, str]] = None,
#                           flux_directory: Optional[Union[Path, str]] = None,
#                           bc_directory: Optional[Union[Path, str]] = None,
#                           resample_to_data: bool = False,
#                           species_footprint: Optional[str] = None,
#                           chunks: bool = False,
#                           verbose: bool = True,
#                           ) -> Any:
#     """
#     This will be a wrapper for footprints_data_merge function from acrg_name.name file written
#     """

#     # Write this a wrapper for footprints_data_merge function from acrg_name.name file

#     print("The footprint_data_merge() wrapper function will be deprecated.")
#     print("Please use the ModelScenario class to set up your data")
#     print("Then call the model.footprints_data_merge() method e.g.")
#     print(" model = ModelScenario(site, species, inlet, network, domain, ...)")
#     print(" combined_data = model.footprints_data_merge()")

#     # resample_to_data --> resample_to
#     # HiTRes --> linked to species as co2
#     # directories --> could link to adding new data to object store?
#     # height --> inlet
#     # species_footprint --> links to overall species?
#     # site_modifier --> link to footprint name - different site name for footprint?


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
