from __future__ import annotations
import logging
from typing import Optional, Any
import numpy as np
from numpy import ndarray

from openghg.store import DataSchema
from openghg.store.base import BaseStore
from xarray import DataArray

ArrayType = Optional[ndarray | DataArray]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class ObsColumn(BaseStore):
    """This class is used to process emissions / flux data"""

    _data_type = "column"
    _root = "ObsColumn"
    _uuid = "5c567168-0287-11ed-9d0f-e77f5194a415"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    # TODO: Add in transform method for gosat and tropomi raw data files
    #  - Included emissions version as starting point to be updated.
    # @staticmethod
    # def transform_data(
    #     datapath: Union[str, Path],
    #     database: str,
    #     overwrite: bool = False,
    #     **kwargs: Dict,
    # ) -> Dict:
    #     """
    #     Read and transform an emissions database. This will find the appropriate
    #     parser function to use for the database specified. The necessary inputs
    #     are determined by which database ie being used.

    #     The underlying parser functions will be of the form:
    #         - openghg.transform.emissions.parse_{database.lower()}
    #             - e.g. openghg.transform.emissions.parse_edgar()

    #     Args:
    #         datapath: Path to local copy of database archive (for now)
    #         database: Name of database
    #         overwrite: Should this data overwrite currently stored data
    #             which matches.
    #         **kwargs: Inputs for underlying parser function for the database.
    #             Necessary inputs will depend on the database being parsed.

    #     TODO: Could allow Callable[..., Dataset] type for a pre-defined function be passed
    #     """
    #     import inspect
    #     from openghg.store import assign_data, load_metastore, datasource_lookup
    #     from openghg.types import EmissionsDatabases
    #     from openghg.util import load_emissions_database_parser

    #     datapath = Path(datapath)

    #     try:
    #         data_type = EmissionsDatabases[database.upper()].value
    #     except KeyError:
    #         raise ValueError(f"Unable to transform '{database}' selected.")

    #     # Load the data retrieve object
    #     parser_fn = load_emissions_database_parser(database=database)

    #     em_store = Emissions.load()

    #     # Load in the metadata store
    #     metastore = load_metastore(key=em_store._metakey)

    #     # Find all parameters that can be accepted by parse function
    #     all_param = list(inspect.signature(parser_fn).parameters.keys())

    #     # Define parameters to pass to the parser function from kwargs
    #     param: Dict[Any, Any] = {key: value for key, value in kwargs.items() if key in all_param}
    #     param["datapath"] = datapath  # Add datapath explicitly (for now)

    #     emissions_data = parser_fn(**param)

    #     # Checking against expected format for Emissions
    #     for split_data in emissions_data.values():
    #         em_data = split_data["data"]
    #         Emissions.validate_data(em_data)

    #     required = ("species", "source", "domain", "date")
    #     lookup_results = datasource_lookup(metastore=metastore, data=emissions_data, required_keys=required)

    #     data_type = "emissions"
    #     overwrite = False
    #     datasource_uuids = assign_data(
    #         data_dict=emissions_data,
    #         lookup_results=lookup_results,
    #         overwrite=overwrite,
    #         data_type=data_type,
    #     )

    #     em_store.add_datasources(uuids=datasource_uuids, data=emissions_data, metastore=metastore)

    #     em_store.save()
    #     metastore.close()

    #     return datasource_uuids

    def format_inputs(self, **kwargs: Any) -> tuple[dict, dict]:
        """
        Apply appropriate formatting for expected inputs for ObsColumn. Expected
        inputs will typically be defined within the openghg.standardse.standardise_column()
        function.

        Args:
            kwargs: Set of keyword arguments. Selected keywords will be
                appropriately formatted.
        Returns:
            (dict, dict): Formatted parameters and any additional parameters
                for this data type.

        TODO: Decide if we can phase out additional_metadata or if this could be
            added to params.
        """

        from openghg.util import (
            clean_string,
            format_platform,
            synonyms,
            check_and_set_null_variable,
        )

        params = kwargs.copy()

        # Apply clean string formatting
        params["species"] = clean_string(params.get("species"))
        params["platform"] = clean_string(params.get("platform"))
        params["site"] = clean_string(params.get("site"))
        params["satellite"] = clean_string(params.get("satellite"))
        params["network"] = clean_string(params.get("network"))
        params["instrument"] = clean_string(params.get("instrument"))
        params["domain"] = clean_string(params.get("domain"))
        params["obs_region"] = clean_string(params.get("obs_region"))
        params["pressure_weights_method"] = clean_string(params.get("pressure_weights_method"))

        # Checks input combinations are correct
        site = params.get("site")
        satellite = params.get("satellite")

        if site is None and satellite is None:
            msg = "Value for 'site' or 'satellite' must be specified"
            logger.exception(msg)
            raise ValueError(msg)
        elif site is not None and satellite is not None:
            msg = "Only one of 'site' or 'satellite' should be specified"
            logger.exception(msg)
            raise ValueError(msg)

        domain = params.get("domain")
        obs_region = params.get("obs_region")

        if domain is not None and obs_region is not None:
            err_msg = f"Only one of 'domain' : {domain} or 'obs_region': {obs_region} should be specified"
            logger.exception(err_msg)
            raise ValueError(err_msg)
        elif domain is not None and obs_region is None:
            params["obs_region"] = domain
            logger.info(f"Updated 'obs_region' to match 'domain': {domain}")

        # Apply individual formatting as appropriate
        # - apply synonyms substitution for species
        species = params.get("species")
        if species is not None:
            params["species"] = synonyms(species)

        # - format platform
        params["platform"] = format_platform(params.get("platform"))

        # Ensure we have a clear missing value (not_set) where needed (required keys)
        params["domain"] = check_and_set_null_variable(params.get("domain"))

        # Specify any additional metadata to be added
        additional_metadata: dict = {}

        return params, additional_metadata

    @staticmethod
    def schema(species: str, vertical_name: str = "lev") -> DataSchema:  # type: ignore[override]
        """
        Define schema for a column Dataset.

        Includes column data for each time point:
            - standardised species and column name as "x{species}" (e.g. "xch4")
            - averaging kernel variable as "x{species_name}_averaging_kernel"
            - profile apriori variable as "{species_name}_profile_apriori"
            - expected "time" dimension
            - expected vertical dimension (defined by input)

        Expected data types for all variables and coordinates also included.

        Args:
            species: Species name which will be used to construct appropriate
                variable names e.g. "ch4" will create "xch4"
            vertical_name: Name of the vertical dimension for averaging kernel
                and apriori.
                Default = "lev"
        Returns:
            DataSchema : Contains schema for ObsColumn.

        TODO: Expand valid list of vertical names as needed (e.g. "lev", "height") and
            check vertical_name inputs against valid list of options.
        """
        from openghg.standardise.meta import define_species_label

        data_vars: dict[str, tuple[str, ...]] = {}
        dtypes: dict[str, Any] = {"time": np.datetime64}

        species_name = define_species_label(species)[0]

        column_name = f"x{species_name}"
        averaging_kernal_name = f"x{species_name}_averaging_kernel"
        profile_apriori_name = f"{species_name}_profile_apriori"

        data_vars[column_name] = ("time",)
        data_vars[averaging_kernal_name] = ("time", vertical_name)
        data_vars[profile_apriori_name] = ("time", vertical_name)

        dtypes = {
            column_name: np.floating,
            averaging_kernal_name: np.floating,
            profile_apriori_name: np.floating,
        }

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format
