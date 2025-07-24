from __future__ import annotations
import logging
from typing import Optional
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

    def format_inputs(self, **kwargs) -> tuple[dict, dict]:
        """ """

        from openghg.util import (
            clean_string,
            format_platform,
            synonyms,
            not_set_metadata_values,
        )

        #     Args:
        #         filepath: Path of observation file
        #         species: Species name or synonym e.g. "ch4"
        #         platform: Type of platform. Should be one of:
        #             - "satellite"
        #             - "site"
        #         satellite: Name of satellite (if relevant). Should include satellite OR site.
        #         domain: For satellite only. If data has been selected on an area include the
        #             identifier name for domain covered. This can map to previously defined domains
        #             (see openghg_defs "domain_info.json" file) or a newly defined domain.
        #         selection: For satellite only, identifier for any data selection which has been
        #             performed on satellite data. This can be based on any form of filtering, binning etc.
        #             but should be unique compared to other selections made e.g. "land", "glint", "upperlimit".
        #             If not specified, domain will be used.
        #         site : Site code/name (if relevant). Should include satellite OR site.
        #         instrument: Instrument name e.g. "TANSO-FTS"
        #         network: Name of in-situ or satellite network e.g. "TCCON", "GOSAT"

        params = kwargs.copy()

        # TODO: Evaluate which inputs need cleaning (if any)
        species = clean_string(params["species"])
        params["species"] = synonyms(species)

        platform = format_platform(params["platform"])
        params["platform"] = clean_string(platform)

        site = params.get("site")
        satellite = params.get("satellite")

        if site is None and satellite is None:
            raise ValueError("Value for 'site' or 'satellite' must be specified")
        elif site is not None and satellite is not None:
            raise ValueError("Only one of 'site' or 'satellite' should be specified")

        params["site"] = clean_string(site)
        params["satellite"] = clean_string(satellite)
        params["network"] = clean_string(params.get("network"))
        params["instrument"] = clean_string(params.get("instrument"))

        domain = clean_string(params.get("domain"))
        obs_region = clean_string(params.get("obs_region"))

        if domain is not None and obs_region is not None:
            err_msg = f"Only one of 'domain' : {domain} or 'obs_region': {obs_region} should be specified"
            logger.exception(err_msg)
            raise ValueError(err_msg)
        elif domain is not None and obs_region is None:
            obs_region = domain
            logger.info(f"Updated 'obs_region' to match 'domain': {domain}")
        elif obs_region is not None and domain is None:
            not_set_value = not_set_metadata_values()[0]
            domain = not_set_value  # Do we want this to be "NOT_SET" or just not included?
            logging.info(f"Updated value of 'domain': {domain}")

        params["domain"] = domain
        params["obs_region"] = obs_region

        # Specify any additional metadata to be added
        additional_metadata = {}

        return params, additional_metadata

    # TODO: Check and update schema methods for ObsColumn to make sure this works for site-column
    @staticmethod
    def schema(species: str) -> DataSchema:
        """
        Define schema for emissions Dataset.

        Includes column data for each time point:
            - standardised species and column name (e.g. "xch4")
            - expected dimensions: ("time")

        Expected data types for all variables and coordinates also included.

        Returns:
            DataSchema : Contains schema for Emissions.
        """
        from openghg.standardise.meta import define_species_label

        name = define_species_label(species)[0]
        column_name = f"x{name}"

        data_vars: dict[str, tuple[str, ...]] = {column_name: ("time",)}
        dtypes = {column_name: np.floating, "time": np.datetime64}

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format
