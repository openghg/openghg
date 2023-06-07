from __future__ import annotations
import logging
from pathlib import Path
from typing import Dict, Optional, Union

from numpy import ndarray

# from openghg.store import DataSchema
from openghg.store.base import BaseStore
from xarray import DataArray
from types import TracebackType

ArrayType = Optional[Union[ndarray, DataArray]]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class ObsColumn(BaseStore):
    """This class is used to process emissions / flux data"""

    _root = "ObsColumn"
    _uuid = "5c567168-0287-11ed-9d0f-e77f5194a415"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def __enter__(self) -> ObsColumn:
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.save()

    def read_file(
        self,
        filepath: Union[str, Path],
        satellite: Optional[str] = None,
        domain: Optional[str] = None,
        selection: Optional[str] = None,
        site: Optional[str] = None,
        species: Optional[str] = None,
        network: Optional[str] = None,
        instrument: Optional[str] = None,
        platform: str = "satellite",
        source_format: str = "openghg",
        overwrite: bool = False,
    ) -> Optional[Dict]:
        """Read column observation file

        Args:
            filepath: Path of observation file
            satellite: Name of satellite (if relevant)
            domain: For satellite only. If data has been selected on an area include the
                identifier name for domain covered. This can map to previously defined domains
                (see openghg_defs "domain_info.json" file) or a newly defined domain.
            selection: For satellite only, identifier for any data selection which has been
                performed on satellite data. This can be based on any form of filtering, binning etc.
                but should be unique compared to other selections made e.g. "land", "glint", "upperlimit".
                If not specified, domain will be used.
            site : Site code/name (if relevant). Can include satellite OR site.
            species: Species name or synonym e.g. "ch4"
            instrument: Instrument name e.g. "TANSO-FTS"
            network: Name of in-situ or satellite network e.g. "TCCON", "GOSAT"
            platform: Type of platform. Should be one of:
                - "satellite"
                - "site"
            source_format : Type of data being input e.g. openghg (internal format)
            overwrite: Should this data overwrite currently stored data.
        Returns:
            dict: Dictionary of datasource UUIDs data assigned to
        """
        from openghg.types import ColumnTypes
        from openghg.util import clean_string, hash_file, load_column_parser

        # TODO: Evaluate which inputs need cleaning (if any)
        satellite = clean_string(satellite)
        site = clean_string(site)
        species = clean_string(species)
        domain = clean_string(domain)
        network = clean_string(network)
        instrument = clean_string(instrument)
        platform = clean_string(platform)

        filepath = Path(filepath)

        try:
            source_format = ColumnTypes[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Load the data retrieve object
        parser_fn = load_column_parser(source_format=source_format)

        # Load in the metadata store

        file_hash = hash_file(filepath=filepath)
        if file_hash in self._file_hashes and not overwrite:
            logger.warning(
                "This file has been uploaded previously with the filename : "
                f"{self._file_hashes[file_hash]} - skipping."
            )
            return None

        # Define parameters to pass to the parser function
        param = {
            "data_filepath": filepath,
            "satellite": satellite,
            "domain": domain,
            "selection": selection,
            "site": site,
            "species": species,
            "network": network,
            "instrument": instrument,
            "platform": platform,
        }

        obs_data = parser_fn(**param)

        # TODO: Add in schema and checks for ObsColumn
        # # Checking against expected format for ObsColumn
        # for split_data in obs_data.values():
        #     col_data = split_data["data"]
        #     ObsColumn.validate_data(col_data)

        # TODO: Do we need to do include a split here of some kind, since
        # this could be "site" or "satellite" keys.
        # platform = list(obs_data.keys())[0]["metadata"]["platform"]

        required = ("satellite", "selection", "domain", "site", "species", "network")

        data_type = "column"
        datasource_uuids = self.assign_data(
            data=obs_data, overwrite=overwrite, data_type=data_type, required_keys=required, min_keys=3
        )

        # Record the file hash in case we see this file again
        self._file_hashes[file_hash] = filepath.name

        return datasource_uuids

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

    # TODO: Define and add schema methods for ObsColumn
    # @staticmethod
    # def schema(species: str, platform: str = "satellite") -> DataSchema:
    #     """
    #     Define schema for emissions Dataset.

    #     Includes column data for each time point:
    #         - standardised species and column name (e.g. "xch4")
    #         - expected dimensions: ("time")

    #     Expected data types for all variables and coordinates also included.

    #     Returns:
    #         DataSchema : Contains schema for Emissions.
    #     """
    #     data_vars: Dict[str, Tuple[str, ...]] \
    #         = {"flux": ("time", "lat", "lon")}
    #     dtypes = {"lat": np.floating,
    #               "lon": np.floating,
    #               "time": np.datetime64,
    #               "flux": np.floating}

    #     data_format = DataSchema(data_vars=data_vars,
    #                              dtypes=dtypes)

    #     return data_format

    # @staticmethod
    # def validate_data(data: Dataset) -> None:
    #     """
    #     Validate input data against Emissions schema - definition from
    #     Emissions.schema() method.

    #     Args:
    #         data : xarray Dataset in expected format

    #     Returns:
    #         None

    #         Raises a ValueError with details if the input data does not adhere
    #         to the Emissions schema.
    #     """
    #     data_schema = Emissions.schema()
    #     data_schema.validate_data(data)
