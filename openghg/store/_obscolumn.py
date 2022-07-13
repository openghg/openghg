from pathlib import Path
from typing import Dict, Optional, Union, Tuple, Any
from xarray import Dataset, DataArray
import numpy as np
from numpy import ndarray

from openghg.store import DataSchema
from openghg.store.base import BaseStore
# from openghg.types import multiPathType

__all__ = ["ObsColumn"]


ArrayType = Optional[Union[ndarray, DataArray]]


class ObsColumn(BaseStore):
    """This class is used to process emissions / flux data"""

    _root = "ObsColumn"
    _uuid = "5c567168-0287-11ed-9d0f-e77f5194a415"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        instrument: Optional[str],
        species: Optional[str],
        domain: Optional[str],
        network: Optional[str],
        site: Optional[str],
        data_type: str = "openghg",
        measurement_type: Optional[str] = None,
        overwrite: bool = False,
    ) -> Dict:
        """Read column observation file

        Args:
            filepath: Path of observation file
            instrument: ***
            species: Species name
            network: ***
            domain: Domain (name of area) covered by column emissions
            site: ***
            data_type : Type of data being input e.g. openghg (internal format)
            measurement_type : Type of measurement e.g. satellite, surface
            overwrite: Should this data overwrite currently stored data.
        Returns:
            dict: Dictionary of datasource UUIDs data assigned to
        """
        from openghg.store import assign_data, load_metastore, datasource_lookup
        from openghg.types import ColumnTypes
        from openghg.util import (
            clean_string,
            hash_file,
            load_column_parser,
        )

        instrument = clean_string(instrument)
        species = clean_string(species)
        domain = clean_string(domain)
        network = clean_string(network)
        site = clean_string(site)

        filepath = Path(filepath)

        try:
            data_type = ColumnTypes[data_type.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {data_type} selected.")

        # Load the data retrieve object
        parser_fn = load_column_parser(data_type=data_type)

        obs_store = ObsColumn.load()

        # Load in the metadata store
        metastore = load_metastore(key=obs_store._metakey)

        file_hash = hash_file(filepath=filepath)
        if file_hash in obs_store._file_hashes and not overwrite:
            print(
                f"This file has been uploaded previously with the filename : {obs_store._file_hashes[file_hash]} - skipping."
            )

        # Define parameters to pass to the parser function
        param = {"data_filepath": filepath,
                 "instrument": instrument,
                 "species": species,
                 "domain": domain,
                 "network": network,
                 "site": site}

        obs_data = parser_fn(**param)

        # # Checking against expected format for ObsColumn
        # for split_data in obs_data.values():
        #     col_data = split_data["data"]
        #     ObsColumn.validate_data(col_data)

        required = ("instrument", "species", "domain")
        lookup_results = datasource_lookup(metastore=metastore, data=obs_data, required_keys=required)

        data_type = "timeseries"
        datasource_uuids = assign_data(
            data_dict=obs_data,
            lookup_results=lookup_results,
            overwrite=overwrite,
            data_type=data_type,
        )

        obs_store.add_datasources(uuids=datasource_uuids, data=obs_data, metastore=metastore)

        # Record the file hash in case we see this file again
        obs_store._file_hashes[file_hash] = filepath.name

        obs_store.save()
        metastore.close()

        return datasource_uuids

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

    # @staticmethod
    # def schema() -> DataSchema:
    #     """
    #     Define schema for emissions Dataset.

    #     Includes flux/emissions for each time and position:
    #         - "flux"
    #             - expected dimensions: ("time", "lat", "lon")

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

    # def lookup_uuid(self, instrument: str, species: str, domain: str, network: str) -> Union[str, bool]:
    #     """Perform a lookup for the UUID of a Datasource

    #     Args:
    #         species: Site code
    #         domain: Domain
    #         model: Model name
    #         height: Height
    #     Returns:
    #         str or dict: UUID or False if no entry
    #     """
    #     uuid = self._datasource_table[species][source][domain][date]

    #     return uuid if uuid else False

    # def set_uuid(self, species: str, source: str, domain: str, date: str, uuid: str) -> None:
    #     """Record a UUID of a Datasource in the datasource table

    #     Args:
    #         site: Site code
    #         domain: Domain
    #         model: Model name
    #         height: Height
    #         uuid: UUID of Datasource
    #     Returns:
    #         None
    #     """
    #     self._datasource_table[species][source][domain][date] = uuid
