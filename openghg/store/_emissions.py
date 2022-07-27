from pathlib import Path
from typing import Dict, Optional, Union, Tuple, Any
from xarray import Dataset, DataArray
import numpy as np
from numpy import ndarray
from tempfile import TemporaryDirectory
from openghg.store import DataSchema
from openghg.store.base import BaseStore

__all__ = ["Emissions"]


ArrayType = Optional[Union[ndarray, DataArray]]


class Emissions(BaseStore):
    """This class is used to process emissions / flux data"""

    _root = "Emissions"
    _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    @staticmethod
    def read_data(binary_data: bytes, metadata: Dict, file_metadata: Dict) -> Dict:
        """Ready a footprint from binary data

        Args:
            binary_data: Footprint data
            metadata: Dictionary of metadata
            file_metadat: File metadata
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            try:
                filename = file_metadata["filename"]
            except KeyError:
                raise KeyError("We require a filename key for metadata read.")

            filepath = tmpdir_path.joinpath(filename)
            filepath.write_bytes(binary_data)

            return Emissions.read_file(filepath=filepath, **metadata)

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        species: str,
        source: str,
        domain: str,
        data_type: str = "openghg",
        date: Optional[str] = None,
        high_time_resolution: Optional[bool] = False,
        period: Optional[Union[str, tuple]] = None,
        continuous: bool = True,
        overwrite: bool = False,
    ) -> Dict:
        """Read emissions file

        Args:
            filepath: Path of emissions file
            species: Species name
            domain: Emissions domain
            source: Emissions source
            date : Date associated with emissions as a string
            data_type : Type of data being input e.g. openghg (internal format)
            high_time_resolution: If this is a high resolution file
            period: Period of measurements. Only needed if this can not be inferred from the time coords
                    If specified, should be one of:
                     - "yearly", "monthly"
                     - suitable pandas Offset Alias
                     - tuple of (value, unit) as would be passed to pandas.Timedelta function
            continuous: Whether time stamps have to be continuous.
            overwrite: Should this data overwrite currently stored data.
        Returns:
            dict: Dictionary of datasource UUIDs data assigned to
        """
        from openghg.store import assign_data, load_metastore, datasource_lookup
        from openghg.types import EmissionsTypes
        from openghg.util import (
            clean_string,
            hash_file,
            load_emissions_parser,
        )

        species = clean_string(species)
        source = clean_string(source)
        domain = clean_string(domain)
        date = clean_string(date)

        filepath = Path(filepath)

        try:
            data_type = EmissionsTypes[data_type.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {data_type} selected.")

        # Load the data retrieve object
        parser_fn = load_emissions_parser(data_type=data_type)

        em_store = Emissions.load()

        # Load in the metadata store
        metastore = load_metastore(key=em_store._metakey)

        file_hash = hash_file(filepath=filepath)
        if file_hash in em_store._file_hashes and not overwrite:
            print(
                f"This file has been uploaded previously with the filename : {em_store._file_hashes[file_hash]} - skipping."
            )

        # Define parameters to pass to the parser function
        # TODO: Update this to match against inputs for parser function.
        param = {
            "filepath": filepath,
            "species": species,
            "domain": domain,
            "source": source,
            "date": date,
            "high_time_resolution": high_time_resolution,
            "period": period,
            "continuous": continuous,
        }

        emissions_data = parser_fn(**param)

        # Checking against expected format for Emissions
        for split_data in emissions_data.values():
            em_data = split_data["data"]
            Emissions.validate_data(em_data)

        required = ("species", "source", "domain", "date")
        lookup_results = datasource_lookup(metastore=metastore, data=emissions_data, required_keys=required)

        data_type = "emissions"
        datasource_uuids = assign_data(
            data_dict=emissions_data,
            lookup_results=lookup_results,
            overwrite=overwrite,
            data_type=data_type,
        )

        em_store.add_datasources(uuids=datasource_uuids, data=emissions_data, metastore=metastore)

        # Record the file hash in case we see this file again
        em_store._file_hashes[file_hash] = filepath.name

        em_store.save()
        metastore.close()

        return datasource_uuids

    @staticmethod
    def transform_data(
        datapath: Union[str, Path],
        database: str,
        overwrite: bool = False,
        **kwargs: Dict,
    ) -> Dict:
        """
        Read and transform an emissions database. This will find the appropriate
        parser function to use for the database specified. The necessary inputs
        are determined by which database ie being used.

        The underlying parser functions will be of the form:
            - openghg.transform.emissions.parse_{database.lower()}
                - e.g. openghg.transform.emissions.parse_edgar()

        Args:
            datapath: Path to local copy of database archive (for now)
            database: Name of database
            overwrite: Should this data overwrite currently stored data
                which matches.
            **kwargs: Inputs for underlying parser function for the database.
                Necessary inputs will depend on the database being parsed.

        TODO: Could allow Callable[..., Dataset] type for a pre-defined function be passed
        """
        import inspect
        from openghg.store import assign_data, load_metastore, datasource_lookup
        from openghg.types import EmissionsDatabases
        from openghg.util import load_emissions_database_parser

        datapath = Path(datapath)

        try:
            data_type = EmissionsDatabases[database.upper()].value
        except KeyError:
            raise ValueError(f"Unable to transform '{database}' selected.")

        # Load the data retrieve object
        parser_fn = load_emissions_database_parser(database=database)

        em_store = Emissions.load()

        # Load in the metadata store
        metastore = load_metastore(key=em_store._metakey)

        # Find all parameters that can be accepted by parse function
        all_param = list(inspect.signature(parser_fn).parameters.keys())

        # Define parameters to pass to the parser function from kwargs
        param: Dict[Any, Any] = {key: value for key, value in kwargs.items() if key in all_param}
        param["datapath"] = datapath  # Add datapath explicitly (for now)

        emissions_data = parser_fn(**param)

        # Checking against expected format for Emissions
        for split_data in emissions_data.values():
            em_data = split_data["data"]
            Emissions.validate_data(em_data)

        required = ("species", "source", "domain", "date")
        lookup_results = datasource_lookup(metastore=metastore, data=emissions_data, required_keys=required)

        data_type = "emissions"
        overwrite = False
        datasource_uuids = assign_data(
            data_dict=emissions_data,
            lookup_results=lookup_results,
            overwrite=overwrite,
            data_type=data_type,
        )

        em_store.add_datasources(uuids=datasource_uuids, data=emissions_data, metastore=metastore)

        em_store.save()
        metastore.close()

        return datasource_uuids

    @staticmethod
    def schema() -> DataSchema:
        """
        Define schema for emissions Dataset.

        Includes flux/emissions for each time and position:
            - "flux"
                - expected dimensions: ("time", "lat", "lon")

        Expected data types for all variables and coordinates also included.

        Returns:
            DataSchema : Contains schema for Emissions.
        """
        data_vars: Dict[str, Tuple[str, ...]] = {"flux": ("time", "lat", "lon")}
        dtypes = {"lat": np.floating, "lon": np.floating, "time": np.datetime64, "flux": np.floating}

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format

    @staticmethod
    def validate_data(data: Dataset) -> None:
        """
        Validate input data against Emissions schema - definition from
        Emissions.schema() method.

        Args:
            data : xarray Dataset in expected format

        Returns:
            None

            Raises a ValueError with details if the input data does not adhere
            to the Emissions schema.
        """
        data_schema = Emissions.schema()
        data_schema.validate_data(data)

    def lookup_uuid(self, species: str, source: str, domain: str, date: str) -> Union[str, bool]:
        """Perform a lookup for the UUID of a Datasource

        Args:
            species: Site code
            domain: Domain
            model: Model name
            height: Height
        Returns:
            str or dict: UUID or False if no entry
        """
        uuid = self._datasource_table[species][source][domain][date]

        return uuid if uuid else False

    def set_uuid(self, species: str, source: str, domain: str, date: str, uuid: str) -> None:
        """Record a UUID of a Datasource in the datasource table

        Args:
            site: Site code
            domain: Domain
            model: Model name
            height: Height
            uuid: UUID of Datasource
        Returns:
            None
        """
        self._datasource_table[species][source][domain][date] = uuid
