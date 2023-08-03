from __future__ import annotations
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Literal, Optional, Tuple, Union
import logging
import numpy as np
from numpy import ndarray
from openghg.store import DataSchema
from openghg.store.base import BaseStore
from xarray import DataArray, Dataset
from types import TracebackType
import warnings
from openghg.store._connection import get_object_store_connection
from openghg.util import to_lowercase

ArrayType = Optional[Union[ndarray, DataArray]]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class Emissions(BaseStore):
    """This class is used to process emissions / flux data"""

    _root = "Emissions"
    _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def __enter__(self) -> Emissions:
        self._metastore.close()  # For now, close metastore before connection opened...
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_type is not None:
            logger.error(msg=f"{exc_type}, {exc_tb}")
        else:
            #self.save()
            pass

    def read_data(self, binary_data: bytes, metadata: Dict, file_metadata: Dict) -> Optional[Dict]:
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

            return self.read_file(filepath=filepath, **metadata)

    def read_file(
        self,
        filepath: Union[str, Path],
        species: str,
        source: str,
        domain: str,
        database: Optional[str] = None,
        database_version: Optional[str] = None,
        model: Optional[str] = None,
        source_format: str = "openghg",
        high_time_resolution: Optional[bool] = False,
        period: Optional[Union[str, tuple]] = None,
        chunks: Union[int, Dict, Literal["auto"], None] = None,
        continuous: bool = True,
        overwrite: bool = False,
    ) -> Optional[Dict]:
        """Read emissions file

        Args:
            filepath: Path of emissions file
            species: Species name
            domain: Emissions domain
            source: Emissions source
            database: Name of database source for this input (if relevant)
            database_version: Name of database version (if relevant)
            model: Model name (if relevant)
            source_format : Type of data being input e.g. openghg (internal format)
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
        from openghg.types import EmissionsTypes
        from openghg.util import clean_string, hash_file, load_emissions_parser

        optional_args = {"database": database,
                         "database_version": database_version,
                         "model": model,
                         "high_time_resolution": high_time_resolution,
                         "period": period,
                         }
        print("optional_args: ", optional_args)
        species = clean_string(species)
        source = clean_string(source)
        domain = clean_string(domain)

        filepath = Path(filepath)

        try:
            source_format = EmissionsTypes[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Load the data retrieve object
        parser_fn = load_emissions_parser(source_format=source_format)

        datasource_uuids = {}
        with get_object_store_connection("emissions", self._bucket) as conn:
            file_hash = hash_file(filepath=filepath)
            if conn.file_hash_already_seen(file_hash) and not overwrite:
                warnings.warn(
                    f"This file has been uploaded previously with the filename : {self._file_hashes[file_hash]} - skipping."
                )
                return None

            # Define parameters to pass to the parser function
            # TODO: Update this to match against inputs for parser function.
            param = {
                "filepath": filepath,
                "species": species,
                "domain": domain,
                "source": source,
                "high_time_resolution": high_time_resolution,
                "period": period,
                "continuous": continuous,
                "data_type": "emissions",
                "chunks": chunks,
            }
            optional_keywords = {k: to_lowercase(optional_args.get(k, None)) for k in conn.optional_keys}
            param.update(optional_keywords)
            emissions_data = parser_fn(**param)

            # Checking against expected format for Emissions
            for split_data in emissions_data.values():
                em_data = split_data["data"]
                Emissions.validate_data(em_data)

            for key, split_data in emissions_data.items():
                print("Passed to datasource_lookup:", split_data["metadata"].get("database_version", "database_version not found"))
                ds_uuid = conn.add_to_store(split_data["metadata"], split_data["data"])
                datasource_uuids[key] = ds_uuid

            # Record the file hash in case we see this file again
            conn.save_file_hash(file_hash, filepath)

        return datasource_uuids

    def transform_data(
        self,
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
        from openghg.types import EmissionsDatabases
        from openghg.util import load_emissions_database_parser

        datapath = Path(datapath)

        try:
            data_type = EmissionsDatabases[database.upper()].value
        except KeyError:
            raise ValueError(f"Unable to transform '{database}' selected.")

        # Load the data retrieve object
        parser_fn = load_emissions_database_parser(database=database)

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

        required_keys = ("species", "source", "domain")

        data_type = "emissions"
        overwrite = False
        datasource_uuids = self.assign_data(
            data=emissions_data, overwrite=overwrite, data_type=data_type, required_keys=required_keys
        )

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
