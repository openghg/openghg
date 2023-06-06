import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Literal, Optional, Tuple, Union

import numpy as np
from numpy import ndarray
from openghg.store import DataSchema
from openghg.store.base import BaseStore
from xarray import DataArray, Dataset
import warnings

__all__ = ["Emissions"]


logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


ArrayType = Optional[Union[ndarray, DataArray]]


class Emissions(BaseStore):
    """This class is used to process emissions / flux data"""

    _root = "Emissions"
    _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    @staticmethod
    def read_data(binary_data: bytes, metadata: Dict, file_metadata: Dict) -> Optional[Dict]:
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
        database: Optional[str] = None,
        database_version: Optional[str] = None,
        model: Optional[str] = None,
        source_format: str = "openghg",
        high_time_resolution: Optional[bool] = False,
        period: Optional[Union[str, tuple]] = None,
        chunks: Union[int, Dict, Literal["auto"], None] = None,
        continuous: bool = True,
        if_exists: Optional[str] = None,
        save_current: Optional[bool] = None,
        overwrite: bool = False,
        force: bool = False,
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
            if_exists: What to do if existing data is present.
                - None - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - just include new data and ignore previous
                - "replace" - replace and insert new data into current timeseries
            save_current: Whether to save data in current form and create a new version.
                If None, this will depend on if_exists input (None -> True), (other -> False)
            overwrite: Deprecated. This will use options for if_exists="new" and save_current=True.
            force: Force adding of data even if this is identical to data stored.
        Returns:
            dict: Dictionary of datasource UUIDs data assigned to
        """
        from openghg.store import assign_data, datasource_lookup, load_metastore, update_metadata
        from openghg.types import EmissionsTypes
        from openghg.util import (
            clean_string,
            hash_file,
            load_emissions_parser,
            check_if_need_new_version,
        )

        species = clean_string(species)
        source = clean_string(source)
        domain = clean_string(domain)

        if overwrite and if_exists is None:
            logger.warning("Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                           "See documentation for details of these inputs and options.")
            if_exists = "new"

        # Making sure data can be force overwritten if force keyword is included.
        if force and if_exists is None:
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

        filepath = Path(filepath)

        try:
            source_format = EmissionsTypes[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Load the data retrieve object
        parser_fn = load_emissions_parser(source_format=source_format)

        em_store = Emissions.load()

        # Load in the metadata store
        metastore = load_metastore(key=em_store._metakey)

        file_hash = hash_file(filepath=filepath)
        if file_hash in em_store._file_hashes and not force:
            warnings.warn(
                f"This file has been uploaded previously with the filename : {em_store._file_hashes[file_hash]} - skipping.\n"
                "If necessary, use force=True to bypass this to add this data."
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

        optional_keywords = {"database": database,
                             "database_version": database_version,
                             "model": model}

        param.update(optional_keywords)

        emissions_data = parser_fn(**param)

        # Checking against expected format for Emissions
        for split_data in emissions_data.values():
            em_data = split_data["data"]
            Emissions.validate_data(em_data)

        min_required = ["species", "source", "domain"]
        for key, value in optional_keywords.items():
            if value is not None:
                min_required.append(key)

        required = tuple(min_required)
        lookup_results = datasource_lookup(metastore=metastore, data=emissions_data, required_keys=required)

        data_type = "emissions"
        datasource_uuids = assign_data(
            data_dict=emissions_data,
            lookup_results=lookup_results,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
        )

        update_keys = ["start_date", "end_date", "latest_version"]
        emissions_data = update_metadata(data_dict=emissions_data, uuid_dict=datasource_uuids, update_keys=update_keys)

        em_store.add_datasources(uuids=datasource_uuids, data=emissions_data, metastore=metastore, update_keys=update_keys)

        # Record the file hash in case we see this file again
        em_store._file_hashes[file_hash] = filepath.name

        em_store.save()
        metastore.close()

        return datasource_uuids

    @staticmethod
    def transform_data(
        datapath: Union[str, Path],
        database: str,
        if_exists: Optional[str] = None,
        save_current: Optional[bool] = None,
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
            if_exists: What to do if existing data is present.
                - None - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - just include new data and ignore previous
                - "replace" - replace and insert new data into current timeseries
            save_current: Whether to save data in current form and create a new version.
                If None, this will depend on if_exists input (None -> True), (other -> False)
            overwrite: Deprecated. This will use options for if_exists="new" and save_current=True.
            **kwargs: Inputs for underlying parser function for the database.
                Necessary inputs will depend on the database being parsed.

        TODO: Could allow Callable[..., Dataset] type for a pre-defined function be passed
        """
        import inspect

        from openghg.store import assign_data, datasource_lookup, load_metastore
        from openghg.types import EmissionsDatabases
        from openghg.util import load_emissions_database_parser, check_if_need_new_version

        if overwrite and if_exists is None:
            logger.warning("Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                           "See documentation for details of these inputs and options.")
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

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

        # TODO: Update this to find a way to include additional kwargs as required.
        # e.g. for EDGAR would also want to look up by database and database_version ...
        # May need to look at metadata passed back from parser_fn
        required = ("species", "source", "domain")
        lookup_results = datasource_lookup(metastore=metastore, data=emissions_data, required_keys=required)

        data_type = "emissions"
        overwrite = False
        datasource_uuids = assign_data(
            data_dict=emissions_data,
            lookup_results=lookup_results,
            if_exists=if_exists,
            new_version=new_version,
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
