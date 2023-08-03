"""
Module for defining the ObjectStoreConnection class
and its children, which facilitate adding data to the
object store.

ObjectStoreConnection objects expose methods to:
- compare a file hash to existing hashes
- add a (metadata, data) pair to the object store
- search the object store

The classes in this module don't do any data or metadata
formatting.

Functions:
- `get_object_store_connection` returns an object store connection
   for a given data type to a given bucket.
"""
from __future__ import annotations
import logging
from pathlib import Path
from types import TracebackType
from typing import Any, Optional, Union

import tinydb
import xarray

import openghg.objectstore as objectstore
import openghg.store as store
from openghg.types import DatasourceLookupError
import openghg.util as util


logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class ObjectStoreConnection:
    """Represents a connection to an object store.

    Public attributes:
        required_keys: keys that must be present for Datasource lookup
                       and creation
        optional_keys: keys that will be used for Datasource lookup and
                       creation if they are present in the metadata passed
                       to `datasource_lookup`
        data_type: used to register subclasses
    """

    _registry: dict[str, Any] = {}  # subclass registry  # TODO replace Any with proper typing

    # variables that should be redefined in subclasses
    _root = "root"
    _uuid = "root_uuid"
    required_keys: tuple = ()
    optional_keys: tuple = ()
    data_type: str = ""

    def __init__(self, bucket: str) -> None:
        """Create object store connection object.

        Args:
            bucket: (path to?) the object store to connect to.
        """
        self._bucket = bucket
        self._key = f"{self._root}/uuid/{self._uuid}"
        self._metakey = f"{self._root}/uuid/{self._uuid}/metastore"
        self._metastore = store.load_metastore(self._bucket, self._metakey)

        # internal metadata stored at bucket/key._data
        self._creation_datatime = str(util.timestamp_now())
        self._datasource_uuids: dict[str, str] = {}  # file names keyed by datasource uuid
        self._file_hashes: dict[str, str] = {}  # hashes of previously uploaded files
        self._retrieved_hashes: dict[str, dict] = {}  # hashes of prev. uploaded files from other platforms
        self._stored = False  # TODO what is this?
        # check for existing internal metadata, and update if found
        if objectstore.exists(bucket=self._bucket, key=self._key):
            result = objectstore.get_object_from_json(self._bucket, key=self._key)
            self.__dict__.update(result)  # TODO do we want to overwrite bucket, key, metakey?

    @classmethod
    def __init_subclass__(cls) -> None:
        """Register subclasses by data_type."""
        ObjectStoreConnection._registry[cls.data_type] = cls

    def __repr__(self) -> str:
        return f"""ObjectStoreConnection to {self._bucket}/{self._metakey}
        Data type: {self.data_type}
        Required keys: {self.required_keys!r}
        Optional keys: {self.optional_keys!r}
        """

    # context manager
    def __enter__(self) -> ObjectStoreConnection:
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if exc_type is not None:
            logger.error(msg=f"{exc_type}: {exc_value}")
            logger.error(msg=f"Traceback:\n{traceback}")
        else:
            self.close()

    def close(self) -> None:
        """Close object store connection.

        This closes the metastore and writes internal metadata.

        If an ObjectStoreConnection is used without a context manager
        ("with" statement), then it must be closed manually.
        """
        self._metastore.close()
        self._write_internal_metadata()

    def _write_internal_metadata(self) -> None:
        """Write internal metadata to bucket/key._data"""
        do_not_store = ["_metastore"]
        internal_metadata = {k: v for k, v in self.__dict__.items() if k not in do_not_store}
        objectstore.set_object_from_json(bucket=self._bucket, key=self._key, data=internal_metadata)

    #  methods for interacting with object store
    def search(self, search_terms: dict[str, Any]) -> list[dict[str, Any]]:
        """Search the object store via the connection.

        Args:
            search_terms: list of keys and values to search for.

        Returns: A (possibly empty) list of results that contain the search terms.
        """
        query = tinydb.Query()
        search_results = self._metastore.search(query.fragment(search_terms))
        if search_results:
            return [dict(doc) for doc in search_results]
        else:
            return []

    def datasource_lookup(self, metadata: dict[str, Any]) -> Optional[str]:
        """Search metastore for a Datasource uuid using the given metadata.

        To add data the the object store, we must determine if a Datasource already
        exists for this data.

        Args:
            metadata: dictionary of metadata from data to be added to object store.

        Returns:
            uuid of the Datasource corresponding to the metadata, or None if a
        datasource is not found.

        Raises:
            DatasourceLookupError if multiple Datasources are found for the given metadata.
        """
        required_metadata = {
            k.lower(): v for k, v in metadata.items() if k in self.required_keys
        }  # NOTE: keys stored as lowercase
        if len(required_metadata) < len(self.required_keys):
            raise ValueError(
                f"The given metadata does not contain enough information. The required keys are: {self.required_keys}"
            )

        for key in self.optional_keys:
            if (val := metadata.get(key, None)) is not None:
                required_metadata[key] = val

        required_metadata = util.to_lowercase(required_metadata, skip_keys=["object_store"])
        print(required_metadata)

        results = self.search(required_metadata)
        if not results:
            return None
        elif len(results) > 1:
            raise DatasourceLookupError("More than one Datasource found for metadata.")
        else:
            return str(results[0]["uuid"])  # str to appease mypy

    def add_to_store(self, metadata: dict[str, Any], data: xarray.Dataset, skip_keys: Optional[list[str]] = ["object_store"]) -> dict[str, Union[str, bool]]:
        """Add (metadata, data) pair to a Datasource in the object store.

        If no existing Datasource is found for the given metadata, then a new Datasource is created,
        and the new Datasource uuid is stored in the internal metadata.

        Args:
            metadata: metadata for data to be stored
            data: data to be stored

        Returns:
           Dictionary with uuid of Datasource and a Boolean that is true if the
        Datasource is new.
        """
        from openghg.store.base import Datasource
        # TODO add overwrite? (currently not used in Datasource.add_data)

        metadata = {k.lower(): v for k, v in metadata.items()}  # metastore keys are lower case
        metadata = util.to_lowercase(metadata, skip_keys=skip_keys)

        lookup_results = self.datasource_lookup(metadata)
        new_datasource = True if lookup_results is None else False

        if new_datasource:
            datasource = Datasource()
            uuid = datasource.uuid()
            metadata["uuid"] = uuid
            metadata["object_store"] = self._bucket  # TODO is Gareth removing this?

            # record datasource in internal metadata
            key = "_".join(str(metadata[k]) for k in self.required_keys)
            self._datasource_uuids[uuid] = key
        else:
            uuid = str(lookup_results)  # we know lookup_results is not none
            datasource = Datasource.load(bucket=self._bucket, uuid=uuid)

        datasource.add_data(metadata=metadata, data=data, data_type=self.data_type, skip_keys=skip_keys)
        datasource.save(bucket=self._bucket)
        if new_datasource:
            self._metastore.insert(metadata)
        else:
            # Since the new data we're adding might have more metadata than was
            # in the existing Datasource, we update to datasource.metadata(),
            # which combines the existing metadata in the Datasource with
            # any newly added metadata.
            self._metastore.update(datasource.metadata(), tinydb.where("uuid") == datasource.uuid())
        return {"uuid": datasource.uuid(), "new": new_datasource}  # TODO change new back to new_datasource

    def file_hash_already_seen(self, file_hash: str) -> bool:
        """Return true if file hash has been saved.

        If the file hash was saved, then this file was already uploaded.

        Args:
           file_hash: hash of file. For instance, from openghg.util.hash_file.

        Returns:
            True if file hash is found in internal metadata for this data type,
            False if file hash is not found.
        """
        return file_hash in self._file_hashes.keys()

    def save_file_hash(self, file_hash: str, file_path: Path) -> None:
        """Add {file_hash: file_name} to internal metadata."""
        self._file_hashes[file_hash] = file_path.name

    # TODO should we have functions for deleting items from the object store?
    # BaseStore will let you clear datasources from the internal metadata,
    # but not the data itself.
    # Datasource has a delete_data method, although it's not obvious (to me) how
    # it works


def get_object_store_connection(data_type: str, bucket: str) -> Any:  # TODO: fix typing
    """Get an object store connection for a given data type.

    Args:
        data_type: type of data to read/store.
        bucket: object store to connect to.

    Returns:
        Connection to object store of given data type.
    """
    try:
        conn = ObjectStoreConnection._registry[data_type](bucket)
    except KeyError:
        raise ValueError(f"No ObjectStoreConnection subclass found for data type {data_type}.")
    else:
        return conn


class SurfaceConnection(ObjectStoreConnection):
    _root = "ObsSurface"
    _uuid = "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"
    required_keys = (
        "species",
        "site",
        "sampling_period",
        "station_long_name",
        "inlet",
        "instrument",
        "network",
        "source_format",
        "data_source",
        "icos_data_level",
        "data_type",
    )
    data_type = "surface"


class ColumnConnection(ObjectStoreConnection):
    _root = "ObsColumn"
    _uuid = "5c567168-0287-11ed-9d0f-e77f5194a415"
    required_keys = ("satellite", "selection", "domain", "site", "species", "network")
    data_type = "column"


class EmissionsConnection(ObjectStoreConnection):
    _root = "Emissions"
    _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"
    required_keys = ("species", "source", "domain")
    optional_keys = ("database", "database_version", "model")
    data_type = "emissions"


class FootprintsConnection(ObjectStoreConnection):
    _root = "Footprints"
    _uuid = "62db5bdf-c88d-4e56-97f4-40336d37f18c"
    required_keys = (
        "site",
        "model",
        "inlet",
        "domain",
    )  # TODO add high_time_resolution, etc. when 0.6.1 is out
    data_type = "footprints"


class BoundaryConditionsConnection(ObjectStoreConnection):
    _root = "BoundaryConditions"
    _uuid = "4e787366-be91-4fc5-ad1b-4adcb213d478"
    required_keys = ("species", "bc_input", "domain")
    data_type = "boundary_conditions"


class EulerianModelConnection(ObjectStoreConnection):
    _root = "EulerianModel"
    _uuid = "63ff2365-3ba2-452a-a53d-110140805d06"
    required_keys = ("model", "species", "date")
    data_type = "eulerian_model"
