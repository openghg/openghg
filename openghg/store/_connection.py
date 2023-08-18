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
from functools import cache
import logging
from pathlib import Path
from types import TracebackType
from typing import Any, Optional, Union, cast

import tinydb
import xarray

import openghg.objectstore as objectstore
import openghg.store as store
from openghg.types import DatasourceLookupError
import openghg.util as util


logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class InternalMetadata:
    """Manages metadata about the object store itself,
    and stores transaction data.

    It tracks:
        - UUIDs of datasources created
        - hashes of files stored, together with the filename of the hashed file
        - hashes of data retrieved from remove sources
        - the creation time of the associated bucket

    """
    def __init__(self, bucket: str, key: str) -> None:
        """Create InternalMetadata object.

        Args:
            bucket: bucket (i.e. object store) where internal metadata stored.
            key: location of internal metadata within the bucket.
        """
        self._bucket = bucket
        self._key = key

        self._creation_datatime = str(util.timestamp_now())
        self.datasource_uuids: dict[str, str] = {}  # file names keyed by datasource uuid # TODO "keys" keyed by uuid?
        self.file_hashes: dict[str, str] = {}  # hashes of previously uploaded files
        self.retrieved_hashes: dict[str, dict] = {}  # hashes of prev. uploaded files from other platforms

        # check for existing internal metadata, and update if found
        if objectstore.exists(bucket=self._bucket, key=self._key):
            result = objectstore.get_object_from_json(self._bucket, key=self._key)
            self.__dict__.update(result)

    def write(self) -> None:
        """Write internal metadata to <bucket>/<key>._data

        Returns:
            None
        """
        internal_metadata = {k: v for k, v in self.__dict__.items() if k not in ["_bucket", "_key"]}
        objectstore.set_object_from_json(bucket=self._bucket, key=self._key, data=internal_metadata)

    def datasources(self) -> list[str]:
        """Return list of stored Datasources UUIDs.

        Returns:
            list: List of Datasource UUIDs
        """
        return list(self.datasource_uuids.keys())


class DatasourcePolicy:
    """This class holds the set of keys used to define datasources.

    To define a datasource, we need a minimum number of keys from a set of required keys.

    By default, the minimum number of required keys is the number of required keys,
    but it may be less.

    Optional keys are used to determine a datasource, if those keys are available.
    """
    def __init__(self, required_keys: list[str], optional_keys: Optional[list[str]] = None, min_required_keys: Optional[int] = None) -> None:
        """Create DatasourcePolicy object.

        Args:
            required_keys: keys necessary to define a datasource.
            optional_keys: keys used to define a datasource, if they are available.
            min_required_keys: the minimum number of required keys that must be present to define a datasource.
        """
        self.required_keys = required_keys
        if optional_keys is None:
            self.optional_keys = []
        else:
            self.optional_keys = optional_keys
        if min_required_keys is None:
            self.min_required_keys = len(required_keys)
        else:
            self.min_required_keys = min_required_keys

    def get_required_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return subset of given metadata used for determining a datasource.

        Given a set of metadata, only some of the metadata is used to determine what
        datasource the data belongs to. Given a collection of metadata, this function
        extracts this "defining metadata".

        Args:
            metadata: dictionary of metadata associated to data that will be added to the
        object store.

        Returns:
           required metadata: subset of given metadata that is required for determining the
        associated datasource.

        Raises:
           ValueError if the number of required keys is less than the minimum number of required
        keys.
        """
        required_metadata = {
            k.lower(): v for k, v in metadata.items() if k in self.required_keys
        }  # NOTE: keys stored as lowercase

        if len(required_metadata) < self.min_required_keys:
            raise ValueError(
                f"""The given metadata does not contain enough information.
                The required keys are: {self.required_keys}.
                You provided: {required_metadata.keys()}.
                """
            )

        for key in self.optional_keys:
            if (val := metadata.get(key, None)) is not None:
                required_metadata[key] = val

        required_metadata = util.to_lowercase(required_metadata)

        return required_metadata


# TODO: read this in from a config file?
datasource_policy_json = {
    "surface": {
        "required_keys": [
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
            "data_type"
        ],
        "min_required_keys": 5,
    },
    "column": {
        "required_keys": ["satellite", "selection", "domain", "site", "species", "network"],
        "min_required_keys": 3,
    },
    "emissions": {
        "required_keys": ["species", "source", "domain"],
        "optional_keys": ["database", "database_version", "model"],
    },
    "footprints": {
        "required_keys": [
            "site",
            "model",
            "inlet",
            "domain",
            "high_time_resolution",
            "high_spatial_resolution",
            "short_lifetime",
        ],
    },
    "boundary_conditions": {
        "required_keys": ["species", "bc_input", "domain"],
    },
    "eulerian_model": {
        "required_keys": ["model", "species", "date"],
    }
}


@cache
def get_datasource_policies() -> dict[str, DatasourcePolicy]:
    """Return dictonary of DatasourcePolicy objects keyed by data type.

    TODO: find somewhere to store polices as a JSON, and use this function to read them
    in.
    """
    datasource_policies = {}
    for k, v in datasource_policy_json.items():
        datasource_policies[k] = DatasourcePolicy(**v)

    return datasource_policies


def get_required_metadata(data_type: str, metadata: dict[str, Any]) -> dict[str, Any]:
    """Return subset of given metadata used for determining a datasource.

        Given a set of metadata, only some of the metadata is used to determine what
    datasource the data belongs to. Given a collection of metadata, this function
    extracts this "defining metadata".

    Args:
        metadata: dictionary of metadata associated to data that will be added to the
    object store.

    Returns:
       required metadata: subset of given metadata that is required for determining the
    associated datasource.

    Raises:
       ValueError: if the number of required keys is less than the minimum number of required
    keys, or if there isn't a policy registered for the given data type.
    """
    try:
        policy = get_datasource_policies()[data_type]
    except KeyError:
        raise ValueError("No Datasource policy for data type f{data_type}.")
    else:
        return policy.get_required_metadata(metadata)


class ObjectStoreConnection:
    """Represents a connection to an object store.

    This class hides the inner workings of the object store from the
    the parts of OpenGHG that deal with processing and analysing data.



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
    min_required_keys: Optional[int] = None
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
        if self.min_required_keys is None:
            self.min_required_keys = len(self.required_keys)
        self.min_required_keys = cast(int, self.min_required_keys)  # TODO: is this necessary?

        self._internal_metadata = InternalMetadata(bucket, self._key)

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
        self._internal_metadata.write()

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

    def add(self, metadata: dict[str, Any], data: xarray.Dataset, skip_keys: Optional[list[str]] = ["object_store"]) -> dict[str, Union[str, bool]]:
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
        # TODO: check for overwrites... make alternate update_store method for conflicts

        metadata = {k.lower(): v for k, v in metadata.items()}  # metastore keys are lower case
        metadata = util.to_lowercase(metadata, skip_keys=skip_keys)


        # get datasource uuid
        required_metadata = get_required_metadata(self.data_type, metadata)
        results = self.search(required_metadata)
        if len(results) > 1:
            raise DatasourceLookupError("More than one Datasource found for metadata.")
        lookup_results = None if not results else str(results[0]["uuid"])
        new_datasource = True if lookup_results is None else False

        # get datasource
        if new_datasource:
            datasource = Datasource()
            uuid = datasource.uuid()
            metadata["uuid"] = uuid
            metadata["object_store"] = self._bucket  # TODO is Gareth removing this?

            # record datasource in internal metadata
            key = "_".join(str(metadata.get(k, k)) for k in self.required_keys)  # TODO find better way to store internal metadata
            self._internal_metadata.datasource_uuids[uuid] = key
        else:
            uuid = cast(str, lookup_results)  # we know lookup_results is not none
            datasource = Datasource.load(bucket=self._bucket, uuid=uuid)

        datasource.add_data(metadata=metadata, data=data, data_type=self.data_type, skip_keys=skip_keys)
        datasource.save(bucket=self._bucket)
        # TODO remove call back from datasource, so that search metadata isn't influenced by datasource?
        if new_datasource:
            self._metastore.insert(datasource.metadata())  # NOTE: cannot use `metadata` here because datasource.add_data updates time info
        else:
            # Since the new data we're adding might have more metadata than was
            # in the existing Datasource, we update to datasource.metadata(),
            # which combines the existing metadata in the Datasource with
            # any newly added metadata.
            self._metastore.update(datasource.metadata(), tinydb.where("uuid") == datasource.uuid())
        return {"uuid": datasource.uuid(), "new": new_datasource}  # TODO change new back to new_datasource

    def delete(self, uuid: str) -> None:
        """Delete a Datasource with the given UUID.

        This deletes both the data and the record in
        the metastore.

        Args:
            uuid: UUID of the Datasource to delete.

        Returns:
            None
        """
        from openghg.objectstore import delete_object
        from openghg.store.base import Datasource
        from tinydb import where

        # Delete Datasource data
        Datasource.load(bucket=self._bucket, uuid=uuid).delete_all_data()

        # Delete the Datasource itself
        key = f"{Datasource._datasource_root}/uuid/{uuid}"
        delete_object(bucket=self._bucket, key=key)

        # Delete the UUID from the metastore
        self._metastore.remove(where("uuid") == uuid)

        # Remove Datasource from internal metadata
        del self._internal_metadata.datasource_uuids[uuid]

        # TODO: Add logging?
        # TODO: remove file hashes associated with this uuid?

    # Methods related to internal metadata
    def _datasources(self) -> list[str]:
        """For testing purposes, expose convenient list
        of datasource uuids.

        Note: do not use @cache with this function, since it
        will give incorrect results if a datasource is deleted.

        Returns:
            list of datasource uuids stored in metastore.
        """
        return [doc["uuid"] for doc in self._metastore]

    def check_file_hash(self, file_hash: str) -> None:
        if file_hash in self._internal_metadata.file_hashes.keys():
            e = f"This file has been uploaded previously with the filename: {self._internal_metadata.file_hashes[file_hash]}."
            raise ValueError(e)

    def save_file_hash(self, file_hash: str, file_path: Union[Path, str]) -> None:
        """Add {file_hash: file_name} to internal metadata."""
        if isinstance(file_path, Path):
            file_path = file_path.name
        self._internal_metadata.file_hashes[file_hash] = file_path

    def get_retrieved_hashes(self) -> list[str]:
        """Return list of previously retrieved hashes for remote data sources.

        Returns:
            List of previously retrieved hashes.
        """
        return list(self._internal_metadata.retrieved_hashes.keys())

    def save_retrieved_hashes(self, hashes: dict) -> None:
        """Store hashes of data retrieved from a remote data source such as
        ICOS or CEDA. This takes the full dictionary of hashes, removes the ones we've
        seen before and adds the new.

        Args:
            hashes: Dictionary of hashes provided by the hash_retrieved_data function,
                    of the form {hash: species_key}

        Returns:
            None
        """
        # TODO: should this deal with one item at a time and move loop to `store_data`
        # in _obssurface.py? (This is the only place this function is used.)
        new = {k: v for k, v in hashes.items() if k not in self._internal_metadata.retrieved_hashes}
        self._internal_metadata.retrieved_hashes.update(new)


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
    data_type = "surface"


class ColumnConnection(ObjectStoreConnection):
    _root = "ObsColumn"
    _uuid = "5c567168-0287-11ed-9d0f-e77f5194a415"
    data_type = "column"


class EmissionsConnection(ObjectStoreConnection):
    _root = "Emissions"
    _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"
    data_type = "emissions"


class FootprintsConnection(ObjectStoreConnection):
    _root = "Footprints"
    _uuid = "62db5bdf-c88d-4e56-97f4-40336d37f18c"
    data_type = "footprints"


class BoundaryConditionsConnection(ObjectStoreConnection):
    _root = "BoundaryConditions"
    _uuid = "4e787366-be91-4fc5-ad1b-4adcb213d478"
    data_type = "boundary_conditions"


class EulerianModelConnection(ObjectStoreConnection):
    _root = "EulerianModel"
    _uuid = "63ff2365-3ba2-452a-a53d-110140805d06"
    data_type = "eulerian_model"
