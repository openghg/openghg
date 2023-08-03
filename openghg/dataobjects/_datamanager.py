from collections import defaultdict
import copy
from openghg.store.base import Datasource
from openghg.store.spec import define_data_type_classes
from openghg.store import load_metastore
from openghg.objectstore import delete_object, get_writable_bucket
import logging
import tinydb
from typing import DefaultDict, Dict, List, Set, Optional, Union

logger = logging.getLogger("openghg.dataobjects")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class DataManager:
    def __init__(self, metadata: Dict[str, Dict], store: str):
        self.metadata = metadata
        self._store = store
        self._bucket = get_writable_bucket(name=store)
        self._backup: DefaultDict[str, Dict[str, Dict]] = defaultdict(dict)
        self._latest = "latest"

    def __str__(self) -> str:
        return str(self.metadata)

    def __bool__(self) -> bool:
        return bool(self.metadata)

    def _check_datatypes(self, uuid: Union[str, List]) -> str:
        """Check the UUIDs are correct and ensure they all
        belong to a single data type

        Args:
            uuid: UUID(s) to check
        Returns:
            None
        """
        if not isinstance(uuid, list):
            uuid = [uuid]

        invalid_keys = [k for k in uuid if k not in self.metadata]

        if invalid_keys:
            raise ValueError(f"Invalid UUIDs: {invalid_keys}")

        # We should only have one data type
        data_types: Set[str] = {self.metadata[i]["data_type"] for i in uuid}

        if not data_types:
            raise ValueError("Unable to read data_type from metadata.")

        if len(data_types) > 1:
            raise ValueError(
                f"We can only modify Datasources of a single data type at once. We currently have {data_types}"
            )

        return data_types.pop()

    def refresh(self) -> None:
        """Force refresh the internal metadata store with data from the object store.

        Returns:
            None
        """
        from openghg.retrieve import search

        uuids = list(self.metadata.keys())
        res = search(uuid=uuids)
        self.metadata = res.metadata

    def restore(self, uuid: str, version: Union[str, int] = "latest") -> None:
        """Restore a backed-up version of a Datasource's metadata.

        Args:
            uuid: UUID of Datasource to retrieve
            version: Version of metadata to restore
        Returns:
            None
        """
        if version == "latest":
            version = self._latest

        version = str(version)

        dtype = self._check_datatypes(uuid=uuid)

        data_objs = define_data_type_classes()
        data_class = data_objs[dtype]

        with data_class(bucket=self._bucket) as dclass:
            metastore = dclass._metastore
            backup = self._backup[uuid][version]
            self.metadata[uuid] = backup

            metastore.remove(tinydb.where("uuid") == uuid)
            metastore.insert(backup)

            d = Datasource.load(bucket=self._bucket, uuid=uuid)
            d._metadata = backup

    def view_backup(self, uuid: Optional[str] = None, version: Optional[str] = None) -> Dict:
        """View backed-up metadata for all Datasources
        or a single Datasource if a UUID is passed in.

        Args:
            uuid: UUID of Datasource
        Returns:
            dict: Dictionary of versioned metadata
        """
        if uuid is not None:
            if version is not None:
                version = str(version)
                return self._backup[uuid][version]

            return self._backup[uuid]
        else:
            return self._backup

    def update_metadata(
        self,
        uuid: Union[List, str],
        to_update: Optional[Dict] = None,
        to_delete: Union[str, List, None] = None,
    ) -> None:
        """Update the metadata associated with data. This takes UUIDs of Datasources and updates
        the associated metadata. If you want to delete some metadata

        Args:
            uuid: UUID(s) of Datasources to be updated.
            to_update: Dictionary of metadata to add/update. New key/value pairs will be added.
            If the key already exists in the metadata the value will be updated.
            to_delete: Key(s) to delete from the metadata
        Returns:
            None
        """
        from tinydb.operations import delete as tinydb_delete

        if to_update is None and to_delete is None:
            return None

        # Add in ability to delete metadata keys
        if not isinstance(uuid, list):
            uuid = [uuid]

        dtype = self._check_datatypes(uuid=uuid)

        data_objs = define_data_type_classes()
        metakey = data_objs[dtype]._metakey

        with load_metastore(bucket=self._bucket, key=metakey) as store:
            for u in uuid:
                d = Datasource.load(bucket=self._bucket, uuid=u, shallow=True)
                # Save a backup of the metadata for now
                found_record = store.search(tinydb.where("uuid") == u)
                current_metadata = found_record[0]

                version = str(len(self._backup[u].keys()) + 1)
                self._latest = version
                self._backup[u][version] = copy.deepcopy(dict(current_metadata))
                # To update this object's records
                internal_copy = copy.deepcopy(dict(current_metadata))
                n_records = len(self._backup[u][version])

                # Do a quick check to make sure we're not being asked to delete all the metadata
                if to_delete is not None:
                    if not isinstance(to_delete, list):
                        to_delete = [to_delete]

                    if "uuid" in to_delete:
                        raise ValueError("Cannot delete the UUID key.")

                    if len(to_delete) == n_records:
                        raise ValueError("We can't remove all the metadata associated with this Datasource.")
                    for k in to_delete:
                        d._metadata.pop(k)
                        internal_copy.pop(k)

                    try:
                        store.update_multiple(
                            [(tinydb_delete(k), tinydb.where("uuid") == u) for k in to_delete]
                        )
                    except KeyError:
                        raise ValueError(
                            "Unable to remove keys from metadata store, please ensure they exist."
                        )

                if to_update is not None:
                    if "uuid" in to_update:
                        raise ValueError("Cannot update the UUID.")

                    d._metadata.update(to_update)
                    internal_copy.update(to_update)
                    response = store.update(to_update, tinydb.where("uuid") == u)

                    if not response:
                        raise ValueError("Unable to update metadata, possible metadata sync error.")

                d.save(bucket=self._bucket)

                # Update the metadata stored internally so we're up to date
                self.metadata[u] = internal_copy

                logger.info(f"Modified metadata for {u}.")

    def delete_datasource(self, uuid: Union[List, str]) -> None:
        """Delete a Datasource in the object store.
        At the moment we only support deleting the complete Datasource.

        NOTE: Make sure you really want to delete the Datasource(s)

        Args:
            uuid: UUID(s) of objects to delete
        Returns:
            None
        """
        # Add in ability to delete metadata keys
        if not isinstance(uuid, list):
            uuid = [uuid]

        dtype = self._check_datatypes(uuid=uuid)
        data_objs = define_data_type_classes()
        dclass = data_objs[dtype]

        with dclass(bucket=self._bucket) as dc:
            for uid in uuid:
                # First remove the data from the metadata store
                dc._metastore.remove(tinydb.where("uuid") == uid)
                # Delete all the data associated with a Datasource
                d = Datasource.load(bucket=self._bucket, uuid=uid, shallow=True)
                d.delete_all_data()
                # Then delete the Datasource itself
                key = d.key()
                delete_object(bucket=self._bucket, key=key)
                # Remove from the list of Datasources the object knows about
                dc.remove_datasource(uuid=uid)

                logger.info(f"Deleted Datasource with UUID {uid}.")
