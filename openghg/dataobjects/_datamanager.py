from collections import defaultdict
import copy
import logging

from openghg.store.base import Datasource
from openghg.objectstore.metastore import open_metastore
from openghg.objectstore import get_writable_bucket, get_writable_buckets
from openghg.types import ObjectStoreError

logger = logging.getLogger("openghg.dataobjects")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class DataManager:
    def __init__(self, metadata: dict[str, dict], store: str):
        # We don't want the object store in this metadata as we want it to be the
        # unadulterated metadata to properly reflect what's stored.
        self.metadata = self._clean_metadata(metadata=metadata)
        self._store = store
        self._bucket = get_writable_bucket(name=store)
        self._backup: defaultdict[str, dict[str, dict]] = defaultdict(dict)
        self._latest = "latest"

    def __str__(self) -> str:
        return str(self.metadata)

    def __bool__(self) -> bool:
        return bool(self.metadata)

    def _clean_metadata(self, metadata: dict) -> dict:
        """Ensures the metadata we give to the user is the metadata
        stored in the metastore and the Datasource and hasn't been modified by the
        search function. Currently this just removes the object_store key

        Args:
            metadata: Dictionary of metadata, we expect
        Returns:
            dict: Metadata without specific keys
        """
        metadata = copy.deepcopy(metadata)
        for m in metadata.values():
            try:
                del m["object_store"]
            except KeyError:
                pass

        return metadata

    def _check_datatypes(self, uuid: str | list) -> str:
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
        data_types: set[str] = {self.metadata[i]["data_type"] for i in uuid}

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
        # We don't want the object store in this metadata as we want it to be the
        # unadulterated metadata to properly reflect what's stored.
        for m in res.metadata.values():
            try:
                del m["object_store"]
            except KeyError:
                pass

        self.metadata = self._clean_metadata(metadata=res.metadata)

    def restore(self, uuid: str, version: str | int = "latest") -> None:
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
        with open_metastore(data_type=dtype, bucket=self._bucket) as metastore:
            backup = self._backup[uuid][version]
            self.metadata[uuid] = backup

            metastore.delete({"uuid": uuid})
            metastore.insert(backup)

            d = Datasource(bucket=self._bucket, uuid=uuid)
            d._metadata = backup
            d.save()

    def view_backup(self, uuid: str | None = None, version: str | None = None) -> dict:
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
        uuid: list | str,
        to_update: dict | None = None,
        to_delete: str | list | None = None,
    ) -> None:
        """Update the metadata associated with data. This takes UUIDs of Datasources and updates
        the associated metadata. To update metadata pass in a dictionary of key/value pairs to update.
        To delete metadata pass in a list of keys to delete.

        Args:
            uuid: UUID(s) of Datasources to be updated.
            to_update: Dictionary of metadata to add/update. New key/value pairs will be added.
            If the key already exists in the metadata the value will be updated.
            to_delete: Key(s) to delete from the metadata
        Returns:
            None
        """
        if to_update is None and to_delete is None:
            return None

        if not isinstance(uuid, list):
            uuid = [uuid]

        dtype = self._check_datatypes(uuid=uuid)

        with open_metastore(bucket=self._bucket, data_type=dtype) as metastore:
            for u in uuid:
                updated = False
                d = Datasource(bucket=self._bucket, uuid=u)
                # Save a backup of the metadata for now
                found_record = metastore.search({"uuid": u})
                current_metadata = found_record[0]

                version = str(len(self._backup[u].keys()) + 1)
                self._latest = version
                self._backup[u][version] = copy.deepcopy(dict(current_metadata))
                # To update this object's records
                internal_copy = copy.deepcopy(dict(current_metadata))
                n_records = len(self._backup[u][version])

                # Do a quick check to make sure we're not being asked to delete all the metadata
                if to_delete is not None and to_delete:
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
                        metastore.update(where={"uuid": u}, to_delete=to_delete)
                    except KeyError:
                        raise ValueError(
                            "Unable to remove keys from metadata store, please ensure they exist."
                        )

                    updated = True

                if to_update is not None and to_update:
                    if "uuid" in to_update:
                        raise ValueError("Cannot update the UUID.")

                    d._metadata.update(to_update)
                    internal_copy.update(to_update)
                    metastore.update(where={"uuid": u}, to_update=to_update)

                    updated = True

                if updated:
                    d.save()
                    # Update the metadata stored internally so we're up to date
                    self.metadata[u] = internal_copy
                    logger.info(f"Modified metadata for {u}.")

    def delete_datasource(self, uuid: list | str) -> None:
        """Delete Datasource(s) in the object store.
        At the moment we only support deleting the complete Datasource.

        NOTE: Make sure you really want to delete the Datasource(s)

        Args:
            uuid: UUID(s) of objects to delete
        Returns:
            None
        """
        from openghg.objectstore import delete_object

        # Add in ability to delete metadata keys
        if not isinstance(uuid, list):
            uuid = [uuid]

        dtype = self._check_datatypes(uuid=uuid)

        with open_metastore(bucket=self._bucket, data_type=dtype) as metastore:
            for uid in uuid:
                # First remove the data from the metadata store
                metastore.delete({"uuid": uid})

                # Delete all the data associated with a Datasource and the
                # data in its zarr store.
                d = Datasource(bucket=self._bucket, uuid=uid)
                d.delete_all_data()

                # Then delete the Datasource itself
                delete_object(bucket=self._bucket, key=d.key())

                logger.info(f"Deleted Datasource with UUID {uid}.")


def data_manager(data_type: str, store: str, **kwargs: dict) -> DataManager:
    """Lookup the data / metadata you'd like to modify.

    Args:
        data_type: Type of data, for example surface, flux, footprint
        store: Name of store
        kwargs: Any pair of keyword arguments for searching
    Returns:
        DataManager: A handler object to help modify the metadata
    """
    from openghg.dataobjects import DataManager
    from openghg.retrieve import search

    writable_stores = get_writable_buckets()

    if store not in writable_stores:
        raise ObjectStoreError(f"You do not have permission to write to the {store} store.")

    res = search(data_type=data_type, store=store, **kwargs)
    metadata = res.metadata
    return DataManager(metadata=metadata, store=store)
