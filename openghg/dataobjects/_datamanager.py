from collections import defaultdict
from collections.abc import MutableMapping
import copy
import logging

import zarr

from openghg.objectstore import get_datasource, locking_object_store, LockingObjectStoreType
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

    def objectstore(self, data_type: str) -> LockingObjectStoreType:
        return locking_object_store(bucket=self._bucket, data_type=data_type)

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
        with self.objectstore(data_type=dtype) as objstore:
            # update DataManager's copy of metadata
            backup = self._backup[uuid][version].copy()
            self.metadata[uuid] = backup.copy()
            del backup["uuid"]

            current_metadata = objstore.search(uuid=uuid)[0]
            do_not_delete = ("uuid", "object_store", "data_type")
            to_delete = [k for k in current_metadata if k not in backup and k.lower() not in do_not_delete]

            objstore.update(uuid=uuid, metadata=backup, keys_to_delete=to_delete)

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
        """Update the metadata associated with data.

        This takes UUIDs of Datasources and updates the associated metadata.
        To update metadata pass in a dictionary of key/value pairs to update.
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

        with self.objectstore(data_type=dtype) as objstore:
            for u in uuid:
                # get current metadata for backup
                current_metadata = objstore.search(uuid=u)[0]

                # update object store
                objstore.update(uuid=u, metadata=to_update, keys_to_delete=to_delete)

                # back up metadata
                version = str(len(self._backup[u].keys()) + 1)
                self._latest = version
                self._backup[u][version] = copy.deepcopy(dict(current_metadata))

                # update DataManager's current of metadata
                internal_copy = copy.deepcopy(dict(current_metadata))

                if to_delete is not None and to_delete:
                    if not isinstance(to_delete, list):
                        to_delete = [to_delete]
                    for k in to_delete:
                        del internal_copy[k]

                if to_update is not None and to_update:
                    internal_copy.update(to_update)

                self.metadata[u] = internal_copy
                logger.info(f"Modified metadata for {u}.")

    def update_attributes(
        self,
        uuid: list | str,
        version: str | list[str] = "latest",
        data_vars: str | list[str] | None = None,
        update_global: bool = True,
        to_update: dict | None = None,
        to_delete: str | list | None = None,
    ) -> None:
        """Update the attributes of the stored Dataset.

        This takes UUIDs of Datasources (and optionally a version tag) and updates
        the associated attributes:
        - to update attributes pass in a dictionary of key/value pairs to update.
        - to delete attributes pass in a list of keys to delete.

        Args:
            uuid: UUID(s) of Datasources to be updated.
            version: optional version string
            data_vars: optional list of data vars to update; if None, then only global attributes
                will be updated.
            update_global: if True, update global attributes.
            to_update: Dictionary of metadata to add/update. New key/value pairs will be added.
            If the key already exists in the metadata the value will be updated.
            to_delete: Key(s) to delete from the metadata
        Returns:
            None
        """
        if to_update is None and to_delete is None:
            return None

        if update_global is False and data_vars is None:
            return None

        if not isinstance(uuid, list):
            uuid = [uuid]

        if not isinstance(version, list):
            version = [version] * len(uuid)

        if len(uuid) != len(version):
            raise ValueError("List passed for 'version' must have same length as 'uuid'.")

        def updater(
            attrs: MutableMapping, to_update: dict | None = None, to_delete: str | list | None = None
        ) -> bool:
            """Update/delete attributes.

            Can be used on either global attributes or the attributes of a data variable.

            Args:
                attrs: dict (or MutableMapping) of attributes to update.
                to_update: dict of attributes to update.
                to_delete: key or list of keys of attributes to delete.

            Returns:
                True if attributes either updated or deleted, False otherwise.
            """
            updated = False
            if to_delete is not None and to_delete:
                if not isinstance(to_delete, list):
                    to_delete = [to_delete]

                for k in to_delete:
                    attrs.pop(k)

                updated = True

            if to_update is not None and to_update:
                attrs.update(to_update)
                updated = True

            return updated

        for u, v in zip(uuid, version):
            updated = False

            d = get_datasource(bucket=self._bucket, uuid=u)

            if v == "latest":
                v = d._latest_version

            zs = d._store._stores[v]  # zarr store for specified version
            group = zarr.open_group(zs)

            # update global
            if update_global:
                global_updated = updater(group.attrs, to_update, to_delete)
                updated = updated or global_updated
            # update data vars
            if data_vars is not None:
                if not isinstance(data_vars, list):
                    data_vars = [data_vars]

                for dv in data_vars:
                    try:
                        arr = group[dv]
                    except KeyError:
                        logger.warning(f"Data variable {dv} not present in zarr store. Skipping.")
                        continue
                    else:
                        data_var_updated = updater(arr.attrs, to_update, to_delete)
                        updated = updated or data_var_updated

            if updated:
                zarr.consolidate_metadata(zs)
                logger.info(f"Modified attributes for {u}.")

    def delete_datasource(self, uuid: list | str) -> None:
        """Delete Datasource(s) in the object store.
        At the moment we only support deleting the complete Datasource.

        NOTE: Make sure you really want to delete the Datasource(s)

        Args:
            uuid: UUID(s) of objects to delete
        Returns:
            None
        """
        if not isinstance(uuid, list):
            uuid = [uuid]

        dtype = self._check_datatypes(uuid=uuid)

        with self.objectstore(data_type=dtype) as objstore:
            for uid in uuid:
                objstore.delete(uid)
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
