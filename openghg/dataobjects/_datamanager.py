from collections import defaultdict
import copy
import logging
from typing import Dict, List, Set, Optional, Union

from openghg.objectstore.metastore import open_metastore
from openghg.objectstore import DataObject, get_writable_buckets
from openghg.types import ObjectStoreError
from ._searchresults import SearchResults

logger = logging.getLogger("openghg.dataobjects")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class DataManager:
    def __init__(self, data_objects: SearchResults):
        # We don't want the object store in this metadata as we want it to be the
        # unadulterated metadata to properly reflect what's stored.
        self.data_objects = data_objects
        self._backup: defaultdict[str, dict[str, DataObject]] = defaultdict(dict)
        self._latest = "latest"

    def __str__(self) -> str:
        return str(self.data_objects)

    def __bool__(self) -> bool:
        return bool(self.data_objects)

    def _check_datatypes(self, uuid: Union[str, DataObject, list]) -> str:
        """Check the UUIDs are correct and ensure they all
        belong to a single data type

        Args:
            uuid: UUID(s) to check
        Returns:
            None
        """
        if not isinstance(uuid, list):
            uuid = [uuid]

        invalid_keys = [k for k in uuid if k not in self.data_objects]

        if invalid_keys:
            raise ValueError(f"Invalid UUIDs: {invalid_keys}")

        # We should only have one data type
        data_types: Set[str] = {do["data_type"] for do in self.data_objects}

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

        self.data_objects = search(uuid=self.data_objects.uuids)

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

        try:
            data_object = [do for do in self.data_objects if do.uuid == uuid][0]
        except IndexError:
            raise ValueError(f"UUID {uuid} not found in DataManager.")

        with open_metastore(data_type=dtype, bucket=data_object.bucket) as metastore:
            backup = self._backup[uuid][version]
            self.data_objects[uuid] = backup

            metastore.delete({"uuid": uuid})
            metastore.insert(dict(backup))

            with data_object.datasource as ds:
                ds._metadata = dict(backup)

    def view_backup(
        self, uuid: Optional[str] = None, version: Optional[str] = None
    ) -> Union[dict, DataObject]:
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
        uuids: Union[list[Union[str, DataObject]], str, DataObject],
        to_update: Optional[Dict] = None,
        to_delete: Union[str, List, None] = None,
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

        if not isinstance(uuids, list):
            uuids = [uuids]

        self._check_datatypes(uuids)  # TODO: this name isn't really fitting...

        for uuid in uuids:
            if uuid in self.data_objects:
                do = self.data_objects[uuid]
                updated = False

                # Save a backup of the metadata for now
                current_metadata = do.copy()

                version = str(len(self._backup[do.uuid].keys()) + 1)
                self._latest = version
                self._backup[do.uuid][version] = copy.deepcopy(current_metadata)

                if to_delete is not None and to_delete:
                    if not isinstance(to_delete, list):
                        to_delete = [to_delete]

                    # Do a quick check to make sure we're not being asked to delete all the metadata
                    n_records = len(self._backup[do.uuid][version])
                    if len(to_delete) == n_records:
                        raise ValueError("We can't remove all the metadata associated with this Datasource.")

                    do.delete_metadata(to_delete)
                    updated = True

                if to_update is not None and to_update:
                    do.update_metadata(to_update)
                    updated = True

                if updated:
                    logger.info(f"Modified metadata for {do.uuid}.")

    def delete_datasource(self, uuids: Union[list, str, DataObject]) -> None:
        """Delete Datasource(s) in the object store.
        At the moment we only support deleting the complete Datasource.

        NOTE: Make sure you really want to delete the Datasource(s)

        Args:
            uuid: UUID(s) of objects to delete
        Returns:
            None
        """
        # Add in ability to delete metadata keys
        if not isinstance(uuids, list):
            uuids = [uuids]

        self._check_datatypes(uuids)

        for uuid in uuids:
            if uuid in self.data_objects:
                do = self.data_objects[uuid]
                do.delete()
                logger.info(f"Deleted Datasource with UUID {do.uuid}.")


def data_manager(data_type: str, store: str, **kwargs: Dict) -> DataManager:
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
    return DataManager(data_objects=res)
