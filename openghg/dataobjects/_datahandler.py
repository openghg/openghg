# from collections import defaultdict
# import copy
from openghg.store.base import Datasource
from openghg.store.spec import define_data_type_classes
from openghg.store import load_metastore

# from openghg.util import timestamp_now
import tinydb
from typing import Dict, List, Optional, Union


class DataHandler:
    def __init__(self, metadata: Optional[Dict[str, Dict]] = None):
        self.metadata = metadata if metadata is not None else {}
        # self._backup = defaultdict(dict)
        self._version = None

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

    # def restore(uuid: str, version: str = "latest") -> Dict:
    #     """Restore a version of metadata from the backup store

    #     Args:
    #         uuid: UUID of Datasource to retrieve
    #         version: Version of metadata to restore
    #     Returns:
    #         dict: Dictionary of metadata
    #     """

    # def _version() -> str:
    #     """ Get the latest version in the backup

    #     """
    #     # Backup the old data keys at "latest"
    #     version_str = f"v{str(len(self._data_keys))}"

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

        # timestamp_str = str(timestamp_now())

        with load_metastore(key=metakey) as store:
            for u in uuid:
                d = Datasource.load(uuid=u, shallow=True)
                # Save a backup of the metadata for now
                # if
                # version = sorted(self._backup.keys())[-1]
                # self._backup[u][timestamp_str] = copy.deepcopy(d._metadata)

                # Do a quick check to make sure we're not being asked to delete all the metadata
                if to_delete is not None:
                    if len(to_delete) == len(d._metadata):
                        raise ValueError("We can't remove all the metadata associated with this Datasource.")
                    for k in to_delete:
                        d._metadata.pop(k)

                    try:
                        store.update_multiple(
                            [(tinydb_delete(k), tinydb.where("uuid") == u) for k in to_delete]
                        )
                    except KeyError:
                        raise ValueError("Unable to remove keys, please ensure they exist.")

                if to_update is not None:
                    d._metadata.update(to_update)
                    response = store.update(to_update, tinydb.where("uuid") == u)

                    if not response:
                        raise ValueError("Unable to update metadata, possible metadata sync error.")

                d.save()

                print(f"Modified metadata for {u}.")

    def delete_datasource(self, uuid: Union[List, str]) -> None:
        """Delete a Datasource in the object store.
        At the moment we only support deleting the complete Datasource.

        NOTE: Make sure you really want to delete the Datasource(s)

        Args:
            uuid: UUID(s) of objects to delete
        Returns:
            None
        """
        from openghg.objectstore import delete_object, get_bucket

        # Add in ability to delete metadata keys
        if not isinstance(uuid, list):
            uuid = [uuid]

        bucket = get_bucket()

        dtype = self._check_datatypes(uuid=uuid)
        data_objs = define_data_type_classes()
        data_obj = data_objs[dtype].load()
        metakey = data_obj._metakey

        with load_metastore(key=metakey) as store:
            for u in uuid:
                # First remove the data from the metadata store
                store.remove(tinydb.where("uuid") == u)
                # Delete all the data associated with a Datasource
                d = Datasource.load(uuid=u)
                d.delete_all_data()
                # Then delete the Datasource itself
                key = d.key()
                delete_object(bucket=bucket, key=key)
                # Remove from the list of Datasources the object knows about
                data_obj.remove_datasource(uuid=u)

            print(f"Deleted Datasource with UUID {u}.")

        data_obj.save()
