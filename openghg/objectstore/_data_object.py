"""Module for class DataObject"""

from __future__ import annotations

from collections.abc import Iterable, Generator, MutableMapping
from contextlib import contextmanager
from typing import Any, cast, Hashable, Iterator, Optional, Union

import pandas as pd
import xarray as xr

from openghg.objectstore.metastore import open_metastore, TinyDBMetaStore
from openghg.store.base import Datasource
from openghg.types import ObjectStoreError
from openghg.util import daterange_overlap


DateType = Union[str, pd.Timestamp]


class DataObject(MutableMapping):
    """DataObjects represent a unit of metadata and data stored in the object store.

    A DataObject acts like a dictionary of metadata, but also has methods to query and
    return data from the Datasource associated with that metadata.
    """

    # TODO: should this be a mutable mapping? or just a mapping? I don't really see why it should be modified...
    # Any updates should probably be propagated to the object store, which would require a different mechanism
    # ...for now, DataManager needs this to be mutable
    def __init__(
        self,
        metadata: dict,
        bucket: Optional[str] = None,
    ) -> None:
        if "uuid" not in metadata:
            raise ValueError("Metadata must contain UUID.")

        self.uuid = metadata["uuid"]

        if "object_store" not in metadata and bucket is None:
            raise ValueError("If 'object_store' not in metadata, you must provide a value for `bucket`.")

        self.bucket = bucket or metadata["object_store"]

        # TODO: when DataObjects are created directly by an ObjectStore class, we won't need to remove this internal info
        if "object_store" in metadata:
            del metadata["object_store"]

        self._metadata = metadata

    @property
    def datasource(self) -> Datasource:
        try:
            result = Datasource(self.bucket, self.uuid)
        except ObjectStoreError:
            # this option is to allow tests for search without adding real data to the object store
            result = Datasource(self.bucket)
        return result

    @property
    def metadata(self) -> dict:
        return self._metadata

    @property
    @contextmanager
    def metastore(self) -> Generator[TinyDBMetaStore, None, None]:
        with open_metastore(bucket=self.bucket, data_type=self.data_type) as ms:
            yield ms

    @property
    def data_type(self) -> str:
        return cast(str, self._metadata["data_type"])

    def __hash__(self) -> int:
        return hash(f"{self.uuid}_{self.bucket}")

    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, DataObject):
            return NotImplemented
        return (self.uuid == other.uuid) and (self.bucket == other.bucket)

    def __iter__(self) -> Iterator:
        return iter(self._metadata)

    def __len__(self) -> int:
        return len(self._metadata)

    def __getitem__(self, key: Hashable) -> Any:
        # TODO: add lookup for Datasource metadata as well, so it doesn't need to be copied to the metastore
        # if key == "object_store":
        #     return self.bucket
        return self._metadata[key]

    def __setitem__(self, key: Hashable, value: Any) -> None:
        self._metadata[key] = value

    def __delitem__(self, key: Hashable) -> None:
        del self._metadata[key]

    def get_data(
        self,
        start_date: Optional[DateType] = None,
        end_date: Optional[DateType] = None,
        version: str = "latest",
    ) -> xr.Dataset:
        result = self.datasource.get_data(version=version)

        # NOTE: len(result.time) > 1 is a hack to avoid slicing annual fluxes
        # but it only works if there is a single year of fluxes
        # TODO: move this slicing logic to datasource, where it can be
        # implemented more carefully
        if (start_date or end_date) and len(result.time) > 1:
            if start_date is not None:
                start_date = pd.to_datetime(start_date)
            if end_date is not None:
                end_date = pd.to_datetime(end_date) - pd.to_timedelta("1ns")

            result = result.sortby("time").sel(time=slice(start_date, end_date))

        elif not result.indexes["time"].is_monotonic_increasing:
            # Slicing won't work correctly if the data isn't sorted by time
            result = result.sortby("time")

        return result

    def has_data_between(
        self, start_date: Optional[DateType] = None, end_date: Optional[DateType] = None
    ) -> bool:
        # TODO: move this logic to Datasource

        latest_version = self.datasource._latest_version
        date_keys = self.datasource._data_keys[latest_version] if self.datasource._data_keys else []

        if start_date is not None or end_date is not None:
            if start_date is None:
                start_date = pd.to_datetime(0)  # UNIX epoch

            if end_date is None:
                end_date = pd.Timestamp.now()

            new_daterange = f"{start_date}_{end_date}"

            return any(daterange_overlap(existing, new_daterange) for existing in date_keys)

        return True

    def copy(self) -> DataObject:
        """This is needed in DataManager, which makes a copy of the metadata returned by a search."""
        return DataObject(self.metadata.copy(), self.bucket)

    def __copy__(self) -> DataObject:
        return self.copy()

    def __deepcopy__(self, memo: Any) -> DataObject:
        return self.copy()  # TODO: do a deepcopy of metadata here?

    def update_metadata(self, to_update: dict[str, Any]) -> None:
        # TODO `to_update` needs to be formatted...
        if "uuid" in to_update:
            raise ValueError("Cannot update UUID.")  # TODO: is this necessary? the metastore should check...

        with self.metastore as metastore:
            metastore.update(where={"uuid": self.uuid}, to_update=to_update)

        self._metadata.update(to_update)  # update internal copy if metastore update successful

        with self.datasource as ds:
            ds._metadata.update(to_update)

    def delete_metadata(self, to_delete: list[str]) -> None:
        # TODO `to_update` needs to be formatted...
        if "uuid" in to_delete:
            raise ValueError("Cannot delete UUID.")

        bad_keys = set(to_delete).difference(set(self._metadata.keys()))

        if bad_keys:
            raise ValueError(f"Keys {bad_keys} not present in metadata; only existing keys can be deleted.")

        with self.metastore as metastore:
            metastore.update(where={"uuid": self.uuid}, to_delete=to_delete)

        # update internal metadata to match
        for key in to_delete:
            del self._metadata[key]

        with self.datasource as ds:
            for key in to_delete:
                ds._metadata.pop(key)

    def delete(self) -> None:
        from openghg.objectstore import delete_object

        with self.metastore as metastore:
            metastore.delete({"uuid": self.uuid})

        with self.datasource as ds:
            print("open datasource", ds)
            key = ds.key()
            ds.delete_all_data()

        # need to delete Datasource object since closing the datasource context manager save
        # some metadata
        delete_object(bucket=self.bucket, key=key)  # TODO: this should be done by Datasource?


class DataObjectContainer:
    def __init__(
        self,
        data_objects: Iterable[DataObject],
    ) -> None:
        """Create a container for DataObjects.

        Args:
            data_objects: Iterable (e.g. list) of DataObjects.

        Returns:
            None
        """
        self.data_objects = list(data_objects) or []

    def to_dict(self) -> dict:
        return {do.uuid: do.metadata for do in self.data_objects}

    def __str__(self) -> str:
        return str(self.data_objects)

    def __repr__(self) -> str:
        return f"DataObjectContainer({self.__str__()})"

    def __bool__(self) -> bool:
        return bool(self.data_objects)

    def __len__(self) -> int:
        return len(self.data_objects)

    def __iter__(self) -> Iterator:
        return iter(self.data_objects)

    @property
    def uuids(self) -> list[str]:
        """Return the UUIDs of the found data

        Returns:
            list: List of UUIDs
        """
        return [do.uuid for do in self.data_objects]

    # NOTE: these get/set item methods would probably be faster if we stored the data by UUID.
    # These methods are needed by `DataManager`, and eventually, it would be better if DataObjects could
    # be updated directly.

    def __getitem__(self, key: Hashable) -> DataObject:
        for do in self.data_objects:
            if key in (do, do.uuid):
                return do
        raise KeyError(f"Item with key {key} not found.")

    def __setitem__(self, key: Hashable, value: DataObject) -> None:
        """Set value based on UUID."""
        if key not in self.uuids:
            self.data_objects.append(value)
        else:
            old = next(do for do in self.data_objects if do.uuid == key)
            self.data_objects.remove(old)
            self.data_objects.append(value)

    def __contains__(self, value: Union[str, DataObject]) -> bool:
        """Return True if `value` is a DataObject or UUID of a DataObject in the container."""
        return any(value in (do, do.uuid) for do in self)
