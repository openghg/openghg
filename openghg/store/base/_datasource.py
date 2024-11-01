from __future__ import annotations
from collections import defaultdict
import warnings
from typing import Any, cast, Dict, List, Literal, Optional, Tuple, TypeVar, Union
from types import TracebackType
import logging
from pandas import DataFrame, Timestamp, Timedelta
import xarray as xr
import numpy as np
from uuid import uuid4
from openghg.objectstore import exists, get_object_from_json
from openghg.util import split_daterange_str, timestamp_tzaware
from openghg.store.spec import define_data_types
from openghg.types import DataOverlapError, ObjectStoreError


logger = logging.getLogger("openghg.store.base")
logger.setLevel(logging.DEBUG)

__all___ = ["Datasource"]

T = TypeVar("T", bound="Datasource")


class Datasource:
    """A Datasource holds data relating to a single source, such as a specific species
    at a certain height on a specific instrument
    """

    _datasource_root = "datasource"

    def __init__(self, bucket: str, uuid: Optional[str] = None, mode: Literal["r", "rw"] = "rw") -> None:
        from openghg.util import timestamp_now
        from openghg.store.storage._localzarrstore import get_local_zarr_store

        if uuid is not None:
            key = f"{Datasource._datasource_root}/uuid/{uuid}"
            if exists(bucket=bucket, key=key):
                stored_data = get_object_from_json(bucket=bucket, key=key)
                self.__dict__.update(stored_data)
            else:
                raise ObjectStoreError(f"No Datasource with uuid {uuid} found in bucket {bucket}")
        else:
            self._uuid = str(uuid4())
            self._creation_datetime = str(timestamp_now())
            self._metadata: Dict[str, Union[str, List, Dict]] = {}
            self._data_type: str = ""
            # Hold information regarding the versions of the data
            self._latest_version: str = ""
            self._timestamps: Dict[str, str] = {}

        if mode not in ("r", "rw"):
            raise ValueError("Invalid mode. Please select r or rw.")

        self._mode = mode
        # TODO - add in selection of other store types, this could NetCDF, sparse, whatever
        # self._store = LocalZarrStore(bucket=bucket, datasource_uuid=self._uuid, mode=mode)
        self._store = get_local_zarr_store(bucket=bucket, datasource_uuid=self._uuid)
        # So we know where to write out to
        self._bucket = bucket

    def __enter__(self) -> Datasource:
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
            self.save()

    def add_metadata_key(self, key: str, value: str) -> None:
        """Add a label to the metadata dictionary with the key value pair
        This will overwrite any previous entry stored at that key.

        Args:
            key: Key for dictionary
            value: Value for dictionary
        Returns:
            None
        """
        value = str(value)
        self._metadata[key.lower()] = value.lower()

    def add_data(
        self,
        metadata: Dict,
        data: xr.Dataset,
        data_type: str,
        sort: bool = False,
        drop_duplicates: bool = False,
        skip_keys: Optional[List] = None,
        new_version: bool = True,
        if_exists: str = "auto",
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
    ) -> None:
        """Add data to this Datasource and segment the data by size.
        The data is stored as a tuple of the data and the daterange it covers.

        Args:
            metadata: Metadata on the data for this Datasource
            data: xarray.Dataset
            data_type: Type of data, one of ["boundary_conditions", "column", "emissions", "flux", "flux_timeseries", "footprints", "surface", "eulerian_model"].
            sort: Sort data in time dimension
            drop_duplicates: Drop duplicate timestamps, keeping the first value
            skip_keys: Keys to not standardise as lowercase
            new_version: Create a new version of the data
            if_exists: What to do if existing data is present.
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - creates new version with just new data
                - "combine" - replace and insert new data into current timeseries
            compressor: Compression for zarr encoding
            filters: Filters for zarr encoding
        Returns:
            None
        """
        expected_data_types = define_data_types()

        data_type = data_type.lower()
        if data_type not in expected_data_types:
            raise TypeError(f"Incorrect data type selected. Please select from one of {expected_data_types}")

        self.add_metadata(metadata=metadata, skip_keys=skip_keys)

        if "time" in data.coords:
            return self.add_timed_data(
                data=data,
                data_type=data_type,
                sort=sort,
                drop_duplicates=drop_duplicates,
                new_version=new_version,
                if_exists=if_exists,
                compressor=compressor,
                filters=filters,
            )
        else:
            raise NotImplementedError()

    def add_timed_data(
        self,
        data: xr.Dataset,
        data_type: str,
        sort: bool,
        drop_duplicates: bool,
        new_version: bool = True,
        if_exists: str = "auto",
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
    ) -> None:
        """Add data to this Datasource

        Args:
            data: An xarray.Dataset
            data_type: Name of data_type defined by
                openghg.store.spec.define_data_types()
            sort: If True sort by time, may load all data into memory
            drop_duplicates: If True drop duplicates, keeping first found duplicate
            new_version: Create a new version of the data
            if_exists: What to do if existing data is present.
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - creates new version with just new data
                - "combine" - replace and insert new data into current timeseries
            compressor: Compression for zarr encoding
            filters: Filters for zarr encoding
        Returns:
            None
        """
        from openghg.util import timestamp_now

        if sort:
            data = data.sortby("time")
        if drop_duplicates:
            data = data.drop_duplicates("time", keep="first")

        if not self._store:
            self._store.insert(data)
        elif if_exists == "new":
            # short cut to avoid copying before overwriting
            if new_version is True:
                self._store.create_version(checkout=True, copy_current=False)
                self._store.insert(data)
            else:
                self._store.overwrite(data)
        elif if_exists == "combine":
            if new_version is True:
                self._store.create_version(checkout=True, copy_current=True)
            self._store.upsert(data)
        else:
            # if_exists == "auto"
            self._store.insert(data, on_conflict="error")

        self._latest_version = self._store.latest_version

        # update metaata
        index = self._store.index.to_array()
        start = np.min(index)
        end = np.max(index)
        self.add_metadata_key("start_date", start)
        self.add_metadata_key("end_date", end)

        self._data_type = data_type
        self.add_metadata_key(key="data_type", value=data_type)

        self.add_metadata_key(key="latest_version", value=self._latest_version)

        # update timestamps
        timestamp_str_now = str(timestamp_now())
        self._timestamps[self._latest_version] = timestamp_str_now
        self.add_metadata_key(key="timestamp", value=timestamp_str_now)

        self._last_updated = timestamp_str_now

        self._metadata["versions"] = self.versions  # TODO hack for backwards compatibility; need to fix SearchResults to remove this

    def delete_all_data(self) -> None:
        """Delete datasource entirely.

        Deletes the zarr store that contains all the data
        associated with this Datasource, clears out all keys
        stored in this Datasource, and removes the uuid
        from the `data` path.

        Returns:
            None
        """
        self._store.delete_all()
        # self._store.close() # TODO: do we need this to cover cases besides zarr stores?
        self._metadata.clear()
        self._timestamps.clear()

    def delete_version(self, version: str) -> None:
        """Delete a specific version of data.

        Args:
            bucket: Bucket containing data
            version: Version string
        Returns:
            None
        """
        if version == "latest":
            raise ValueError("Specific version required for deletion.")

        self._store.checkout_version(version)
        self._store.delete()
        del self._timestamps[version]

    def add_metadata(self, metadata: Dict, skip_keys: Optional[List] = None) -> None:
        """Add all metadata in the dictionary to this Datasource

        Args:
            metadata: Dictionary of metadata
            skip_keys: Keys to not standardise as lowercase
        Returns:
            None
        """
        from openghg.util import to_lowercase

        try:
            del metadata["object_store"]
        except KeyError:
            pass
        else:
            logger.warning("object_store should not be added to the metadata, removing.")

        lowercased: Dict = to_lowercase(metadata, skip_keys=skip_keys)
        self._metadata.update(lowercased)

    def save(self) -> None:
        """Save this Datasource object as JSON to the object store

        Args:
            bucket: Bucket to hold data
            compression: True if data should be compressed on save
        Returns:
            None
        """
        from openghg.objectstore import set_object_from_json

        DO_NOT_STORE = {
            "_store",
            "_bucket",
        }

        internal_metadata = {k: v for k, v in self.__dict__.items() if k not in DO_NOT_STORE}
        set_object_from_json(bucket=self._bucket, key=self.key(), data=internal_metadata)
        # self._store.close()

    def key(self) -> str:
        """Returns the Datasource's key

        Returns:
            str: Key for Datasource in object store
        """
        return f"{Datasource._datasource_root}/uuid/{self._uuid}"

    def get_data(self, version: str = "latest") -> xr.Dataset:
        """Get the version of the dataset stored in the zarr store.

        Args:
            version: Version string, e.g. v1, v2
        Returns:
            None
        """
        if version == "latest":
            version = self._latest_version

        self._store.checkout_version(version)

        return self._store.get()

    def bytes_stored(self) -> int:
        """Get the amount of data stored in the zarr store in bytes

        Returns:
            int: Number of bytes
        """
        raise NotImplementedError

    def uuid(self) -> str:
        """Return the UUID of this object

        Returns:
            str: UUID
        """
        return self._uuid

    def metadata(self) -> Dict:
        """Return the metadata of this Datasource

        Returns:
            dict: Metadata of Datasource
        """
        return self._metadata

    def data_type(self) -> str:
        """Returns the data type held by this Datasource

        Returns:
            str: Data type held by Datasource
        """
        return self._data_type

    @property
    def versions(self) -> list[str]:
        """Return a summary of the versions of data stored for
        this Datasource

        Returns:
            dict: Dictionary of versions
        """
        return self._store.versions

    @property
    def latest_version(self) -> str:
        """Return the string of the latest version

        Returns:
            str: Latest version
        """
        return self._latest_version
