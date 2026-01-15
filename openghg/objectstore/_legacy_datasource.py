from __future__ import annotations
from collections import defaultdict
from typing import Any, cast, Literal
from typing_extensions import Self
from types import TracebackType
import logging
from pandas import Timestamp, Timedelta
import xarray as xr

from openghg.objectstore import exists, get_object_from_json
from openghg.objectstore._local_store import delete_object
from openghg.storage import get_versioned_zarr_directory_store
from openghg.types import DataOverlapError, ObjectStoreError, ZarrStoreError
from openghg.util import (
    create_daterange_str,
    get_representative_daterange_str,
    split_daterange_str,
    timestamp_now,
    timestamp_tzaware,
)
from openghg.util._versioning import next_version

from ._datasource import AbstractDatasource, DatasourceFactory

logger = logging.getLogger("openghg.objectstore")
logger.setLevel(logging.DEBUG)

__all___ = ["Datasource"]


class Datasource(AbstractDatasource[xr.Dataset]):
    """A Datasource holds data relating to a single source.

    For instance, a specific species at a certain height on a specific
    instrument could be a single "Datasource".
    """

    _datasource_root = "datasource"

    def __init__(self, bucket: str, uuid: str, mode: Literal["r", "rw"] = "rw", data_type: str = "") -> None:
        from pathlib import Path

        self._uuid = uuid
        self._creation_datetime = str(timestamp_now())
        self._metadata: dict[str, str | list | dict] = {}
        self._start_date = None
        self._end_date = None
        self._status: dict | None = None
        self._data_keys = defaultdict(list)  # dict mapping version to lists of daterange strings
        self._data_type = data_type
        # Hold information regarding the versions of the data
        self._timestamps: dict[str, str] = {}

        if mode not in ("r", "rw"):
            raise ValueError("Invalid mode. Please select r or rw.")

        self._mode = mode
        self._bucket = bucket

        # Setup versioned zarr store directly
        self._root_store_key = f"data/{uuid}/zarr"
        self._stores_path = Path(bucket, self._root_store_key).expanduser().resolve()
        self._vzds = get_versioned_zarr_directory_store(path=self._stores_path)

        self.update_daterange()

    # Methods to satisfy AbstractDatasource ABC
    @classmethod
    def load(cls, uuid: str, bucket: str, mode: Literal["r", "rw"] = "rw", data_type: str = "") -> Self:
        key = f"{Datasource._datasource_root}/uuid/{uuid}"
        if exists(bucket=bucket, key=key):
            stored_data = get_object_from_json(bucket=bucket, key=key)
        else:
            raise ObjectStoreError(f"No Datasource with uuid {uuid} found in bucket {bucket}")

        ds = cls(bucket, uuid, mode, data_type)
        ds.__dict__.update(stored_data)
        ds._data_keys = defaultdict(list, ds._data_keys)

        return ds

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
            "_vzds",
            "_root_store_key",
            "_stores_path",
            "_bucket",
            "_status",
            "_start_date",
            "_end_date",
        }

        internal_metadata = {k: v for k, v in self.__dict__.items() if k not in DO_NOT_STORE}
        set_object_from_json(bucket=self._bucket, key=self.key, data=internal_metadata)
        # Zarr directory stores do not need to be closed

    def add(self, data: xr.Dataset, **kwargs) -> None:
        if (period := kwargs.pop("period", None)) is not None:
            self._metadata["period"] = period

        self.add_data(metadata={}, data=data, data_type=self._data_type, **kwargs)

    def get_data(self, version: str = "latest") -> xr.Dataset:
        """Get the version of the dataset stored in the zarr store.

        Args:
            version: Version string, e.g. v1, v2
        Returns:
            xr.Dataset: Dataset from the store
        """
        if version == "latest":
            version = self.latest_version
            if not version:
                raise ZarrStoreError("No data versions available")

        try:
            self._vzds.checkout_version(version.lower())
        except ValueError as e:
            raise ZarrStoreError(f"Invalid version: {version}") from e

        return self._vzds.get()

    def delete(self) -> None:
        self.delete_all_data()
        delete_object(bucket=self._bucket, key=self.key)

    # Context manager
    def __enter__(self) -> Datasource:
        return self

    def __exit__(
        self,
        exc_type: BaseException | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            logger.error(msg=f"{exc_type}, {exc_tb}")
        else:
            self.save()

    # properties
    @property
    def start_date(self) -> Timestamp:
        """Start datetime for the data in this Datasource."""
        return self._start_date

    @property
    def end_date(self) -> Timestamp:
        """End datetime for the data in this Datasource."""
        return self._end_date

    @property
    def key(self) -> str:
        """Key for Datasource in object store."""
        return f"{Datasource._datasource_root}/uuid/{self._uuid}"

    @property
    def uuid(self) -> str:
        """UUID of this object."""
        return self._uuid

    @property
    def metadata(self) -> dict:
        """Metadata of this Datasource."""
        return self._metadata

    @property
    def data_type(self) -> str:
        """Data type held by this Datasource."""
        return self._data_type

    @property
    def latest_version(self) -> str:
        """String of the latest version."""
        if not self._vzds.versions:
            return ""

        # versions have form v1, v2, ..., so order them by digit after 'v' to find latest
        return max(self._vzds.versions, key=lambda x: int(x[1:]))

    @property
    def period(self) -> str | None:
        """Period from metadata for creating a pandas Timedelta or DataOffset object."""
        # Extract period associated with data from metadata
        # This will be the "sampling_period" for obs and "time_period" for other
        # TODO: May want to add period as a potential data variable so would need to extract from there if needed
        from openghg.util._metadata_util import get_period

        if "period" not in self._metadata:
            self._metadata["period"] = get_period(self._metadata)

        return cast(str | None, self._metadata["period"])

    @property
    def nbytes(self) -> int:
        """Size of data stored in bytes."""
        return self._vzds.bytes_stored()

    # Methods related storing, getting, deleting data
    def add_data(
        self,
        metadata: dict,
        data: xr.Dataset,
        data_type: str,
        sort: bool = False,
        drop_duplicates: bool = False,
        skip_keys: list | None = None,
        extend_keys: list | None = None,
        new_version: bool = True,
        if_exists: str = "auto",
        compressor: Any | None = None,
        filters: Any | None = None,
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
            extend_keys: Keys to add to to current keys (extend a list), if present.
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
        self.add_metadata(metadata=metadata, skip_keys=skip_keys, extend_keys=extend_keys)

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
        compressor: Any | None = None,
        filters: Any | None = None,
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
        # Check write permissions
        if self._mode == "r":
            raise PermissionError("Cannot modify a read-only datasource")

        if if_exists not in ("auto", "new", "combine"):
            raise ValueError(f"'if_exists' must be 'auto', 'new', or 'combine'; received '{if_exists}'.")

        # Ensure data is in time order
        time_coord = "time"
        new_daterange_str = get_representative_daterange_str(dataset=data, period=self.period)

        # Save details of current Datasource status
        self._status = {}

        # We'll use this to store the dates covered by this version of the data
        latest_version = self.latest_version
        if self._data_keys and latest_version and latest_version in self._data_keys:
            date_keys = self._data_keys[latest_version]
        else:
            if self._data_keys and latest_version and latest_version not in self._data_keys:
                logger.warning(
                    "Datasource metadata and zarr store versions appear inconsistent: "
                    "latest_version=%r not found in _data_keys keys=%r. "
                    "Proceeding with empty date_keys.",
                    latest_version,
                    list(self._data_keys.keys()),
                )
            date_keys = []

        if sort and drop_duplicates:
            data = data.drop_duplicates(time_coord, keep="first").sortby(time_coord)
        elif sort:
            data = data.sortby(time_coord)
        elif drop_duplicates:
            data = data.drop_duplicates(time_coord, keep="first")

        # Update append_dim if necessary
        if self._vzds.append_dim != time_coord:
            logger.warning(f"Updating VersionedZarrStore append dim. to {time_coord}.")
            self._vzds.append_dim = time_coord

        # Update encoding; this will only apply to new variables; existing variables
        # will have their encoding set to match what was previously used by xarray.
        if compressor:
            self._vzds.compressor = compressor
        if filters:
            self._vzds.filters = filters

        # no data stored, so create initial version and insert data
        if not self.latest_version:
            self._vzds.create_version("v1", checkout=True)
            self._vzds.insert(data)
            date_keys.append(new_daterange_str)

        else:
            self._vzds.checkout_version(self.latest_version)  # all updates relative to latest version

            overlapping = self._vzds.overlaps(data)

            if overlapping and if_exists == "auto":
                raise DataOverlapError(
                    "Unable to add new data. Time overlaps with current data, but 'if_exists' is 'auto'. "
                    "To update current data in object store use `if_exists` input (see options in documentation)"
                )

            if new_version:
                # create new version; copy data if using "combine" and data is overlapping
                version_str = next_version(self.latest_version)
                copy_current = (if_exists == "combine") and overlapping
                self._vzds.create_version(version_str, checkout=True, copy_current=copy_current)

            if not overlapping:
                # NOTE: if a new version was created, the current behaviour is to not copy over data
                # from the current version
                self._vzds.insert(data)

                if new_version:
                    # start fresh list of keys
                    date_keys = [new_daterange_str]
                else:
                    date_keys.append(new_daterange_str)

            elif if_exists == "new":
                # If we have existing data we'll just keep the new data
                # If new_version is True then we create a new version containing just this data
                # If new_version is False then we delete the current data and replace it with just the new data
                logger.info("Updating store to include new added data only.")

                if new_version:
                    self._vzds.insert(data)
                else:
                    self._vzds.overwrite(data)

                # only save daterange of new data
                date_keys = [new_daterange_str]

            else:
                # We have: if_exists == "combine", since overlapping is true, and if_exists == "auto", "new"
                # already covered by previous elif and guard clause above.
                # Combine new data with existing data (upsert operation)
                logger.info("Updating store by combining new data with existing.")

                self._vzds.upsert(data)

                # Get the daterange of the combined data
                date_keys = [get_representative_daterange_str(self.get_data())]

        self._data_type = data_type
        self.add_metadata_key(key="data_type", value=data_type)

        self._status["updates"] = True
        self._status["if_exists"] = if_exists

        # We'll store the daterange for this version of the data and update the latest to the current version
        timestamp_str_now = str(timestamp_now())
        self._data_keys[self.latest_version] = sorted(date_keys)
        self._timestamps[self.latest_version] = timestamp_str_now
        self.add_metadata_key(key="latest_version", value=self.latest_version)
        self.add_metadata_key(key="timestamp", value=timestamp_str_now)

        self.update_daterange()
        # Store the start and end date of the most recent data
        start, end = self.daterange()
        self.add_metadata_key(key="start_date", value=str(start))
        self.add_metadata_key(key="end_date", value=str(end))
        # Store the version data, it's less information now and we can then
        # present version data to the users
        self._metadata["versions"] = self._data_keys

        self._last_updated = timestamp_str_now

    def delete_all_data(self) -> None:
        """Delete datasource entirely.

        Deletes the zarr store that contains all the data
        associated with this Datasource, clears out all keys
        stored in this Datasource, and removes the uuid
        from the `data` path.

        Returns:
            None
        """
        if self._mode == "r":
            raise PermissionError("Cannot modify a read-only datasource")

        self._vzds.delete_all_versions()

        if self._stores_path.exists():
            self._stores_path.rmdir()

        self._data_keys.clear()
        self._metadata.clear()
        self._timestamps.clear()

    def delete_version(self, version: str) -> None:
        """Delete a specific version of data.

        Args:
            version: Version string
        Returns:
            None
        """
        if self._mode == "r":
            raise PermissionError("Cannot modify a read-only datasource")

        if version == "latest":
            raise ValueError("Specific version required for deletion.")

        if version not in self._data_keys:
            raise KeyError("Invalid version.")

        try:
            self._vzds.delete_version(version.lower())
        except ValueError as e:
            raise ZarrStoreError(f"Invalid version: {version}") from e
        del self._data_keys[version]
        del self._timestamps[version]

    # Metadata methods
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

    def add_metadata(
        self, metadata: dict, skip_keys: list | None = None, extend_keys: list | None = None
    ) -> None:
        """Add all metadata in the dictionary to this Datasource.
        This will overwrite any previously stored values for keys of the same name.

        Args:
            metadata: Dictionary of metadata
            skip_keys: Keys to not standardise as lowercase
            extend_keys: Keys to add in addition to current keys (extend a list) if present.

        Returns:
            None
        """
        from openghg.util import to_lowercase, merge_dict, merge_and_extend_dict

        try:
            del metadata["object_store"]
        except KeyError:
            pass
        else:
            logger.warning("object_store should not be added to the metadata, removing.")

        if extend_keys is None:
            extend_keys = []

        lowercased: dict = to_lowercase(metadata, skip_keys=skip_keys)
        metadata_to_add = {key: value for key, value in lowercased.items() if key not in extend_keys}

        merged_metadata = merge_dict(
            self._metadata, metadata_to_add, on_overlap="ignore", on_conflict="right"
        )

        # Extend current keys with new values
        metadata_extend = {}
        for key in extend_keys:
            if key in metadata:
                value = metadata[key]
                if isinstance(value, str):
                    value = [value]
                metadata_extend[key] = value

        merged_and_extended_metadata = merge_and_extend_dict(merged_metadata, metadata_extend)

        self._metadata = merged_and_extended_metadata

    # Date range (and "data keys") methods
    def data_keys(self, version: str = "latest") -> list:
        """Returns the dateranges of data covered by a specific version of the data stored.

        Args:
            version: Version of keys to retrieve
        Returns:
            list: List of data keys
        """
        if version == "latest":
            version = self.latest_version

        try:
            keys = self._data_keys[version]
        except KeyError:
            raise KeyError(f"Invalid version, valid versions {list(self._data_keys.keys())}")

        return keys

    def all_data_keys(self) -> dict:
        """Return a summary of the versions of data stored for
        this Datasource

        Returns:
            dict: Dictionary of versions
        """
        return self._data_keys

    def update_daterange(self) -> None:
        """Update the dates stored by this Datasource

        Returns:
            None
        """
        if not self._data_keys:
            return

        date_keys = sorted(self._data_keys[self.latest_version])

        start, _ = split_daterange_str(daterange_str=date_keys[0])
        _, end = split_daterange_str(daterange_str=date_keys[-1])

        self._start_date = start  # type: ignore
        self._end_date = end  # type: ignore

    def daterange(self) -> tuple[Timestamp, Timestamp]:
        """Get the daterange the data in this Datasource covers as tuple
        of start, end datetime objects

        Returns:
            tuple (Timestamp, Timestamp): Start, end timestamps
        """
        if self.start_date is None and self._data_keys is not None:
            self.update_daterange()

        return self.start_date, self.end_date

    def daterange_str(self) -> str:
        """Get the daterange this Datasource covers as a string in
        the form start_end

        Returns:
            str: Daterange covered by this Datasource
        """
        start, end = self.daterange()

        return create_daterange_str(start=start, end=end)

    # Integrity check
    def integrity_check(self) -> None:
        """Checks to ensure all data stored by this Datasource exists in the object store.

        Returns:
            None
        """
        for version, dateranges in self._data_keys.items():
            start_date, _ = split_daterange_str(daterange_str=dateranges[0])
            _, end_date = split_daterange_str(daterange_str=dateranges[-1])

            if version not in self._vzds.versions:
                raise ObjectStoreError(f"{version} not found in object store.")

            self._vzds.checkout_version(version)
            with xr.open_zarr(self._vzds.store, consolidated=True) as ds:
                if ds.time.size == 1:
                    start_keys = timestamp_tzaware(start_date)
                    start_data = timestamp_tzaware(ds.time[0].values)

                    assert start_keys.year == start_data.year

                    continue

                start_keys = timestamp_tzaware(start_date)
                start_data = timestamp_tzaware(ds.time[0].values)

                if abs(start_keys - start_data) > Timedelta(minutes=1):
                    raise ValueError(
                        f"Timestamp mismatch between expected ({start_keys}) and stored {start_data}"
                    )

                end_keys = timestamp_tzaware(end_date)
                end_data = timestamp_tzaware(ds.time[-1].values)

                if abs(end_keys - end_data) > Timedelta(minutes=1):
                    raise ValueError(
                        f"Timestamp mismatch between expected ({end_keys}) and stored {end_data}"
                    )


def get_legacy_datasource_factory(
    bucket: str,
    data_type: str,
    new_kwargs: dict | None = None,
    load_kwargs: dict | None = None,
    **kwargs: Any,
) -> DatasourceFactory[Datasource]:
    """Create DatasourceFactory for legacy Datasource class.

    Args:
        bucket: bucket of object store
        data_type: data type for saved data
        new_kwargs: keyword args for creating new Datasources (e.g. `mode`)
        load_kwargs: keyword args for loading existing Datasources (e.g. `mode`)
        kwargs: keyword args to add to both new_kwargs and load_kwargs

    Returns:
        DatasourceFactory for creating/loading Datasources from specified
        bucket.

    """
    kwargs_to_add = {"bucket": bucket, "data_type": data_type}
    kwargs_to_add.update(kwargs)

    new_kwargs = new_kwargs or {}
    new_kwargs.update(kwargs_to_add)

    load_kwargs = load_kwargs or {}
    load_kwargs.update(kwargs_to_add)

    return DatasourceFactory[Datasource](Datasource, new_kwargs, load_kwargs)
