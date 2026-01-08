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
from openghg.util import (
    create_daterange_str,
    get_representative_daterange_str,
    split_daterange_str,
    timestamp_now,
    timestamp_tzaware,
)
from openghg.types import DataOverlapError, ObjectStoreError

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
        from openghg.store.storage import LocalZarrStore

        self._uuid = uuid
        self._creation_datetime = str(timestamp_now())
        self._metadata: dict[str, str | list | dict] = {}
        self._start_date = None
        self._end_date = None
        self._status: dict | None = None
        self._data_keys = defaultdict(list)  # dict mapping version to lists of daterange strings
        self._data_type = data_type
        # Hold information regarding the versions of the data
        self._latest_version: str = ""
        self._timestamps: dict[str, str] = {}

        if mode not in ("r", "rw"):
            raise ValueError("Invalid mode. Please select r or rw.")

        self._mode = mode
        # TODO - add in selection of other store types, this could NetCDF, sparse, whatever
        self._store = LocalZarrStore(bucket=bucket, datasource_uuid=self._uuid, mode=mode)
        # So we know where to write out to
        self._bucket = bucket

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
            "_store",
            "_bucket",
            "_status",
            "_start_date",
            "_end_date",
        }

        internal_metadata = {k: v for k, v in self.__dict__.items() if k not in DO_NOT_STORE}
        set_object_from_json(bucket=self._bucket, key=self.key, data=internal_metadata)
        self._store.close()

    def add(self, data: xr.Dataset, **kwargs) -> None:
        if (period := kwargs.pop("period", None)) is not None:
            self._metadata["period"] = period

        self.add_data(metadata={}, data=data, data_type=self._data_type, **kwargs)

    def get_data(self, version: str = "latest") -> xr.Dataset:
        """Get the version of the dataset stored in the zarr store.

        Args:
            version: Version string, e.g. v1, v2
        Returns:
            None
        """
        if version == "latest":
            version = self._latest_version

        return self._store.get(version=version)

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
        return self._latest_version

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
        return self._store.bytes_stored()

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
        # Ensure data is in time order
        time_coord = "time"
        new_daterange_str = get_representative_daterange_str(dataset=data, period=self.period)

        if self._latest_version and not new_version:
            version_str = self._latest_version
        else:
            version_str = f"v{len(self._data_keys) + 1!s}"

        # Save details of current Datasource status
        self._status = {}

        # We'll use this to store the dates covered by this version of the data
        date_keys = self._data_keys[self._latest_version] if self._data_keys else []

        if sort and drop_duplicates:
            data = data.drop_duplicates(time_coord, keep="first").sortby(time_coord)
        elif sort:
            data = data.sortby(time_coord)
        elif drop_duplicates:
            data = data.drop_duplicates(time_coord, keep="first")

        overlapping = self._store and self._store._vzds._overlap_determiner.has_overlaps(
            data.get_index(self._store._vzds.append_dim)
        )

        # TODO: what does the following comment mean? (BM Jan 2026)
        # We'll only need to sort the new dataset if the data we add comes before the current data

        # If we don't have any data in this Datasource or we have no overlap we'll just add the new data
        if not self._store or not overlapping:
            self._store.add(version=version_str, dataset=data, compressor=compressor, filters=filters)
            date_keys.append(new_daterange_str)
        # Otherwise if we have data already stored in the Datasource
        elif if_exists == "new":
            # If we have existing data we'll just keep the new data
            # If new_version is True then we create a new version containing just this data
            # If new_version is False then we delete the current data and replace it with just the new data
            logger.info("Updating store to include new added data only.")

            if new_version:
                self._store.add(version=version_str, dataset=data, compressor=compressor, filters=filters)
            else:
                self._store.overwrite(
                    version=version_str, dataset=data, compressor=compressor, filters=filters
                )
            # Only save the current daterange string for this version
            date_keys = [new_daterange_str]
        elif if_exists == "combine":
            logger.info("Updating store by combining new data with existing.")
            self._store.update(version=version_str, dataset=data, compressor=compressor, filters=filters)
            date_keys = [get_representative_daterange_str(self.get_data())]
        # If we don't know what (i.e. we've got "auto") to do we'll raise an error
        else:
            # if_exists == "auto" (or at least... not "new" or "combine"), but we already have data
            # and the new data overlaps
            raise DataOverlapError(
                "Unable to add new data, because it overlaps with current data and `if_exists` is set to 'auto'. "
                "To update current data in object store use `if_exists` input (see options in documentation)."
            )

        self._data_type = data_type
        self.add_metadata_key(key="data_type", value=data_type)

        self._status["updates"] = True
        self._status["if_exists"] = if_exists
        self._latest_version = version_str

        # We'll store the daterange for this version of the data and update the latest to the current version
        timestamp_str_now = str(timestamp_now())
        self._data_keys[version_str] = sorted(date_keys)
        self._timestamps[version_str] = timestamp_str_now
        self.add_metadata_key(key="latest_version", value=version_str)
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
        self._store.delete_all()
        self._store.close()
        self._data_keys.clear()
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

        if version not in self._data_keys:
            raise KeyError("Invalid version.")

        self._store.delete_version(version=version)
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
            version = self._latest_version

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

        date_keys = sorted(self._data_keys[self._latest_version])

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

            if version not in self._store._vzds.versions:
                raise ObjectStoreError(f"{version} not found in object store.")

            self._store._vzds.checkout_version(version)
            with xr.open_zarr(self._store._vzds.store, consolidated=True) as ds:
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
