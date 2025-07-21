from __future__ import annotations
from collections import defaultdict
from typing import Any, cast, Literal, TypeVar
from typing_extensions import Self
from types import TracebackType
import logging
from pandas import Timestamp, Timedelta
import xarray as xr

from openghg.objectstore import exists, get_object_from_json
from openghg.objectstore._local_store import delete_object
from openghg.util import split_daterange_str, timestamp_tzaware
from openghg.types import DataOverlapError, ObjectStoreError

from ._datasource import AbstractDatasource, DatasourceFactory

logger = logging.getLogger("openghg.objectstore")
logger.setLevel(logging.DEBUG)

__all___ = ["Datasource"]

T = TypeVar("T", bound="Datasource")


class Datasource(AbstractDatasource[xr.Dataset]):
    """A Datasource holds data relating to a single source, such as a specific species
    at a certain height on a specific instrument
    """

    _datasource_root = "datasource"

    def __init__(self, bucket: str, uuid: str, mode: Literal["r", "rw"] = "rw", data_type: str = "") -> None:
        from openghg.util import timestamp_now
        from openghg.store.storage import LocalZarrStore

        self._uuid = uuid
        self._creation_datetime = str(timestamp_now())
        self._metadata: dict[str, str | list | dict] = {}
        self._start_date = None
        self._end_date = None
        self._status: dict | None = None
        self._data_keys = defaultdict(list)
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
        print("Classmethod data keys", ds._data_keys)
        return ds

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

    def start_date(self) -> Timestamp:
        """Returns the starting datetime for the data in this Datasource

        Returns:
            Timestamp: Timestamp for start of data
        """
        return self._start_date

    def end_date(self) -> Timestamp:
        """Returns the end datetime for the data in this Datasource

        Returns:
            Timestamp: Timestamp for end of data
        """
        return self._end_date

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

    def add(self, data: xr.Dataset, **kwargs) -> None:
        self.add_data(metadata={}, data=data, data_type=self._data_type, **kwargs)

    def delete(self) -> None:
        self.delete_all_data()
        delete_object(bucket=self._bucket, key=self.key())

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
        from openghg.util import daterange_overlap, timestamp_now

        # Extract period associated with data from metadata
        # TODO: May want to add period as a potential data variable so would need to extract from there if needed
        period = self.get_period()

        # Ensure data is in time order
        time_coord = "time"
        new_daterange_str = self.get_representative_daterange_str(dataset=data, period=period)

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

        # We'll only do a concat if we actually have overlapping data
        # Othwerwise we'll just add the new data
        overlapping = [
            new_daterange_str
            for existing in date_keys
            if daterange_overlap(daterange_a=existing, daterange_b=new_daterange_str)
        ]

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
                self._store.overwrite(version=version_str, dataset=data, compressor=compressor, filters=filters)
            # Only save the current daterange string for this version
            date_keys = [new_daterange_str]
        elif if_exists == "combine":
            raise NotImplementedError("Combining data not yet implemented.")
        # If we don't know what (i.e. we've got "auto") to do we'll raise an error
        else:
            date_chunk_str = f"Current: {date_keys}; new: {overlapping}\n"
            raise DataOverlapError(
                f"Unable to add new data. Time overlaps with current data:\n{date_chunk_str}"
                f"To update current data in object store use `if_exists` input (see options in documentation)"
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

    def get_dataset_daterange(self, dataset: xr.Dataset) -> tuple[Timestamp, Timestamp]:
        """Get the daterange for the passed Dataset

        Args:
            dataset (xarray.DataSet): Dataset to parse
        Returns:
            tuple (Timestamp, Timestamp): Start and end datetimes for DataSet

        """
        from openghg.util import timestamp_tzaware

        try:
            start = timestamp_tzaware(dataset.time.min().values)
            end = timestamp_tzaware(dataset.time.max().values)

            return start, end
        except AttributeError:
            raise AttributeError("This dataset does not have a time attribute, unable to read date range")

    def get_representative_daterange_str(self, dataset: xr.Dataset, period: str | None = None) -> str:
        """Get representative daterange which incorporates any period the data covers.

        A representative daterange covers the start - end time + any additional period that is covered
        by each time point. The start and end times can be extracted from the input dataset and
        any supplied period used to extend the end of the date range to cover the representative period.

        If there is only one time point (i.e. start and end datetimes are the same) and no period is
        supplied 1 additional second will be added to ensure these values are not identical.

        Args:
            dataset: Data containing (at least) a time dimension. Used to extract start and end datetimes.
            period: Value representing a time period e.g. "12H", "1AS" "3MS". Should be suitable for
                creation of a pandas Timedelta or DataOffset object.

        Returns:
            str : Date string covering representative date range e.g. "YYYY-MM-DD hh:mm:ss_YYYY-MM-DD hh:mm:ss"
        """
        from openghg.util import create_daterange_str, relative_time_offset
        from pandas import Timedelta

        # Extract start and end dates from grouped data
        start_date, end_date = self.get_dataset_daterange(dataset)

        # If period is defined add this to the end date
        # This ensure start-end range includes time period covered by data
        if period is not None:
            period_td = relative_time_offset(period=period)
            end_date = (
                end_date + period_td - Timedelta(seconds=1)
            )  # Subtract 1 second to make this exclusive end.

        # If start and end times are identical add 1 second to ensure the range duration is > 0 seconds
        if start_date == end_date:
            end_date += Timedelta(seconds=1)

        daterange_str = create_daterange_str(start=start_date, end=end_date)

        return daterange_str

    def get_period(self) -> str | None:
        """Extract period value from metadata. This expects keywords of either "sampling_period" (observation data) or
        "time_period" (derived or ancillary data). If neither keyword is found, None is returned.

        This is a suitable format to use to create a pandas Timedelta or DataOffset object.

        Returns:
            str or None: time period in the form of number and time unit e.g. "12s" if found in metadata, else None
        """
        # Extract period associated with data from metadata
        # This will be the "sampling_period" for obs and "time_period" for other
        metadata = self._metadata

        time_period_attrs = ["sampling_period", "time_period"]
        for attr in time_period_attrs:
            # These will always be strings
            value = cast(str, metadata.get(attr))
            if value is not None:
                # For sampling period data, expect this to be in seconds
                if attr == "sampling_period":
                    if value.endswith("s"):  # Check if str includes "s"
                        period: str | None = value
                    else:
                        try:
                            value_num: int | None = int(value)
                        except ValueError:
                            try:
                                value_num = int(float(value))
                            except ValueError:
                                value_num = None
                                continue
                        period = f"{value_num}s"
                else:
                    # Expect period data to include value and time unit
                    period = value

                break
        else:
            period = None

        return period

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
        set_object_from_json(bucket=self._bucket, key=self.key(), data=internal_metadata)
        self._store.close()

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

        return self._store.get(version=version)

    def bytes_stored(self) -> int:
        """Get the amount of data stored in the zarr store in bytes

        Returns:
            int: Number of bytes
        """
        return self._store.bytes_stored()

    def update_daterange(self) -> None:
        """Update the dates stored by this Datasource

        Returns:
            None
        """
        from openghg.util import split_daterange_str

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
        if self._start_date is None and self._data_keys is not None:
            self.update_daterange()

        return self._start_date, self._end_date

    def daterange_str(self) -> str:
        """Get the daterange this Datasource covers as a string in
        the form start_end

        Returns:
            str: Daterange covered by this Datasource
        """
        from openghg.util import create_daterange_str

        start, end = self.daterange()

        return create_daterange_str(start=start, end=end)

    def in_daterange(self, start_date: str | Timestamp, end_date: str | Timestamp) -> bool:
        """Check if the data contained within this Datasource overlaps with the
        dates given.

        Args:
            start: Start datetime
            end: End datetime
        Returns:
            bool: True if overlap
        """
        from openghg.util import in_daterange as _in_daterange
        from openghg.util import timestamp_tzaware

        start_date = timestamp_tzaware(start_date)
        end_date = timestamp_tzaware(end_date)

        return _in_daterange(
            start_a=start_date, end_a=end_date, start_b=self._start_date, end_b=self._end_date
        )

    def uuid(self) -> str:
        """Return the UUID of this object

        Returns:
            str: UUID
        """
        return self._uuid

    def metadata(self) -> dict:
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

    def latest_version(self) -> str:
        """Return the string of the latest version

        Returns:
            str: Latest version
        """
        return self._latest_version

    def integrity_check(self) -> None:
        """Checks to ensure all data stored by this Datasource exists in the object store.

        Returns:
            None
        """
        for version, dateranges in self._data_keys.items():
            start_date, _ = split_daterange_str(daterange_str=dateranges[0])
            _, end_date = split_daterange_str(daterange_str=dateranges[-1])

            if version not in self._store._stores:
                raise ObjectStoreError(f"{version} not found in object store.")

            with xr.open_zarr(self._store._stores[version], consolidated=True) as ds:
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
