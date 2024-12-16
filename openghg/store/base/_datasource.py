from __future__ import annotations
from collections import defaultdict
import warnings
from typing import Any, cast, Literal, TypeVar
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

    def __init__(self, bucket: str, uuid: str | None = None, mode: Literal["r", "rw"] = "rw") -> None:
        from openghg.util import timestamp_now
        from openghg.store.storage import LocalZarrStore

        if uuid is not None:
            key = f"{Datasource._datasource_root}/uuid/{uuid}"
            if exists(bucket=bucket, key=key):
                stored_data = get_object_from_json(bucket=bucket, key=key)
                self.__dict__.update(stored_data)
                self._data_keys: dict[str, list] = defaultdict(list, self._data_keys)
            else:
                raise ObjectStoreError(f"No Datasource with uuid {uuid} found in bucket {bucket}")
        else:
            self._uuid = str(uuid4())
            self._creation_datetime = str(timestamp_now())
            self._metadata: dict[str, str | list | dict] = {}
            self._start_date = None
            self._end_date = None
            self._status: dict | None = None
            self._data_keys = defaultdict(list)
            self._data_type: str = ""
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

    def add_data(
        self,
        metadata: dict,
        data: xr.Dataset,
        data_type: str,
        sort: bool = False,
        drop_duplicates: bool = False,
        skip_keys: list | None = None,
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
        from xarray import concat as xr_concat

        # Extract period associated with data from metadata
        # TODO: May want to add period as a potential data variable so would need to extract from there if needed
        period = self.get_period()

        # Ensure data is in time order
        time_coord = "time"
        new_daterange_str = self.get_representative_daterange_str(dataset=data, period=period)

        if self._latest_version and not new_version:
            version_str = self._latest_version
        else:
            version_str = f"v{str(len(self._data_keys) + 1)}"

        # Ensure daterange strings are independent and do not overlap each other
        # (this can occur due to representative date strings)
        # new_data = self._clip_daterange_label(new_data)

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
        # already_sorted = True

        # If we don't have any data in this Datasource or we have no overlap we'll just add the new data
        if not self._store or not overlapping:
            self._store.add(version=version_str, dataset=data, compressor=compressor, filters=filters)
            date_keys.append(new_daterange_str)

            # if sorted(date_keys) != date_keys:
            #     already_sorted = False
        # Otherwise if we have data already stored in the Datasource
        else:
            # If we have existing data we'll just keep the new data
            # If new_version is True then we create a new version containing just this data
            # If new_version is False then we delete the current data and replace it with just the new data
            if if_exists == "new":
                logger.info("Updating store to include new added data only.")

                if new_version:
                    self._store.add(version=version_str, dataset=data, compressor=compressor, filters=filters)
                else:
                    self._store.update(
                        version=version_str, dataset=data, compressor=compressor, filters=filters
                    )
                # Only save the current daterange string for this version
                date_keys = [new_daterange_str]
            elif if_exists == "combine":
                raise NotImplementedError("Combining data not yet implemented.")
                # We'll copy the data into a temporary store and then combine the data
                memory_store = self._store._copy_to_memorystore(version=self._latest_version)
                existing = xr.open_zarr(store=memory_store, consolidated=True)

                logger.info("Combining overlapping data dateranges")
                # Concatenate datasets along time dimension
                try:
                    combined = xr_concat((existing, data), dim=time_coord)
                except (ValueError, KeyError):
                    # If data variables in the two datasets are not identical,
                    # xr_concat will raise an error
                    dv_ex = set(existing.data_vars.keys())
                    dv_new = set(data.data_vars.keys())

                    # Check difference between datasets and fill any
                    # missing variables with NaN values.
                    dv_not_in_new = dv_ex - dv_new
                    for dv in dv_not_in_new:
                        fill_values = np.zeros(len(data[time_coord])) * np.nan
                        data = data.assign({dv: (time_coord, fill_values)})

                    dv_not_in_ex = dv_new - dv_ex
                    for dv in dv_not_in_ex:
                        fill_values = np.zeros(len(existing[time_coord])) * np.nan
                        existing = existing.assign({dv: (time_coord, fill_values)})

                    # Should now be able to concatenate successfully
                    combined = xr_concat((existing, data), dim=time_coord)

                # TODO: May need to find a way to find period for *last point* rather than *current point*
                # combined_daterange = self.get_dataset_daterange_str(dataset=combined)
                combined_daterange = self.get_representative_daterange_str(dataset=combined, period=period)

                # logger.warning(
                #     f"Dropping duplicates and rechunking data with time chunks of size {time_chunksize} and sorting."
                # )

                if data_type == "footprints":
                    logger.warning("Sorting footprints by time may consume large amounts of memory.")

                logger.debug(
                    "Dropping duplicates, rechunking data and sorting by time variable in add_timed_data."
                )

                combined = (
                    combined.drop_duplicates(dim=time_coord, keep="first")
                    # .chunk({"time": time_chunksize})
                    .sortby(time_coord)
                )

                if new_version:
                    self._store.add(
                        version=version_str,
                        dataset=combined,
                        compressor=compressor,
                        filters=filters,
                    )
                else:
                    self._store.update(
                        version=version_str,
                        dataset=combined,
                        compressor=compressor,
                        filters=filters,
                    )

                date_keys = [combined_daterange]
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

    def add_metadata(self, metadata: dict, skip_keys: list | None = None) -> None:
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

        lowercased: dict = to_lowercase(metadata, skip_keys=skip_keys)
        self._metadata.update(lowercased)

    def get_dataframe_daterange(self, dataframe: DataFrame) -> tuple[Timestamp, Timestamp]:
        """Returns the daterange for the passed DataFrame

        Args:
            dataframe: DataFrame to parse
        Returns:
            tuple (Timestamp, Timestamp): Start and end Timestamps for data
        """
        from openghg.util import timestamp_tzaware
        from pandas import DatetimeIndex

        warnings.warn("This function is deprecated any may be removed", DeprecationWarning)
        if not isinstance(dataframe.index, DatetimeIndex):
            raise TypeError("Only DataFrames with a DatetimeIndex must be passed")

        # Here we want to make the pandas Timestamps timezone aware
        start = timestamp_tzaware(dataframe.first_valid_index())
        end = timestamp_tzaware(dataframe.last_valid_index())

        return start, end

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

    def get_dataset_daterange_str(self, dataset: xr.Dataset) -> str:
        start, end = self.get_dataset_daterange(dataset=dataset)

        # Tidy the string and concatenate them
        start = str(start).replace(" ", "-")
        end = str(end).replace(" ", "-")

        daterange_str: str = start + "_" + end

        return daterange_str

    def get_representative_daterange_str(self, dataset: xr.Dataset, period: str | None = None) -> str:
        """
        Get representative daterange which incorporates any period the data covers.

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

    def clip_daterange(self, end_date: Timestamp, start_date_next: Timestamp) -> Timestamp:
        """
        Clip any end_date greater than the next start date (start_date_next) to be
        1 second less.
        """
        if end_date >= start_date_next:
            end_date = start_date_next - Timedelta(seconds=1)

        return end_date

    def clip_daterange_from_str(self, daterange_str1: str, daterange_str2: str) -> str:
        """
        Ensure the end date of a daterange string is not greater than the start
        date of the next daterange string. Update as needed.
        """
        from openghg.util import create_daterange_str

        start_date_str, end_date_str = daterange_str1.split("_")
        start_date_next_str, _ = daterange_str2.split("_")

        start_date = Timestamp(start_date_str)
        end_date = Timestamp(end_date_str)
        start_date_next = Timestamp(start_date_next_str)

        end_date = self.clip_daterange(end_date, start_date_next)

        daterange_str1_clipped = create_daterange_str(start_date, end_date)

        return daterange_str1_clipped

    def _clip_daterange_label(self, labelled_datasets: dict[str, xr.Dataset]) -> dict[str, xr.Dataset]:
        """
        Check the daterange string labels for the datasets and ensure neighbouring
        date ranges are not overlapping. The daterange string labels will be updated
        as required.

        Args:
            labelled_datasets: Dictionary of datasets labelled by date range strings.
                These are expected to be in time order.
        Returns:
            Dict : Same format as input with labels updated as necessary.
        """

        datestr_labels = list(labelled_datasets.keys())
        num_data_groups = len(datestr_labels)

        labelled_datasets_clipped = {}
        for i in range(num_data_groups):
            daterange_str_1 = datestr_labels[i]
            if i < num_data_groups - 1:
                daterange_str_2 = datestr_labels[i + 1]
                daterange_str = self.clip_daterange_from_str(daterange_str_1, daterange_str_2)
            else:
                daterange_str = daterange_str_1
            dataset = labelled_datasets[daterange_str_1]
            labelled_datasets_clipped[daterange_str] = dataset

        return labelled_datasets_clipped

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

    @staticmethod
    def load_dataset(bucket: str, key: str) -> xr.Dataset:
        """Loads a xarray Dataset from the passed key for creation of a Datasource object

        Data is lazy-loaded because we use `xarray.open_dataset`. This means that the
        file handler for the data file remains open until the data is actually required.

        This improves performance, since we often only update one or two chunks of the dataset
        at a time.

        Args:
            bucket: Bucket containing data
            key: Key for data
        Returns:
            xarray.Dataset: Dataset from NetCDF file
        """

        raise NotImplementedError(
            "Loading of data directly from Datasource no longer supported. Use memory_store to access data stored in zarr store."
        )

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

    def _copy_to_memorystore(self, version: str = "latest") -> dict:
        """Copy the compressed data for a version from the zarr store into memory.
        Most users should use get_data in place of this function as it offers a simpler
        way of retrieving data.

        Copying the compressed data into memory may be useful when exploring xarray and zarr
        functionality.

        Example:
            memory_store = datasource._copy_to_memorystore(version="v1")
            with xr.open_zarr(memory_store, consolidated=True) as ds:
                ...

        Returns:
            Dict: In-memory copy of compressed data
        """
        if not self._data_keys:
            return {}

        if version == "latest":
            version = self._latest_version

        return self._store._copy_to_memorystore(version=version)

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

    def keys_in_daterange(self, start_date: str | Timestamp, end_date: str | Timestamp) -> list[str]:
        """Return the keys for data between the two passed dates

        Args:
            start_date: Start date
            end_date: end date
        Return:
            list: List of keys to data
        """
        data_keys = self._data_keys[self._latest_version]

        return self.key_date_compare(keys=data_keys, start_date=start_date, end_date=end_date)

    def keys_in_daterange_str(self, daterange: str) -> list[str]:
        """Return the keys for data within the specified daterange string

        Args:
            daterange: Daterange string of the form
            2019-01-01T00:00:00_2019-12-31T00:00:00
        Return:
            list: List of keys to data
        """
        from openghg.util import timestamp_tzaware

        split_daterange = daterange.split("_")

        if len(split_daterange) > 2:
            # raise DateError("")
            raise TypeError("Invalid daterange string passed.")

        start_date = timestamp_tzaware(split_daterange[0])
        end_date = timestamp_tzaware(split_daterange[1])

        data_keys = self._data_keys[self._latest_version]

        return self.key_date_compare(keys=data_keys, start_date=start_date, end_date=end_date)

    def key_date_compare(self, keys: list, start_date: Timestamp, end_date: Timestamp) -> list:
        """Returns the keys in the key list that are between the given dates

        Args:
            keys: List of object store keys
            start_date: Start date
            end_date: End date
        Returns:
            list: List of keys
        """
        from openghg.util import timestamp_tzaware

        in_date = []
        for key in keys:
            end_key = key.split("/")[-1]
            dates = end_key.split("_")

            if len(dates) > 2:
                raise ValueError("Invalid date string")

            start_key = timestamp_tzaware(dates[0])
            end_key = timestamp_tzaware(dates[1])

            # For this logic see
            # https://stackoverflow.com/a/325964
            if (start_key <= end_date) and (end_key >= start_date):
                in_date.append(key)

        return in_date

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

    def raw_keys(self) -> dict[str, list]:
        """Returns the raw keys dictionary

        Returns:
            dict: Dictionary of keys
        """
        return self._data_keys

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
