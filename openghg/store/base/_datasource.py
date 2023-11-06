from collections import defaultdict
import warnings

# import re
from typing import Any, DefaultDict, Dict, List, Optional, Tuple, Type, TypeVar, Union
from types import TracebackType
import logging
import numpy as np
from pandas import DataFrame, Timestamp, Timedelta
import xarray as xr
from uuid import uuid4
from openghg.objectstore import exists, get_object_from_json, delete_objects
from openghg.store.spec import define_data_types
from openghg.types import DataOverlapError, ObjectStoreError


logger = logging.getLogger("openghg.store.base")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

dataKeyType = DefaultDict[str, Dict[str, Dict[str, str]]]

__all___ = ["Datasource"]

T = TypeVar("T", bound="Datasource")


class Datasource:
    """A Datasource holds data relating to a single source, such as a specific species
    at a certain height on a specific instrument
    """

    _datasource_root = "datasource"
    _data_root = "data"

    def __init__(self, bucket: str, uuid: Optional[str] = None, new_version: bool = True) -> None:
        from openghg.util import timestamp_now
        from openghg.store.base import LocalZarrStore

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
            self._metadata: Dict[str, Union[str, Dict]] = {}
            self._start_date = None
            self._end_date = None
            self._status: Optional[Dict] = None
            # This dictionary stored the keys for each version of data added
            # data_key = d._data_keys[version]["keys"][date_key]
            self._data_keys: dataKeyType = defaultdict(dict)
            self._data_type: str = ""
            # Hold information regarding the versions of the data
            self._latest_version: str = ""

        # TODO - zarr - add type of stores in here for mypy
        self._zarr_store = LocalZarrStore(bucket=bucket, datasource_uuid=self._uuid)
        # So we know where to write out to
        self._bucket = bucket
        # Should we create a new version for the data we're adding
        self._new_version = new_version

        self.update_daterange()

    def __enter__(self):
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
        metadata: Dict,
        data: xr.Dataset,
        data_type: str,
        sort: bool,
        drop_duplicates: bool,
        skip_keys: Optional[List] = None,
        if_exists: str = "auto",
        compressor: Optional[Any] = None,
    ) -> None:
        """Add data to this Datasource and segment the data by size.
        The data is stored as a tuple of the data and the daterange it covers.

        Args:
            metadata: Metadata on the data for this Datasource
            data: xarray.Dataset
            data_type: Type of data, one of ["surface", "emissions", "met", "footprints", "eulerian_model"].
            sort: Sort data in time dimension
            drop_duplicates: Drop duplicate timestamps, keeping the first value
            skip_keys: Keys to not standardise as lowercase
            if_exists: What to do if existing data is present.
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - creates new version with just new data
                - "combine" - replace and insert new data into current timeseries
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
                if_exists=if_exists,
                compressor=compressor,
            )
        else:
            raise NotImplementedError()

    def _get_version_str(self) -> str:
        """Get the version string needed to add data to this Datasource, depending on the
        parameters given. Should be given new_version, if_exists etc
        """
        # Add data to same version as previous unless new_version is True
        if self._latest_version and not self._new_version:
            version_str = self._latest_version
        else:
            version_str = f"v{str(len(self._data_keys))}"

        return version_str

    def add_timed_data(
        self,
        data: xr.Dataset,
        data_type: str,
        sort: bool,
        drop_duplicates: bool,
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
            if_exists: What to do if existing data is present.
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - creates new version with just new data
                - "combine" - replace and insert new data into current timeseries
            compressor: Compressor to use when adding data to the zarr store
            filters: Filters to apply to data when adding to the zarr store
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

        new_data = {self.get_representative_daterange_str(data, period=period): data}
        daterange_str = self.get_representative_daterange_str(dataset=data, period=period)

        # NOTE - added version string here as we add data to the zarr store in this function
        version_str = self._get_version_str()

        # TODO - Check the hash of this data and compare it to our stored hashes
        # this means Dataset hash and not the file hash

        # Ensure daterange strings are independent and do not overlap each other
        # (this can occur due to representative date strings)
        new_data = self._clip_daterange_label(new_data)

        # Save details of current Datasource status
        self._status = {}

        # We'll use this to store the dates covered by this version of the data
        date_keys = self._data_keys[self._latest_version]["keys"] if self._data_keys else []

        # TODO - test to see if this chaining does help
        if sort and drop_duplicates:
            data = data.drop_duplicates(time_coord, keep="first").sortby(time_coord)
        elif sort:
            data = data.sortby(time_coord)
        elif drop_duplicates:
            data = data.drop_duplicates(time_coord, keep="first")

        # If we have data already stored in the Datasource
        if date_keys:
            # Check if we have any overlap
            overlapping = []
            for existing_daterange in date_keys:
                if daterange_overlap(daterange_a=existing_daterange, daterange_b=daterange_str):
                    overlapping.append((existing_daterange, daterange_str))

            self._status["current_data"] = True
            self._status["overlapping"] = overlapping

            if if_exists == "new":
                # Remove all current data on Datasource and add new data
                # self._data is a dictionary containing datestr: Dataset values
                logger.info("Updating store to include new added data only.")
                # Add this to the memory store and move on
                self._zarr_store.add(key=daterange_str, version=version_str, dataset=data)
                # We only want this key for a new version
                date_keys = [daterange_str]
            elif overlapping:
                if if_exists == "combine":
                    combined_datasets = {}
                    for existing_daterange, new_daterange in overlapping:
                        ex = self._zarr_store.pop(key=existing_daterange, version=self._latest_version)
                        new = new_data.pop(new_daterange)
                        # new = data
                        logger.info("Combining overlapping data dateranges")

                        # What's another way of storing this data?
                        # Concatenate datasets along time dimension
                        try:
                            # TODO - how to lazy concatenate?
                            combined = xr_concat((ex, new), dim=time_coord)
                        except (ValueError, KeyError):
                            # If data variables in the two datasets are not identical,
                            # xr_concat will raise an error
                            dv_ex = set(ex.data_vars.keys())
                            dv_new = set(new.data_vars.keys())

                            # Check difference between datasets and fill any
                            # missing variables with NaN values.
                            dv_not_in_new = dv_ex - dv_new
                            for dv in dv_not_in_new:
                                fill_values = np.zeros(len(new[time_coord])) * np.nan
                                new = new.assign({dv: (time_coord, fill_values)})

                            dv_not_in_ex = dv_new - dv_ex
                            for dv in dv_not_in_ex:
                                fill_values = np.zeros(len(ex[time_coord])) * np.nan
                                ex = ex.assign({dv: (time_coord, fill_values)})

                            # Should now be able to concatenate successfully
                            combined = xr_concat((ex, new), dim=time_coord)

                        # TODO - zarr/dask - is there a better way of doing this?
                        # We sorted and drop the dupes
                        if sort and drop_duplicates:
                            combined = combined.drop_duplicates(time_coord, keep="first").sortby(time_coord)
                        elif sort:
                            combined = combined.sortby(time_coord)
                        elif drop_duplicates:
                            combined = combined.drop_duplicates(time_coord, keep="first")

                        # TODO: May need to find a way to find period for *last point* rather than *current point*
                        # combined_daterange = self.get_dataset_daterange_str(dataset=combined)
                        combined_daterange = self.get_representative_daterange_str(
                            dataset=combined, period=period
                        )

                        combined_datasets[combined_daterange] = combined

                    # Checking for overlapping date range strings in combined
                    # data and clipping the labels as necessary.
                    combined_datasets = self._clip_daterange_label(combined_datasets)

                    for key, dataset in combined_datasets.items():
                        # chunks = get_chunks(data_type=data_type)
                        # logger.info(f"Rechunking {data_type} data using: {chunks}")
                        # dataset = dataset.chunk(chunks)
                        self._zarr_store.add(
                            key=key,
                            version=version_str,
                            dataset=dataset,
                            compressor=compressor,
                            filters=filters,
                        )

                    # Store the updated dateranges
                    date_keys.extend(combined_datasets.keys())
                    # self._delete_version = True
                else:
                    date_chunk_str = ""
                    for existing_daterange, new_daterange in overlapping:
                        date_chunk_str += f" - current: {existing_daterange}; new: {new_daterange}\n"
                    raise DataOverlapError(
                        f"Unable to add new data. Time overlaps with current data:\n{date_chunk_str}"
                        f"To update current data in object store use `if_exists` input (see options in documentation)"
                    )
            else:
                date_keys.append(daterange_str)
                self._zarr_store.add(
                    key=daterange_str,
                    version=version_str,
                    dataset=data,
                    compressor=compressor,
                    filters=filters,
                )
        else:
            date_keys.append(daterange_str)
            self._zarr_store.add(
                key=daterange_str, version=version_str, dataset=data, compressor=compressor, filters=filters
            )

            self._status["current_data"] = False
            self._status["overlapping"] = False

        self._data_type = data_type
        self.add_metadata_key(key="data_type", value=data_type)

        self._status["updates"] = True
        self._status["if_exists"] = if_exists
        self._latest_version = version_str

        # We'll store the daterange for this version of the data and update the latest to the current version
        timestamp_str_now = str(timestamp_now())
        self._data_keys[version_str]["keys"] = date_keys
        self._data_keys[version_str]["timestamp"] = timestamp_str_now
        self.add_metadata_key(key="latest_version", value=version_str)
        self.add_metadata_key(key="timestamp", value=timestamp_str_now)

        self.update_daterange()
        # Store the start and end date of the most recent data
        start, end = self.daterange()
        self.add_metadata_key(key="start_date", value=str(start))
        self.add_metadata_key(key="end_date", value=str(end))
        # Store the version data, it's less information now and we can then present version data to the users
        self._metadata["versions"] = self._data_keys

        self._latest_version = version_str
        self._last_updated = timestamp_str_now

    def delete_all_data(self) -> None:
        """Delete the zarr stor that contains all the data
        associated with this Datasource

        Returns:
            None
        """
        self._zarr_store.delete_all()
        self._zarr_store.close()
        delete_objects(bucket=self._bucket, prefix=self._zarr_store.store_key())

    def delete_data(self, version: str, keys: List) -> None:
        """Delete specific keys

        Args:
            bucket: Bucket containing data
            keys: List of keys to delete
        Returns:
            None
        """
        for key in keys:
            self._zarr_store.delete(key=key, version=version)

    def delete_version(self, version: str) -> None:
        """Delete a specific version of data.

        Args:
            bucket: Bucket containing data
            version: Version string
        Returns:
            None
        """
        if version not in self._data_keys:
            raise KeyError("Invalid version.")

        self.delete_data(version=version, keys=self._data_keys[version]["keys"])

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

    def get_dataframe_daterange(self, dataframe: DataFrame) -> Tuple[Timestamp, Timestamp]:
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

    def get_dataset_daterange(self, dataset: xr.Dataset) -> Tuple[Timestamp, Timestamp]:
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

    def get_representative_daterange_str(self, dataset: xr.Dataset, period: Optional[str] = None) -> str:
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

    def _clip_daterange_label(self, labelled_datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
        """
        Check the daterange string labels for the datasets and ensure neighbouring
        date ranges are not overlapping. The daterange string labels will be updated
        as required.

        Args:
            labelled_datasets : Dictionary of datasets labelled by date range strings.
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

    def get_period(self) -> Optional[str]:
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
            value = metadata.get(attr)
            if value is not None:
                # For sampling period data, expect this to be in seconds
                if attr == "sampling_period":
                    if value.endswith("s"):  # Check if str includes "s"
                        period: Optional[str] = value
                    else:
                        try:
                            value_num: Optional[int] = int(value)
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
        from openghg.objectstore import get_object_data_path

        file_path = get_object_data_path(bucket, key)
        # This works
        # TODO - this should be changed to use open_dataset really
        # but open_dataset causes issues due to internal xarray caching when
        # we try and write out to the same file
        return xr.load_dataset(file_path)
        # This does not
        # return xr.open_dataset(file_path)

    def define_version_key(self, version: str) -> str:
        """Define key for version on Datasource"""
        datasource_key = self.key()
        version_key = f"{datasource_key}/{version}"
        return version_key

    def define_data_key(self, label: str, version: str) -> str:
        """Define data key for internally split data element
        (e.g. by daterange) on Datasource
        """
        version_key = self.define_version_key(version)
        data_key = f"{version_key}/{label}"
        return data_key

    def define_backup_version(self, version: str) -> str:
        """Define backup name for version folder"""
        version_backup = f"{version}_backup"
        return version_backup

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
            "_memory_store",
            "_zarr_store",
            "_bucket",
            "_status",
            "_new_version",
            "_start_date",
            "_end_date",
        }

        internal_metadata = {k: v for k, v in self.__dict__.items() if k not in DO_NOT_STORE}
        set_object_from_json(bucket=self._bucket, key=self.key(), data=internal_metadata)

    def get_latest_datekeys(self) -> List[str]:
        """Get the latest date keys for this Datasource

        Returns:
            list: List of date keys
        """
        return list(self._data_keys[self._latest_version]["keys"].keys())

    def key(self) -> str:
        """Returns the Datasource's key

        Returns:
            str: Key for Datasource in object store
        """
        return f"{Datasource._datasource_root}/uuid/{self._uuid}"

    def data(self) -> Dict:
        """Get the data stored in this Datasource

        Returns:
            dict: Dictionary of data keyed by daterange
        """
        raise NotImplementedError("Data is now stored in the zarr store")
        return self._zarr_store
        # if not self._data:
        #     for date_key in self._data_keys["latest"]["keys"]:
        #         data_key = self._data_keys["latest"]["keys"][date_key]
        #         self._data[date_key] = Datasource.load_dataset(bucket=self._bucket, key=data_key)

        # return self._data

    def update_daterange(self) -> None:
        """Update the dates stored by this Datasource

        Returns:
            None
        """
        from openghg.util import split_daterange_str

        if not self._data_keys:
            return

        date_keys = sorted(self._data_keys[self._latest_version]["keys"])

        start, _ = split_daterange_str(daterange_str=date_keys[0])
        _, end = split_daterange_str(daterange_str=date_keys[-1])

        self._start_date = start  # type: ignore
        self._end_date = end  # type: ignore

    def daterange(self) -> Tuple[Timestamp, Timestamp]:
        """Get the daterange the data in this Datasource covers as tuple
        of start, end datetime objects

        Returns:
            tuple (Timestamp, Timestamp): Start, end timestamps
        """
        if self._start_date is None and self._data is not None:
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

    def search_metadata_old(
        self,
        search_terms: Union[str, List[str]],
        start_date: Optional[Timestamp] = None,
        end_date: Optional[Timestamp] = None,
        find_all: Optional[bool] = False,
    ) -> bool:
        """Search the values of the metadata of this Datasource for search terms

        Args:
            search_term: String or list of strings to search for in metadata
            find_all: If True all search terms must be matched
        Returns:
            bool: True if found else False
        """
        from warnings import warn

        warn("This function will be removed in a future release", DeprecationWarning)

        if start_date is not None and end_date is not None:
            if not self.in_daterange(start_date=start_date, end_date=end_date):
                return False

        if not isinstance(search_terms, list):
            search_terms = [search_terms]

        search_terms = [s.lower() for s in search_terms if s is not None]

        results = {}

        def search_recurse(term: str, data: Dict) -> None:
            for v in data.values():
                if v == term:
                    results[term] = True
                elif isinstance(v, dict):
                    search_recurse(term, v)

        for term in search_terms:
            search_recurse(term, self._metadata)

        # If we want all the terms to match these should be the same length
        if find_all:
            return len(results) == len(search_terms)
        # Otherwise there should be at least a True in results
        else:
            return len(results) > 0

    def search_metadata(self, find_all: Optional[bool] = True, **kwargs: str) -> bool:
        """Search the metadata for any available keyword

        Args:
            find_all: If True all arguments must be matched
        Keyword Arguments:
            Keyword arguments passed will be checked against the metadata of the Datasource
        Returns:
            bool: True if some/all parameters matched
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        if start_date is not None and end_date is not None:
            if not self.in_daterange(start_date=start_date, end_date=end_date):
                return False

        # Now we've checked the dates we can remove them as it's unlikely a comparison below
        # will match the dates exactly
        try:
            del kwargs["start_date"]
            del kwargs["end_date"]
        except KeyError:
            pass

        results = []
        for key, value in kwargs.items():
            try:
                # Here we want to check if it's a list and if so iterate over it
                if isinstance(value, (list, tuple)):
                    for val in value:
                        val = str(val).lower()
                        if self._metadata[key.lower()] == val:
                            results.append(val)
                else:
                    value = str(value).lower()
                    if self._metadata[key.lower()] == value:
                        results.append(value)
            except KeyError:
                pass

        # If we want all the terms to match these should be the same length
        if find_all and not len(kwargs.keys()) == len(results):
            return False

        if results:
            return True
        else:
            return False

    def in_daterange(self, start_date: Union[str, Timestamp], end_date: Union[str, Timestamp]) -> bool:
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

    def keys_in_daterange(
        self, start_date: Union[str, Timestamp], end_date: Union[str, Timestamp]
    ) -> List[str]:
        """Return the keys for data between the two passed dates

        Args:
            start_date: Start date
            end_date: end date
        Return:
            list: List of keys to data
        """
        data_keys = self._data_keys[self._latest_version]["keys"]

        return self.key_date_compare(keys=data_keys, start_date=start_date, end_date=end_date)

    def keys_in_daterange_str(self, daterange: str) -> List[str]:
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

        data_keys = self._data_keys[self._latest_version]["keys"]

        return self.key_date_compare(keys=data_keys, start_date=start_date, end_date=end_date)

    def key_date_compare(self, keys: Dict[str, str], start_date: Timestamp, end_date: Timestamp) -> List:
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
        for key, data_key in keys.items():
            end_key = key.split("/")[-1]
            dates = end_key.split("_")

            if len(dates) > 2:
                raise ValueError("Invalid date string")

            start_key = timestamp_tzaware(dates[0])
            end_key = timestamp_tzaware(dates[1])

            # For this logic see
            # https://stackoverflow.com/a/325964
            if (start_key <= end_date) and (end_key >= start_date):
                in_date.append(data_key)

        return in_date

    def species(self) -> str:
        """Returns the species of this Datasource

        Returns:
            str: Species of this Datasource
        """
        return self._metadata["species"]

    def inlet(self) -> str:
        """Returns the inlet height of this Datasource

        Returns:
            str: Inlet height of this Datasource
        """
        return self._metadata["inlet"]

    def site(self) -> str:
        """Return the site name

        Returns:
            str: Site name
        """
        return self._metadata.get("site", "NA")

    def instrument(self) -> str:
        """Return the instrument name

        Returns:
            str: Instrument name
        """
        return self._metadata.get("instrument", "NA")

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

    def raw_keys(self) -> dataKeyType:
        """Returns the raw keys dictionary

        Returns:
            dict: Dictionary of keys
        """
        return self._data_keys

    def data_keys(self, version: str = "latest") -> List:
        """Returns the object store keys where data related
        to this Datasource is stored

        Args:
            version: Version of keys to retrieve
            return_all: Return all data keys
        Returns:
            list: List of data keys
        """
        if version == "latest":
            version = self._latest_version

        try:
            keys = self._data_keys[version]["keys"]
        except KeyError:
            raise KeyError(f"Invalid version, valid versions {list(self._data_keys.keys())}")

        return keys

    def versions(self) -> Dict:
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
        from openghg.objectstore import exists

        for version, key_data in self._data_keys.items():
            for key in key_data["keys"].values():
                if not exists(bucket=self._bucket, key=key):
                    raise ObjectStoreError(
                        f"The key {key} for version {version} of this Datasource does not exist in the object store {self._bucket}"
                    )
