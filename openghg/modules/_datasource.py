from pandas import DataFrame, Timestamp
from typing import Dict, List, Optional, Tuple, Union
from xarray import Dataset

__all___ = ["Datasource"]


class Datasource:
    """A Datasource holds data relating to a single source, such as a specific species
    at a certain height on a specific instrument
    """

    _datasource_root = "datasource"
    _datavalues_root = "values"
    _data_root = "data"

    def __init__(self):
        from Acquire.ObjectStore import get_datetime_now
        from collections import defaultdict
        from uuid import uuid4

        self._uuid = str(uuid4())
        self._creation_datetime = get_datetime_now()
        self._metadata = {}
        # Dictionary keyed by daterange of data in each Dataset
        self._data = {}

        self._start_date = None
        self._end_date = None

        self._stored = False
        # This dictionary stored the keys for each version of data uploaded
        self._data_keys = defaultdict(dict)
        self._data_type = None
        # Hold information regarding the versions of the data
        # Currently unused
        self._latest_version = None
        self._versions = {}
        # A rank of 10 is unset, 1 is a primary source, 2 secondary
        self._rank = defaultdict(list)

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

    def add_data_dataframe(
        self, metadata: Dict, data: DataFrame, data_type: Optional[str] = None, overwrite: Optional[bool] = False
    ) -> None:
        """Add data to this Datasource and segment the data by size.
        The data is stored as a tuple of the data and the daterange it covers.

        Args:
            metadata: Metadata on the data for this Datasource
            data: Measurement data
            data_type: Placeholder for combination of this fn
            with add_footprint_data in the future
            overwrite: Overwrite existing data
            None
        """
        from pandas import Grouper
        from openghg.processing import get_split_frequency

        # Ensure metadata values are all lowercase
        metadata = {k: v.lower() for k, v in metadata.items()}
        self._metadata.update(metadata)

        if self._data:
            # Exisiting data in Datsource
            start_data, end_data = self.daterange()
            # This is the data that we may want to add to the Datasource
            start_new, end_new = self.get_dataframe_daterange(data)

            # Check if there's overlap of data
            if start_new >= start_data and end_new <= end_data and overwrite is False:
                raise ValueError("The provided data overlaps dates covered by existing data")

        # Need to check here if we've seen this data before
        freq = get_split_frequency(data)
        # Split into sections by splitting frequency
        group = data.groupby(Grouper(freq=freq))
        # Create a list tuples of the split dataframe and the daterange it covers
        # As some (years, months, weeks) may be empty we don't want those dataframes
        self._data = [(g, self.get_dataframe_daterange(g)) for _, g in group if len(g) > 0]
        self.add_metadata_key(key="data_type", value="timeseries")
        self._data_type = "timeseries"
        # Use daterange() to update the recorded values
        self.update_daterange()

    def add_data(self, metadata: Dict, data: Dataset, data_type: str, overwrite: Optional[bool] = False) -> None:
        """Add data to this Datasource and segment the data by size.
        The data is stored as a tuple of the data and the daterange it covers.

        Args:
            metadata: Metadata on the data for this Datasource
            data: xarray.Dataset
            data_type: Type of data, one of ["timeseries", "emissions", "met", "footprint"].
            overwrite: Overwrite existing data
        Returns:
            None
        """
        data_types = ["timeseries", "emissions", "met", "footprint"]

        data_type = data_type.lower()
        if data_type not in data_types:
            raise TypeError(f"Incorrect data type selected. Please select from one of {data_types}")

        self.add_metadata(metadata=metadata)

        if data_type == "timeseries":
            return self.add_timeseries_data(data=data)
        elif data_type == "footprint":
            return self.add_footprint_data(data=data)
        elif data_type == "emissions":
            return self.add_emissions_data(data=data)
        elif data_type == "met":
            raise NotImplementedError()

    def add_timeseries_data(self, data: Dataset) -> None:
        """Add timeseries data to this Datasource

        Args:
            data: An xarray.Dataset
        Returns:
            None
        """
        from openghg.util import date_overlap

        # Group by year
        year_group = list(data.groupby("time.year"))
        year_data = [data for _, data in year_group if data]

        # Use a dictionary keyed with the daterange covered by each segment of data
        additional_data = {}

        for year in year_data:
            daterange_str = self.get_dataset_daterange_str(dataset=year)
            additional_data[daterange_str] = year

        if self._data:
            # We don't want the same data twice, this will be stored in previous versions
            # Check for overlap between exisiting and new dateranges
            to_keep = []
            for current_daterange in self._data:
                for new_daterange in additional_data:
                    if not date_overlap(daterange_a=current_daterange, daterange_b=new_daterange):
                        to_keep.append(current_daterange)

            updated_data = {}
            for k in to_keep:
                updated_data[k] = self._data[k]
            # Add in the additional new data
            updated_data.update(additional_data)

            self._data = updated_data
        else:
            self._data = additional_data

        data_type = "timeseries"
        self._data_type = data_type
        self.add_metadata_key(key="data_type", value=data_type)
        self.update_daterange()

    def add_metadata(self, metadata: Dict) -> None:
        """Add all metadata in the dictionary to this Datasource

        Args:
            metadata: Dictionary of metadata
        Returns:
            None
        """
        from openghg.util import to_lowercase

        metadata = to_lowercase(metadata)
        self._metadata.update(metadata)

    def add_emissions_data(self, data: Dataset) -> None:
        """Add flux data to this Datasource

        Args:
            data: Flux data as an xarray.Dataset
            metadata: Metadata
        Returns:
            None
        """
        self.add_field_data(data=data, data_type="emissions")

    def add_footprint_data(self, data: Dataset) -> None:
        """Add footprint data to this Datasource

        Args:
            data: Footprint data in an xarray.Dataset
            metadata: Metadata
        Returns:
            None
        """
        self.add_field_data(data=data, data_type="footprint")

    def add_field_data(self, data: Dataset, data_type: str) -> None:
        """Add footprint data to this Datasource

        TODO - unsure if add_field_data is the best name for this function
        Could add a more general function that allows toggle of chunking

        Args:
            data: Footprint data in an xarray.Dataset
            metadata: Metadata
            data_type: Type of data (footprint, flux, met)
        Returns:
            None
        """
        from openghg.util import date_overlap

        # Use a dictionary keyed with the daterange covered by each segment of data
        new_data = {}
        # This daterange string covers the whole of the Dataset
        # For the moment we're not going to chunk footprints
        daterange_str = self.get_dataset_daterange_str(dataset=data)
        new_data[daterange_str] = data

        if self._data:
            # We don't want the same data twice, this will be stored in previous versions
            # Check for overlap between exisiting and new dateranges
            to_keep = []
            for current_daterange in self._data:
                for new_daterange in new_data:
                    if not date_overlap(daterange_a=current_daterange, daterange_b=new_daterange):
                        to_keep.append(current_daterange)

            updated_data = {}
            for k in to_keep:
                updated_data[k] = self._data[k]
            # Add in the additional new data
            updated_data.update(new_data)

            self._data = updated_data
        else:
            self._data = new_data

        self._data_type = data_type
        self.add_metadata_key(key="data_type", value=data_type)
        self.update_daterange()

    def get_dataframe_daterange(self, dataframe: DataFrame) -> Tuple[Timestamp, Timestamp]:
        """Returns the daterange for the passed DataFrame

        Args:
            dataframe: DataFrame to parse
        Returns:
            tuple (Timestamp, Timestamp): Start and end Timestamps for data
        """
        from pandas import DatetimeIndex
        from openghg.util import timestamp_tzaware

        if not isinstance(dataframe.index, DatetimeIndex):
            raise TypeError("Only DataFrames with a DatetimeIndex must be passed")

        # Here we want to make the pandas Timestamps timezone aware
        start = timestamp_tzaware(dataframe.first_valid_index())
        end = timestamp_tzaware(dataframe.last_valid_index())

        return start, end

    def get_dataset_daterange(self, dataset: Dataset) -> Tuple[Timestamp, Timestamp]:
        """Get the daterange for the passed Dataset

        Args:
            dataset (xarray.DataSet): Dataset to parse
        Returns:
            tuple (Timestamp, Timestamp): Start and end datetimes for DataSet

        """
        from pandas import Timestamp

        try:
            start = Timestamp(dataset.time[0].values, tz="UTC")
            end = Timestamp(dataset.time[-1].values, tz="UTC")

            return start, end
        except AttributeError:
            raise AttributeError("This dataset does not have a time attribute, unable to read date range")

    def get_dataset_daterange_str(self, dataset: Dataset) -> str:
        start, end = self.get_dataset_daterange(dataset=dataset)

        # Tidy the string and concatenate them
        start = str(start).replace(" ", "-")
        end = str(end).replace(" ", "-")

        daterange_str = start + "_" + end

        return daterange_str

    @staticmethod
    def exists(datasource_id: str, bucket: Optional[str] = None):
        """Check if a datasource with this ID is already stored in the object store

        Args:
            datasource_id (str): ID of datasource created from data
        Returns:
            bool: True if Datasource exists
        """
        from openghg.objectstore import exists, get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = f"{Datasource._datasource_root}/uuid/{datasource_id}"

        return exists(bucket=bucket, key=key)

    def to_data(self) -> Dict:
        """Return a JSON-serialisable dictionary of object
        for storage in object store

        Storing of the data within the Datasource is done in
        the save function

        Returns:
            dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string

        data = {}
        data["UUID"] = self._uuid
        data["creation_datetime"] = datetime_to_string(self._creation_datetime)
        data["metadata"] = self._metadata
        data["stored"] = self._stored
        data["data_keys"] = self._data_keys
        data["data_type"] = self._data_type
        data["latest_version"] = self._latest_version
        data["rank"] = self._rank

        return data

    @staticmethod
    def load_dataframe(bucket: str, key: str) -> DataFrame:
        """Loads data from the object store for creation of a Datasource object

        Args:
            bucket: Bucket containing data
            key: Key for data
        Returns:
            Pandas.Dataframe: Dataframe from stored HDF file
        """
        from openghg.objectstore import get_object

        data = get_object(bucket, key)

        return Datasource.hdf_to_dataframe(data)

    @staticmethod
    def load_dataset(bucket: str, key: str) -> Dataset:
        """Loads a xarray Dataset from the passed key for creation of a Datasource object

        Currently this function gets binary data back from the object store, writes it
        to a temporary file and then gets xarray to read from this file.

        This is done in a long winded way due to xarray not being able to create a Dataset
        from binary data at the moment.

        Args:
            bucket: Bucket containing data
            key: Key for data
        Returns:
            xarray.Dataset: Dataset from NetCDF file
        """
        from openghg.objectstore import get_object
        from xarray import load_dataset
        import tempfile
        from pathlib import Path

        data = get_object(bucket, key)

        # TODO - is there a cleaner way of doing this?
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).joinpath("tmp.nc")

            with open(tmp_path, "wb") as f:
                f.write(data)

            ds = load_dataset(tmp_path)

            return ds

    # These functions don't work, placeholders for when it's possible to get
    # an in memory NetCDF4 file
    # def dataset_to_netcdf(data):
    #     """ Write the passed dataset to a compressed in-memory NetCDF file
    #     """
    #     import netCDF4
    #     import xarray

    #     store = xarray.backends.NetCDF4DataStore(data)
    #     nc4_ds = netCDF4.Dataset(store)
    #     nc_buf = nc4_ds.close()

    # def netcdf_to_dataset(data):
    #     """ Converts the binary data in data to xarray.Dataset

    #         Args:
    #             data: Binary data
    #         Returns:
    #             xarray.Dataset: Dataset created from data
    #     """
    #     import netCDF4c
    #     import xarray

    #     nc4_ds = netCDF4.Dataset("in_memory.nc", memory=data)
    #     store = xarray.backends.NetCDF4DataStore(nc4_ds)
    #     return xarray.open_dataset(store)

    # Modified from
    # https://github.com/pandas-dev/pandas/issues/9246
    @staticmethod
    def dataframe_to_hdf(data: DataFrame) -> bytes:
        """Writes this Datasource's data to a compressed in-memory HDF5 file

        This function is partnered with hdf_to_dataframe()
        which reads a datframe from the in-memory HDF5 bytes object

        Args:
            dataframe: Dataframe containing raw data
        Returns:
            bytes: HDF5 file as bytes object
        """
        from pandas import HDFStore

        with HDFStore(
            "write.hdf",
            mode="w",
            driver="H5FD_CORE",
            driver_core_backing_store=0,
            complevel=6,
            complib="blosc:blosclz",
        ) as out:

            out["data"] = data
            return out._handle.get_file_image()

    @staticmethod
    def hdf_to_dataframe(hdf_data: bytes) -> DataFrame:
        """Reads a dataframe from the passed HDF5 bytes object buffer

        This function is partnered with dataframe_to_hdf()
        which writes a dataframe to an in-memory HDF5 file

        Args:
            data: Bytes object containing HDF5 file
        Returns:
            Pandas.Dataframe: Dataframe read from HDF5 file buffer
        """
        from pandas import HDFStore, read_hdf

        with HDFStore(
            "read.hdf",
            mode="r",
            driver="H5FD_CORE",
            driver_core_backing_store=0,
            driver_core_image=hdf_data,
        ) as data:
            return read_hdf(data)

    @staticmethod
    def from_data(bucket: str, data: Dict, shallow: bool):
        """Construct a Datasource from JSON

        Args:
            bucket: Bucket containing data
            data: JSON data
            shallow: Load only the JSON data, do not retrieve data from the object store
        Returns:
            Datasource: Datasource created from JSON
        """
        from Acquire.ObjectStore import string_to_datetime
        from collections import defaultdict

        d = Datasource()
        d._uuid = data["UUID"]
        d._creation_datetime = string_to_datetime(data["creation_datetime"])
        d._metadata = data["metadata"]
        d._stored = data["stored"]
        d._data_keys = data["data_keys"]
        d._data = {}
        d._data_type = data["data_type"]
        d._latest_version = data["latest_version"]
        d._rank = defaultdict(list, data["rank"])

        if d._stored and not shallow:
            for date_key in d._data_keys["latest"]["keys"]:
                data_key = d._data_keys["latest"]["keys"][date_key]
                d._data[date_key] = Datasource.load_dataset(bucket=bucket, key=data_key)

        d._stored = False

        d.update_daterange()

        return d

    def save(self, bucket: Optional[str] = None) -> None:
        """Save this Datasource object as JSON to the object store

        Args:
            bucket: Bucket to hold data
        Returns:
            None
        """
        import tempfile
        from copy import deepcopy

        from Acquire.ObjectStore import get_datetime_now_to_string
        from openghg.objectstore import get_bucket, set_object_from_file, set_object_from_json

        if bucket is None:
            bucket = get_bucket()

        if self._data:
            # Ensure we have the latest key
            if "latest" not in self._data_keys:
                self._data_keys["latest"] = {}

            # Backup the old data keys at "latest"
            version_str = f"v{str(len(self._data_keys))}"
            # Store the keys for the new data
            new_keys = {}

            # Iterate over the keys (daterange string) of the data dictionary
            for daterange in self._data:
                data_key = f"{Datasource._data_root}/uuid/{self._uuid}/{version_str}/{daterange}"

                new_keys[daterange] = data_key
                data = self._data[daterange]

                # TODO - for now just create a temporary directory - will have to update Acquire
                # or work on a PR for xarray to allow returning a NetCDF as bytes
                with tempfile.TemporaryDirectory() as tmpdir:
                    filepath = f"{tmpdir}/temp.nc"
                    data.to_netcdf(filepath)
                    set_object_from_file(bucket=bucket, key=data_key, filename=filepath)

            # Copy the last version
            if "latest" in self._data_keys:
                self._data_keys[version_str] = deepcopy(self._data_keys["latest"])

            # Save the new keys and create a timestamp
            self._data_keys[version_str]["keys"] = new_keys
            self._data_keys[version_str]["timestamp"] = get_datetime_now_to_string()

            # Link latest to the newest version
            self._data_keys["latest"] = self._data_keys[version_str]
            self._latest_version = version_str

        self._stored = True
        datasource_key = f"{Datasource._datasource_root}/uuid/{self._uuid}"

        set_object_from_json(bucket=bucket, key=datasource_key, data=self.to_data())

    @staticmethod
    def load(
        bucket: Optional[str] = None, uuid: Optional[str] = None, key: Optional[str] = None, shallow: Optional[bool] = False
    ):
        """Load a Datasource from the object store either by name or UUID

        uuid or name must be passed to the function

        Args:
            bucket: Bucket to store object
            uuid: UID of Datasource
            name: Name of Datasource
            shallow: Only load JSON data, do not read Datasets from object store.
            This will speed up creation of the Datasource object.
        Returns:
            Datasource: Datasource object created from JSON
        """
        from openghg.objectstore import get_bucket, get_object_from_json

        if uuid is None and key is None:
            raise ValueError("Both uuid and key cannot be None")

        if bucket is None:
            bucket = get_bucket()

        if key is None:
            key = f"{Datasource._datasource_root}/uuid/{uuid}"

        data = get_object_from_json(bucket=bucket, key=key)

        return Datasource.from_data(bucket=bucket, data=data, shallow=shallow)

    def data(self) -> dict:
        """Get the data stored in this Datasource

        Returns:
            dict: Dictionary of data keyed by daterange
        """
        from openghg.objectstore import get_bucket

        if not self._data:
            bucket = get_bucket()

            for date_key in self._data_keys["latest"]["keys"]:
                data_key = self._data_keys["latest"]["keys"][date_key]
                self._data[date_key] = Datasource.load_dataset(bucket=bucket, key=data_key)

        return self._data

    def update_daterange(self) -> None:
        """Update the dates stored by this Datasource

        Returns:
            None
        """
        from openghg.util import split_daterange_str
        # If we've only shallow loaded (without the data)
        # this Datasource we use the latest data keys
        if not self._data:
            keys = sorted(self._data_keys["latest"]["keys"])
        else:
            keys = sorted(self._data.keys())

        start, _ = split_daterange_str(daterange_str=keys[0])
        _, end = split_daterange_str(daterange_str=keys[-1])

        self._start_date = start
        self._end_date = end

    def daterange(self) -> Tuple[Timestamp, Timestamp]:
        """Get the daterange the data in this Datasource covers as tuple
        of start, end datetime objects

        Returns:
            tuple (Timestamp, Timestamp): Start, end Timestamps
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
        from Acquire.ObjectStore import datetime_to_string

        start, end = self.daterange()
        return "".join([datetime_to_string(start), "_", datetime_to_string(end)])

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

        def search_recurse(term, data):
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

    def search_metadata(self, find_all: Optional[bool] = True, **kwargs) -> bool:
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
        from openghg.util import timestamp_tzaware

        # if self._start_date is None or self._end_date is None:
        #     self.update_daterange()

        start_date = timestamp_tzaware(start_date)
        end_date = timestamp_tzaware(end_date)

        return (start_date <= self._end_date) and (end_date >= self._start_date)

    def keys_in_daterange(self, start_date: Union[str, Timestamp], end_date: Union[str, Timestamp]) -> List[str]:
        """Return the keys for data between the two passed dates

        Args:
            start_date: Start date
            end_date: end date
        Return:
            list: List of keys to data
        """
        data_keys = self._data_keys["latest"]["keys"]

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

        data_keys = self._data_keys["latest"]["keys"]

        return self.key_date_compare(keys=data_keys, start_date=start_date, end_date=end_date)

    def key_date_compare(self, keys: List[str], start_date: Timestamp, end_date: Timestamp) -> List:
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
                in_date.append(keys[key])

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

    def highest_rank(self, start_date: Optional[Timestamp] = None, end_date: Optional[Timestamp] = None) -> int:
        """ Get the highest rank this Datasource has for the passed dates. If this Datasource is not ranked
        a value of 10 is returned.

        Args:
            start_date: Start date
            end_date: End date
        Returns:
            int: Rank value or 10 if unranked.
        """
        ranking = self.rank(start_date=start_date, end_date=end_date)

        if not ranking:
            return 10

        return int(min(ranking.keys()))

    def rank(self, start_date: Optional[Timestamp] = None, end_date: Optional[Timestamp] = None) -> Dict:
        """Get the ranks of data contained within Datasource for the passed daterange.
        If no rank has been set an empty dictionary is returned.

        If no start or end date is passed all ranking data will be returned.

        Args:
            start_date: Start date
            end_date: End date
        Returns:
            dict: Dictionary of rank: daterange
        """
        from collections import defaultdict
        from openghg.util import daterange_from_str, daterange_to_str, create_daterange

        # If we don't have a rank return an empty dictionary
        if not self._rank:
            return {}

        if start_date is None or end_date is None:
            return self._rank

        search_daterange = create_daterange(start=start_date, end=end_date)

        results = defaultdict(list)

        for rank, dateranges in self._rank.items():
            for daterange_str in dateranges:
                daterange = daterange_from_str(daterange_str)

                intersection = search_daterange.intersection(daterange)
                if len(intersection) > 0:
                    results[rank].append(daterange_to_str(intersection))

        return results

    def set_rank(self, rank: Union[int, str], daterange: Union[str, List]) -> None:
        """Set the rank of this Datsource. This allows users to select
        the best data for a specific species at a site. By default
        a Datasource is unranked with a value of 10. The highest rank is 1 and the lowest 9.

        TODO - add a check to ensure multiple ranks aren't set for the same daterange

        Args:
            rank: Rank number between 1 (highest rank) and 10 (unranked).
            daterange: List of daterange strings such as 2019-01-01T00:00:00_2019-12-31T00:00:00
        Returns:
            None
        """
        if not 1 <= int(rank) <= 10:
            raise ValueError("Rank can only take values 1 to 10 (for unranked). Where 1 is the highest rank.")

        if not isinstance(daterange, list):
            daterange = [daterange]

        try:
            self._rank[rank].extend(daterange)
            # self._rank[rank] = combine_dateranges(self._rank[rank])
        except KeyError:
            self._rank[rank] = daterange

    def data_type(self) -> str:
        """Returns the data type held by this Datasource

        Returns:
            str: Data type held by Datasource
        """
        return self._data_type

    def data_keys(self, version: Optional[str] = "latest", return_all: Optional[bool] = False) -> List:
        """Returns the object store keys where data related
        to this Datasource is stored

        Args:
            version: Version of keys to retrieve
            return_all: Return all data keys
        Returns:
            list: List of data keys
        """
        if return_all:
            return self._data_keys

        try:
            keys = [v for k, v in self._data_keys[version]["keys"].items()]
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
