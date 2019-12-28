__all___ = ["Datasource"]

class Datasource:
    """ A Datasource holds data relating to a single source, such as a specific species
        at a certain height on a specific instrument

        Args:
            name (str, default=None): Name of Datasource
            data (list, default=None): List of xarray Datasets sorted by date
    """
    _datasource_root = "datasource"
    _datavalues_root = "values"
    _data_root = "data"

    def __init__(self, name=None, data=None):
        
        from Acquire.ObjectStore import create_uuid, get_datetime_now, string_to_datetime
        from HUGS.Util import timestamp_tzaware
        
        self._uuid = create_uuid()
        self._name = name
        self._creation_datetime = get_datetime_now()
        self._metadata = {}
        self._parent = None
 
        # Store of data
        if data:
            # This could be a list of dataframes
            self._data = data
            # Just store these as time stamps?
            # Get the first and last datetime from the list of dataframes
            # TODO - update this as each dataframe may have different start and end dates
            # These should be passed in ordered
            self.update_daterange()
            # self._start_datetime = timestamp_tzaware(data[0].first_valid_index())
            # self._end_datetime = timestamp_tzaware(data[-1].last_valid_index())
        else:
            self._data = []
            self._start_datetime = None
            self._end_datetime = None
 
        self._stored = False
        self._data_keys = {}
        self._data_type = None

    def start_datetime(self):
        """ Returns the starting datetime for the data in this Datasource

            Returns:
                datetime: Datetime for start of data
        """        
        return self._start_datetime

    def end_datetime(self):
        """ Returns the end datetime for the data in this Datasource

            Returns:
                datetime: Datetime for end of data
        """
        return self._end_datetime

    def add_metadata(self, key, value):
        """ Add a label to the metadata dictionary with the key value pair
            This will overwrite any previous entry stored at that key.

            Args:
                key (str): Key for dictionary
                value (str): Value for dictionary
            Returns:
                None
        """
        value = str(value)
        self._metadata[key.lower()] = value.lower()

    def add_data_dataframe(self, metadata, data, data_type=None, overwrite=False):
        """ Add data to this Datasource and segment the data by size.
            The data is stored as a tuple of the data and the daterange it covers.

            Args:
                metadata (dict): Metadata on the data for this Datasource
                data (Pandas.DataFrame): Data
                data_type (str, default=None): Placeholder for combination of this fn
                with add_footprint_data in the future
                overwrite (bool, default=False): Overwrite existing data
                None
        """
        from pandas import Grouper
        from HUGS.Processing import get_split_frequency

        # Store the metadata as labels
        # for k, v in metadata.items():
        #     self.add_metadata(key=k, value=v)

        # Ensure metadata values are all lowercase
        metadata = {k: v.lower() for k,v in metadata.items()}
        self._metadata.update(metadata)

        # Add in a type record for timeseries data
        # Can possibly combine this function and the add_footprint (and other)
        # functions in the future
        # Store the hashes of data we've seen previously in a dict?
        # Then also check that the data we're trying to input doesn't overwrite the data we
        # currently have
        # Be easiest to first check the dates covered by the data?

        # Check the daterange covered by this data and if we have an overlap
        # 
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
        self.add_metadata(key="data_type", value="timeseries")
        self._data_type = "timeseries"
        # Use daterange() to update the recorded values
        self.update_daterange()

    def add_data(self, metadata, data, data_type=None, overwrite=False):
        """ Add data to this Datasource and segment the data by size.
            The data is stored as a tuple of the data and the daterange it covers.

            Args:
                metadata (dict): Metadata on the data for this Datasource
                data (Pandas.DataFrame): Data
                data_type (str, default=None): Placeholder for combination of this fn
                with add_footprint_data in the future
                overwrite (bool, default=False): Overwrite existing data
                None
        """
        from HUGS.Processing import get_split_frequency
        from xarray import Dataset

        # Store the metadata as labels
        # for k, v in metadata.items():
        #     self.add_metadata(key=k, value=v)

        # Ensure metadata values are all lowercase
        metadata = {k: v.lower() for k, v in metadata.items()}
        self._metadata.update(metadata)

        # Add in a type record for timeseries data
        # Can possibly combine this function and the add_footprint (and other)
        # functions in the future
        # Store the hashes of data we've seen previously in a dict?
        # Then also check that the data we're trying to input doesn't overwrite the data we
        # currently have
        # Be easiest to first check the dates covered by the data?

        # Check the daterange covered by this data and if we have an overlap
        if self._data:
            # Exisiting data in Datsource
            start_data, end_data = self.daterange()
            # This is the data that we may want to add to the Datasource
            start_new, end_new = self.get_dataset_daterange(data)

            # Check if there's overlap of data
            if start_new >= start_data and end_new <= end_data and overwrite is False:
                raise ValueError("The provided data overlaps dates covered by existing data")

        # Unsure how to assess size of Dataset without just writing to NetCDF
        # Doesn't seem to be an xarray version of memory_usage
        group = list(data.groupby("time.month"))
        # Create a list tuples of the split dataset and the daterange it covers
        # As some (years, months, weeks) may be empty we don't want those dataframes
        self._data = [(g, self.get_dataset_daterange(g)) for _, g in group if len(g) > 0]
        self.add_metadata(key="data_type", value="timeseries")
        self._data_type = "timeseries"
        # Use daterange() to update the recorded values
        self.update_daterange()

    def add_footprint_data(self, metadata, data, overwrite=False):
        """ Add footprint data
            The data is stored as a tuple of the data and the daterange it covers.

            Args:
                metadata (dict): Metadata on the data for this Datasource
                data (xarray.Dataset): Footprint data
            Returns:
                None
        """
        self._metadata.update(metadata)
        self.add_metadata(key="data_type", value="footprint")
        self._data_type = "footprint"
        
        start, end = self.get_dataset_daterange(data)
        
        if self._data:
            start_existing, end_existing = self.daterange()
            # Check if there's overlap of data
            if start >= start_existing and end <= end_existing and overwrite is False:
                raise ValueError("The provided data overlaps dates covered by existing data")
        
        # Really we should just concatenate the Datasets?
        # Concatenated datasets would be be better when we move to zarr
        # but as we're still using NetCDF this current methods works best

        # TODO - here could I create a struct to hold this data?
        # Could be cleaner than trying to ensure people / me remember
        # that this is a list of tuple(dataset, tuple) ?
        self._data.append((data, (start, end)))
        self.update_daterange()

    def get_dataframe_daterange(self, dataframe):
        """ Returns the daterange for the passed DataFrame

            Args:
                dataframe (Pandas.DataFrame): DataFrame to parse
            Returns:
                tuple (datetime, datetime): Start and end datetimes for DataSet
        """
        from pandas import DatetimeIndex
        from HUGS.Util import timestamp_tzaware

        if not isinstance(dataframe.index, DatetimeIndex):
            raise TypeError("Only DataFrames with a DatetimeIndex must be passed")

        # Here we want to make the pandas Timestamps timezone aware
        # This seems a bit long winded but we end up with standard Python datetime objects
        # that are timezone aware and set to UTC
        start = timestamp_tzaware(dataframe.first_valid_index())
        end = timestamp_tzaware(dataframe.last_valid_index())

        return start, end

    def get_dataset_daterange(self, dataset):
        """ Get the daterange for the passed Dataset

            Args:
                dataset (xarray.DataSet): Dataset to parse
            Returns:
                tuple (Timestamp, Timestamp): Start and end datetimes for DataSet

        """
        from xarray import Dataset
        from pandas import Timestamp

        if not isinstance(dataset, Dataset):
            raise TypeError("Only xarray Dataset types can be processed")

        try:
            start = Timestamp(dataset.time[0].values, tz="UTC")
            end = Timestamp(dataset.time[-1].values, tz="UTC")
        except:
            raise AttributeError("This dataset does not have a time attribute, unable to read date range")

        return start, end

    @staticmethod
    def exists(datasource_id, bucket=None):
        """ Check if a datasource with this ID is already stored in the object store

            Args:
                datasource_id (str): ID of datasource created from data
            Returns:
                bool: True if Datasource exists 
        """
        from HUGS.ObjectStore import exists, get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = "%s/uuid/%s" % (Datasource._datasource_root, datasource_id)
        
        return exists(bucket=bucket, key=key)

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Storing of the data within the Datasource is done in
            the save function

            Args:
                store (bool, default=False): True if we are storing this
                in the object store
            Returns:
                dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string

        data = {}
        data["UUID"] = self._uuid
        data["name"] = self._name
        data["creation_datetime"] = datetime_to_string(self._creation_datetime)
        data["metadata"] = self._metadata
        data["stored"] = self._stored
        data["data_keys"] = self._data_keys
        data["data_type"] = self._data_type

        return data

    @staticmethod
    def load_dataframe(bucket, key):
        """ Loads data from the object store for creation of a Datasource object

            Args:
                bucket (dict): Bucket containing data
                key (str): Key for data
            Returns:
                Pandas.Dataframe: Dataframe from stored HDF file
        """
        from HUGS.ObjectStore import get_dated_object

        data = get_dated_object(bucket, key)

        return Datasource.hdf_to_dataframe(data)


    @staticmethod
    def load_dataset(bucket, key):
        """ Loads a xarray Dataset from the passed key for creation of a Datasource object

            Currently this function gets binary data back from the object store, writes it
            to a temporary file and then gets xarray to read from this file. 

            This is done in a long winded way due to xarray not being able to create a Dataset
            from binary data at the moment.

            Args:
                bucket (dict): Bucket containing data
                key (str): Key for data
            Returns:
                xarray.Dataset: Dataset from NetCDF file
        """
        from Acquire.ObjectStore import ObjectStore
        from xarray import open_dataset
        import tempfile

        data = ObjectStore.get_object(bucket, key)

        # TODO - is there a cleaner way of doing this?
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path =  f"{tmpdir}/tmp.nc"
            with open(tmp_path, "wb") as f:
                f.write(data)

            ds = open_dataset(tmp_path)

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
    #     import netCDF4
    #     import xarray

    #     nc4_ds = netCDF4.Dataset("in_memory.nc", memory=data)
    #     store = xarray.backends.NetCDF4DataStore(nc4_ds)
    #     return xarray.open_dataset(store)



    # Modified from
    # https://github.com/pandas-dev/pandas/issues/9246
    @staticmethod
    def dataframe_to_hdf(data):
        """ Writes this Datasource's data to a compressed in-memory HDF5 file

            This function is partnered with hdf_to_dataframe()
            which reads a datframe from the in-memory HDF5 bytes object

            Args:
                dataframe (Pandas.Dataframe): Dataframe containing raw data
            Returns:
                bytes: HDF5 file as bytes object
        """
        from pandas import HDFStore

        with HDFStore("write.hdf", mode="w", driver="H5FD_CORE", driver_core_backing_store=0,
                        complevel=6, complib="blosc:blosclz") as out:
            
            out["data"] = data
            return out._handle.get_file_image()

    @staticmethod
    def hdf_to_dataframe(hdf_data):
        """ Reads a dataframe from the passed HDF5 bytes object buffer

            This function is partnered with dataframe_to_hdf()
            which writes a dataframe to an in-memory HDF5 file

            Args:
                data (bytes): Bytes object containing HDF5 file
            Returns:
                Pandas.Dataframe: Dataframe read from HDF5 file buffer
        """
        from pandas import HDFStore, read_hdf

        with HDFStore("read.hdf", mode="r", driver="H5FD_CORE", driver_core_backing_store=0,
                        driver_core_image=hdf_data) as data:
            return read_hdf(data)

    @staticmethod
    def from_data(bucket, data, shallow):
        """ Construct from a JSON-deserialised dictionary

            Args:
                bucket (dict): Bucket containing data
                data (dict): JSON data
                shallow (bool): Load only the JSON data, do not retrieve
                data from the object store
            Returns:
                Datasource: Datasource created from JSON
        """
        from Acquire.ObjectStore import string_to_datetime

        if data is None or len(data) == 0:
            return Datasource()

        d = Datasource()
        d._uuid = data["UUID"]
        d._name = data["name"]
        d._creation_datetime = string_to_datetime(data["creation_datetime"])
        d._metadata = data["metadata"]
        d._stored = data["stored"]
        d._data_keys = data["data_keys"]
        d._data = []
        d._data_type = data["data_type"]
        
        if d._stored and not shallow:
            for key in d._data_keys:
                daterange = d._data_keys[key].split("_")
                start =  string_to_datetime(daterange[0])
                end = string_to_datetime(daterange[1])
                d._data.append((Datasource.load_dataset(bucket, key), (start,end)))

        d._stored = False

        return d

    def save(self, bucket=None):
        """ Save this Datasource object as JSON to the object store
    
            Args:
                bucket (dict): Bucket to hold data
            Returns:
                None
        """
        import tempfile
        from Acquire.ObjectStore import datetime_to_string, ObjectStore
        from HUGS.ObjectStore import get_bucket
        # from zarr import Blosc

        if bucket is None:
            bucket = get_bucket()

        # For now we'll get the data type from the metadata
        # data_type = self._metadata["data_type"]        

        if self._data is not None:
            for data, daterange in self._data:
                start, end = daterange
                daterange_str = "".join([datetime_to_string(start), "_", datetime_to_string(end)])
                data_key = "%s/uuid/%s/%s" % (Datasource._data_root, self._uuid, daterange_str)
                self._data_keys[data_key] = daterange_str

                # TODO - for now just create a temporary directory - will have to update Acquire
                # or work on a PR for xarray to allow returning a NetCDF as bytes
                with tempfile.TemporaryDirectory() as tmpdir:
                    filepath = f"{tmpdir}/temp.nc"
                    data.to_netcdf(filepath)
                    ObjectStore.set_object_from_file(bucket, data_key, filepath)

                # if self._data_type == "timeseries":
                #     ObjectStore.set_object(bucket, data_key, Datasource.dataframe_to_hdf(data))
                
                # TODO - tidy or remove this
                # Leftover from trying out converting to zarr with compression, will leave for now
                # Set the compression type and level for each variable
                # For some reason conversion to zarr seems to increase the amount of space
                # the data takes, this could be due to compression in the NetCDF file.
                # Compressing using zstd with a compression level of 3 seems to be a good trade-off
                # of size and compression time
                # encodings = {}
                # for k in data.keys():
                #     encodings[k] = {"compressor": Blosc(cname='zstd', clevel=3)}
                # # Convert to zarr format and compress
                # data = data.to_zarr(consolidated=True, encoding=encodings)

                # encodings = {}
                # for k in data.keys():
                #     encodings[k] = {"zlib": True, "complevel": 3}
        
                # Convert to zarr format and compress
                # data = data.to_zarr(consolidated=True, encoding=encodings)
                # data.to_netcdf(filepath, encoding=encodings)        

        self._stored = True

        datasource_key = "%s/uuid/%s" % (Datasource._datasource_root, self._uuid)
        ObjectStore.set_object_from_json(bucket=bucket, key=datasource_key, data=self.to_data())

    @staticmethod
    def load(bucket=None, uuid=None, key=None, shallow=False):
        """ Load a Datasource from the object store either by name or UUID

            uuid or name must be passed to the function

            Args:
                bucket (dict, default=None): Bucket to store object
                uuid (str, default=None): UID of Datasource
                name (str, default=None): Name of Datasource
            Returns:
                Datasource: Datasource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore
        from HUGS.ObjectStore import get_bucket, get_object_json

        if uuid is None and key is None:
            raise ValueError("Both uuid and key cannot be None")

        if bucket is None:
            bucket = get_bucket()
        
        if not key:
            key = "%s/uuid/%s" % (Datasource._datasource_root, uuid)

        data = get_object_json(bucket=bucket, key=key)
        return Datasource.from_data(bucket=bucket, data=data, shallow=shallow)

    @staticmethod
    def _get_name_from_uid(bucket, uuid):
        """ Returns the name of the Datasource associated with
            the passed UID

            Args:
                bucket (dict): Bucket holding data
                name (str): Name to search
            Returns:
                str: UUID for the Datasource
        """
        from HUGS.ObjectStore import get_dated_object_json

        key = "%s/uuid/%s" % (Datasource._datasource_root, uuid)
        data = get_dated_object_json(bucket=bucket, key=key)

        return data["name"]

    @staticmethod
    def _get_uid_from_name(bucket, name):
        """ Returns the UUID associated with this named Datasource

            Args:
                bucket (dict): Bucket holding data
                name (str): Name to search
            Returns:
                str: UUID for the Datasource
        """
        from Acquire.ObjectStore import ObjectStore, string_to_encoded

        encoded_name = string_to_encoded(name)
        prefix = "%s/name/%s" % (Datasource._datasource_root, encoded_name)
        uuid = ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

        if len(uuid) > 1:
            raise ValueError("There should only be one Datasource associated with this name")
        
        return uuid[0].split("/")[-1]

    def data(self):
        """ Get the data stored in this Datasource

            Returns:
                list: List of tuples of Pandas.DataFrame and start, end datetime tuple
        """
        return self._data

    def sort_data(self):
        """ Sorts the data in the data list

            TODO: Could this be better handled using an OrderedDict?

            Returns:
                None
        """
        from operator import itemgetter
        # Data list elements contain a tuple of
        # (data,(start_datetime, end_datetime))
        # Could also check to make sure we don't have overlapping dateranges?
        self._data = sorted(self._data, key=itemgetter(1,0))

    def update_daterange(self):
        """ Update the dates stored by this Datasource

            TODO - cleaner way of doing this?

            Returns:
                None
        """
        # from Acquire.ObjectStore import datetime_to_datetime
        from HUGS.Util import timestamp_tzaware

        if self._data:
            # Sort the data by date
            self.sort_data()
            data_type = self._data_type
            # if data_type == "timeseries":
            #     self._start_datetime = timestamp_tzaware(self._data[0][0].first_valid_index())
            #     self._end_datetime = timestamp_tzaware(self._data[-1][0].last_valid_index())
            # elif data_type == "footprint":
            # TODO - this feels messy
            start, _ = self.get_dataset_daterange(self._data[0][0])
            _, end = self.get_dataset_daterange(self._data[-1][0])
            self._start_datetime = timestamp_tzaware(start)
            self._end_datetime = timestamp_tzaware(end)
            # else:
            #     raise NotImplementedError("Only CRDS, GC and footprint data currently recognized")
        else:
            raise ValueError("Cannot get daterange with no data")

    def daterange(self):
        """ Get the daterange the data in this Datasource covers as tuple
            of start, end datetime objects

            Returns:
                tuple (datetime, datetime): Start, end datetimes
        """
        if not self._start_datetime and self._data:
            self.update_daterange()

        return self._start_datetime, self._end_datetime

    def daterange_str(self):    
        """ Get the daterange this Datasource covers as a string in
            the form start_end

            Returns:
                str: Daterange covered by this Datasource
        """
        from Acquire.ObjectStore import datetime_to_string

        start, end = self.daterange()
        return "".join([datetime_to_string(start), "_", datetime_to_string(end)])

    def search_metadata(self, search_term):
        """ Search the values of the metadata of this Datasource for search_term

            Args:
                search_term (str): String to search for in metadata
            Returns:
                bool: True if found else False
        """
        search_term = search_term.lower()

        for v in self._metadata.values():
            if v == search_term:
                return True

        return False
            
    def species(self):
        """ Returns the species of this Datasource

            Returns:
                str: Species of this Datasource
        """
        return self._metadata["species"]

    def inlet(self):
        """ Returns the inlet of this Datasource

            Returns:
                str: Inlet of this Datasource
        """
        return self._metadata["inlet"]

    def site(self):
        if "site" in self._metadata:
            return self._metadata["site"]
        else:
            return "NA"
        
    def uuid(self):
        """ Return the UUID of this object

            Returns:
                str: UUID
        """
        return self._uuid

    def metadata(self):
        """ Retur the metadata of this Datasource

            Returns:
                dict: Metadata of Datasource
        """
        return self._metadata

    def set_id(self, uid):
        """ Set the UUID for this Datasource

            Args:
                id (str): UUID
            Returns:
                None
        """
        self._uuid = uid

    def data_type(self):
        """ Returns the data type held by this Datasource

            Returns:
                str: Data type held by Datasource
        """
        return self._data_type
