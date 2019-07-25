__all___ = ["Datasource"]

class Datasource:
    """ This class handles all data sources such as sensors

        Datasource objects should be created via Datasource.create()
    """
    _datasource_root = "datasource"
    _datavalues_root = "values"
    _data_root = "data"

    def __init__(self):
        """ Construct a null Datasource """
        self._uuid = None
        self._name = None
        self._creation_datetime = None
        self._labels = {}

        self._metadata = None
        self._parent = None
        # These may be unnecessary?
        self._instrument = None
        self._site = None
        self._network = None
        # Store of data
        self._data = []
        self._start_datetime = None
        self._end_datetime = None
        self._stored = False
        self._data_keys = []

    @staticmethod
    def create(name, data=None, **kwargs):
        """ Create a new datasource
        
            Args:
                name (str): Name for Datasource
                data (list, default=None): List of Pandas.Dataframes
                **kwargs (dict): Dictionary saved as the labels for this object
            Returns:
                Datasource

            TODO - add kwargs to this to allow extra (safely parsed)
            arguments to be added to the dictionary?
            This would allow some elasticity to the datasources
                
        """        
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now
        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime

        d = Datasource()
        d._name = name
        d._uuid = _create_uuid()
        d._creation_datetime = _get_datetime_now()
                
        # Any need to parse these for safety?
        d._labels = kwargs
        d._labels["gas"] = name
        
        if data is not None:
            # This could be a list of dataframes
            d._data = data
            # Just store these as time stamps?
            # Get the first and last datetime from the list of dataframes
            # TODO - update this as each dataframe may have different start and end dates
            d._start_datetime = _string_to_datetime(data[0].first_valid_index())
            d._end_datetime = _string_to_datetime(data[-1].last_valid_index())
        
        return d

    def is_null(self):
        """Return whether this object is null
        
            Returns:
                bool: True if object is null
        """
        return self._uuid is None

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

    def add_label(self, key, value):
        """ Add a label to the label dictionary with the key value pair
            This will overwrite any previous entry stored at that key.

            Args:
                key (str): Key for dictionary
                value (str): Value for dictionary
            Returns:
                None
        """
        self._labels[key.lower()] = value.lower()

    def add_data(self, metadata, data):
        """ Add data to this Datasource and segment the data by size.
            The data is stored as a tuple of the data and the daterange it covers.

            Args:
                metadata (dict): Metadata on the data for this Datasource
                data (Pandas.DataFrame): Data
            Returns:
                None
        """
        from pandas import Grouper as _Grouper
        from HUGS.Processing import get_split_frequency as _get_split_frequency

        # Store the metadata as labels
        for k, v in metadata.items():
            self.add_label(key=k, value=v)

        freq = _get_split_frequency(data)
        # Split into sections by splitting frequency
        group = data.groupby(_Grouper(freq=freq))
        # Create a list tuples of the split dataframe and the daterange it covers
        # As some (years, months, weeks) may be empty we don't want those dataframes
        self._data = [(g, self.get_dataframe_daterange(g)) for _, g in group if len(g) > 0]


    def get_dataframe_daterange(self, dataframe):
        """ Returns the daterange for the passed dataframe

            Args:
                dataframe (Pandas.DataFrame): Dataframe to parse
            Returns:
                tuple (datetime, datetime): Start and end datetimes for dataframe
        """
        import datetime as _datetime

        start = dataframe.first_valid_index()
        end = dataframe.last_valid_index()

        return start, end

    def add_metadata(self, metadata):
        """ Add metadata to this object

            Args:
                metadata (Metadata): Metadata object
        """
        self._metadata = metadata


    @staticmethod
    def exists(datasource_id, bucket=None):
        """ Uses an ID of some kind to query whether or not this is a new
            Datasource and should be created

            Check if a datasource with this ID is already stored in the object store

            WIP

            TODO - update this when I have a clearer idea of how to ID datasources

            Args:
                datasource_id (str): ID of datasource created from Data / given in data
            Returns:
                bool: True if Datasource exists 
        """
        from HUGS.ObjectStore import exists as _exists
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        key = "%s/uuid/%s" % (Datasource._data_root, datasource_id)
        
        # Query object store for Datasource
        return _exists(bucket=bucket, key=key)

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
        if self.is_null():
            return {}

        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        data = {}
        data["UUID"] = self._uuid
        data["name"] = self._name
        data["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        data["labels"] = self._labels
        data["stored"] = self._stored
        data["data_keys"] = self._data_keys

        return data

    @staticmethod
    def load_dataframe(bucket, key):
        """ Loads data from the object store for creation of a Datasource object

            Args:
                bucket (dict): Bucket containing data
                uuid (str): UUID for Datasource 
            Returns:
                Pandas.Dataframe: Dataframe from stored HDF file
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from HUGS.ObjectStore import get_dated_object as _get_dated_object

        data = _get_dated_object(bucket, key)

        return Datasource.hdf_to_dataframe(data)

    # The save_dataframe function was moved to be part of save()

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
        from pandas import HDFStore as _HDFStore
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string
        from Acquire.ObjectStore import get_datetime_now_to_string

        with _HDFStore("write.hdf", mode="w", driver="H5FD_CORE", driver_core_backing_store=0,
                        complevel=6, complib="blosc:blosclz") as out:
            
            out["data"] = data
            # Copy the data and close the file to check if this works
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
        from pandas import HDFStore as _HDFStore
        from pandas import read_hdf as _read_hdf
        from Acquire.ObjectStore import get_datetime_now_to_string

        with _HDFStore("read.hdf", mode="r", driver="H5FD_CORE", driver_core_backing_store=0,
                        driver_core_image=hdf_data) as data:
            return _read_hdf(data)

    @staticmethod
    def from_data(bucket, data, shallow):
        """ Construct from a JSON-deserialised dictionary

            Args:
                bucket (dict): Bucket containing data
                data (dict): JSON data
            Returns:
                Datasource: Datasource created from JSON
        """
        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime

        if data is None or len(data) == 0:
            return Datasource()

        d = Datasource()
        d._uuid = data["UUID"]
        d._name = data["name"]
        d._creation_datetime = _string_to_datetime(data["creation_datetime"])
        d._labels = data["labels"]
        d._stored = data["stored"]
        d._data_keys = data["data_keys"]
        d._data = []
        
        if d._stored and not shallow:
            for key in d._data_keys:
                d._data.append(Datasource.load_dataframe(bucket, key))

        return d

    def save(self, bucket=None):
        """ Save this Datasource object as JSON to the object store
    
            Args:
                bucket (dict): Bucket to hold data
            Returns:
                None
        """
        if self.is_null():
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        if self._data is not None:
            for data, daterange in self._data:
                start, end = daterange
                daterange_str = "".join([_datetime_to_string(start), "_", _datetime_to_string(end)])
                data_key = "%s/uuid/%s/%s" % (Datasource._data_root, self._uuid, daterange_str)
                self._data_keys.append(data_key)
                _ObjectStore.set_object(bucket, data_key, Datasource.dataframe_to_hdf(data))

            self._stored = True


        datasource_key = "%s/uuid/%s" % (Datasource._datasource_root, self._uuid)
        _ObjectStore.set_object_from_json(bucket=bucket, key=datasource_key, data=self.to_data())
        
        # encoded_name = _string_to_encoded(self._name)
        # name_key = "%s/name/%s/%s" % (Datasource._datasource_root, encoded_name, self._uuid)
        # _ObjectStore.set_string_object(bucket=bucket, key=name_key, string_data=self._uuid)


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
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from HUGS.ObjectStore import get_dated_object as _get_dated_object
        from HUGS.ObjectStore import get_object_json as _get_object_json
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if uuid is None and key is None:
            raise ValueError("Both uuid and key cannot be None")

        if bucket is None:
            bucket = _get_bucket()
        
        if not key:
            key = "%s/uuid/%s" % (Datasource._datasource_root, uuid)

        data = _get_object_json(bucket=bucket, key=key)

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
        # from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        from HUGS.ObjectStore import get_dated_object_json as _get_dated_object_json

        key = "%s/uuid/%s" % (Datasource._datasource_root, uuid)

        data = _get_dated_object_json(bucket=bucket, key=key)

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
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded

        encoded_name = _string_to_encoded(name)

        prefix = "%s/name/%s" % (Datasource._datasource_root, encoded_name)

        uuid = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

        if len(uuid) > 1:
            raise ValueError("There should only be one Datasource associated with this name")
        
        return uuid[0].split("/")[-1]

    def data(self):
        """ Get the data stored in this Datasource

            Returns:
                Pandas.Dataframe: Dataframe stored in this object
        """
        return self._data

    def daterange(self):
        """ Get the daterange this Datasource covers as a string

            Returns:
                str: Daterange as string 
        """
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        if self._start_datetime is None or self._end_datetime is None:
            if self._data is not None:
                # TODO - this is clunky - better way?
                self._start_datetime = self._data[0][1][0]
                self._end_datetime = self._data[-1][1][1]
            else:
                raise ValueError("Cannot get daterange with no data")
                
        return "".join([_datetime_to_string(self._start_datetime), "_", _datetime_to_string(self._end_datetime)])

    def search_labels(self, search_term):
        """ Search the values of the labels of this Datasource for search_term

            Args:
                search_term (str): String to search for in labels
            Returns:
                bool: True if found else False
        """
        for v in self._labels.values():
            if v == search_term.lower():
                return True

        return False
            
    def species(self):
        """ Returns the species of this Datasource

            Returns:
                str: Species of this Datasource
        """
        return self._labels["species"]

    def inlet(self):
        """ Returns the inlet of this Datasource

            Returns:
                str: Inlet of this Datasource
        """
        return self._labels["inlet"]

    def site(self):
        if "site" in self._labels:
            return self._labels["site"]
        else:
            return "NA"
        
    def uuid(self):
        """ Return the UUID of this object

            Returns:
                str: UUID
        """
        return self._uuid

    def labels(self):
        """ Retur the labels of this Datasource

            Returns:
                dict: Labels of Datasource
        """
        return self._labels
