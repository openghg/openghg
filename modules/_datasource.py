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
        self._parent = None
        # These may be unnecessary?
        self._instrument = None
        self._site = None
        self._network = None
        # Store of data
        self._data = None
        self._start_datetime = None
        self._end_datetime = None

    @staticmethod
    def create(name, instrument, site, network, data=None):
        """ Create a new datasource
        
            Args:
                name (str): Name for Datasource
                instrument (str): Name of instrument
                site (str): Name of site
                network (str): Name of network
                data (Pandas.Dataframe, default=None): Data from source
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
        d._uuid = _create_uuid()
        d._name = name
        d._creation_datetime = _get_datetime_now()
        # Now unsure about these
        d._instrument = instrument
        d._site = site
        d._network = network
        
        d._data = data
        d._start_datetime = _string_to_datetime(data.iloc[0]["Datetime"])
        d._end_datetime = _string_to_datetime(data.iloc[-1]["Datetime"])

        return d

    def is_null(self):
        """Return whether this object is null
        
            Returns:
                bool: True if object is null
        """
        return self._uuid is None

    def get_start_datetime():
        """ Returns the starting datetime for the data in this Datasource

            Returns:
                datetime: Datetime for start of data
        """        
        return self._start_datetime

    def get_end_datetime():
        """ Returns the end datetime for the data in this Datasource

            Returns:
                datetime: Datetime for end of data
        """
        return self._end_datetime

    def get_site():
        """ Returns the site with which this datasource is 

            Returns:
                str: Name of site
        """
        return self._site()


    def to_data(self, store=False, bucket=None):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

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
        data["instrument"] = self._instrument
        data["site"] = self._site
        data["network"] = self._network

        return data

    def store_data(self, bucket):
        """ Returns the UUID for the data to be saved in the object store
            as a HDF file

            Returns:
                str: UUID for data stored in object store
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        import pandas as _pd
        
        # Store the data in the object store and assign a UUID to it
        data_key = "%s/uuid/%s" % (Datasource._data_root, self._uuid)
        
        filename = "tmp_hdf_%s" % self._uuid
        # Store the HDF file in the object store
        self._data.to_hdf(filename, mode="w", complevel=5,
                          complib="blosc:blosclz")
        
        # Save this HDF file to the object store
        _ObjectStore.set_object(bucket, data_key, )

        # TODO - get im memory saving of HDF file working
        
        # Taken from
        # https: // github.com/pandas-dev/pandas/issues/9246
        # Where frames is a dictionary of dataframes
        # def write_hdf_to_buffer(dataframe):
        #     import pandas as _pd
        #     # Where get_store has been deprecated for
        #     # pd.HDFStore(...)
        #     with _pd.HDFStore("data.h5", mode="a", driver="H5FD_CORE", driver_core_backing_store=0) as out:
        #         out["data"] = dataframe

        #         for key, df in frames.items():
        #             out[key] = df
        #         return out._handle.get_file_image()

        # def read_hdf_from_buffer(buffer):
        #     from pandas import get_store
        #     return get_store(
        #             "data.h5",
        #             mode="r",
        #             driver="H5FD_CORE",
        #             driver_core_backing_store=0,
        #             driver_core_image=buffer.read()
        #             )


    @staticmethod
    def get_data(bucket, uuid):
        """ Get this Datasource's data from the object store
            This data is stored in a HDF format.

            Args:
                bucket (dict): Bucket containing data
                uuid (str): UUID for data
            Returns:
                Pandas.Dataframe: Dataframe from HDF file in object store
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        import pandas as _pd

        key = "%s/uuid/%s" % (Datasource._data_root, uuid)
        # Get the HDF file from the object store
        data = _ObjectStore.get_object(bucket, key)

        return _pd.read_hdf(data)

    @staticmethod
    def from_data(data):
        """ Construct from a JSON-deserialised dictionary

            Returns:
                Datasource: Datasource created from JSON
        """
        if data is None or len(data) == 0:
            return Datasource()

        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime

        d = Datasource()
        d._uuid = data["UUID"]
        d._name = data["name"]
        d._creation_datetime = _string_to_datetime(data["creation_datetime"])
        d._instrument = data["instrument"]
        d._site = data["site"]
        d._network = data["network"]


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
        from objectstore.hugs_objstore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        datasource_key = "%s/uuid/%s" % (Datasource._datasource_root, self._uuid)
        _ObjectStore.set_object_from_json(bucket=bucket, key=datasource_key, data=self.to_data())
        
        encoded_name = _string_to_encoded(self._name)
        name_key = "%s/name/%s/%s" % (Datasource._datasource_root, encoded_name, self._uuid)
        _ObjectStore.set_string_object(bucket=bucket, key=name_key, string_data=self._uuid)

    @staticmethod
    def load(bucket=None, uuid=None, name=None):
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
        from objectstore.hugs_objstore import get_bucket as _get_bucket

        if uuid is None and name is None:
            raise ValueError("Both uuid and name cannot be None")

        if bucket is None:
            bucket = _get_bucket()
        if uuid is None:
            uuid = Datasource._get_uid_from_name(bucket=bucket, name=name)

        key = "%s/uuid/%s" % (Datasource._datasource_root, uuid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return Datasource.from_data(data)

    @staticmethod
    def get_name_from_uid(bucket, uuid):
        """ Returns the name of the Datasource associated with
            the passed UID

            Args:
                bucket (dict): Bucket holding data
                name (str): Name to search
            Returns:
                str: UUID for the Datasource
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded

        key = "%s/uuid/%s" % (Datasource._datasource_root, uuid)

        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

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

    def save_data(self, data):
        """ Store the passed data within the Datasource

            Args:
                data (Pandas.Dataframe): Data to save
            Returns:
                None
        """
        self._data = data
    
    def get_data(self):
        """ Returns the data stored within the Datasource
        
            Returns:
                Pandas.Dataframe: Data stored within the Datasource
        """
        return self._data

        
