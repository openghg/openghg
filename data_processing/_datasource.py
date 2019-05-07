__all___ = ["DataSource"]

_datasource_root = "datasource"
_datavalues_root = "values"


class DataSource:
    """ This class handles all data sources such as sensors

        DataSource objects should be created via DataSource.create()
    """
    def __init__(self):
        """ Construct a null DataSource """
        self._uid = None
        self._name = None
        self._site = None
        self._network = None

    @staticmethod
    def create(name=None, instrument, site, network):
        """Create a new datasource
        
            Args:
                name (str, default=None): Name for DataSource
            Returns:
                
        """        
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        d = DataSource()
        d._uid = _create_uuid()
        d._name = name
        d._site = site
        d._network = network
        d._creation_datetime = _get_datetime_now()

        return d

    def is_null(self):
        """Return whether this object is null
        
            Returns:
                bool: True if object is null
        """
        return self._uid is None

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        if self.is_null():
            return {}

        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        data = {}
        data["uid"] = self._uid
        data["name"] = self._name
        data["creation_datetime"] = _datetime_to_string(
            self._creation_datetime)

        return data

    @staticmethod
    def from_data(data):
        """Construct from a JSON-deserialised dictionary"""
        if data is None or len(data) == 0:
            return DataSource()

        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime
        d = DataSource()

        d._uid = data["uid"]
        d._name = data["name"]
        d._creation_datetime = _string_to_datetime(data["creation_datetime"])

        return d

    def save(self, bucket=None):
        """ Save this DataSource object as JSON to the object store
        
            Args:
                bucket (dict): Bucket to hold data
            Returns:
                None
        """
        if self.is_null():
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded

        from hugs_objstore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket(bucket)

        key = "%s/uid/%s" % (_datasource_root, self._uid)
        _ObjectStore.set_object_from_json(
            bucket=bucket, key=key, data=self.to_data())

        encoded_name = _string_to_encoded(self._name)

        key = "%s/name/%s/%s" % (_datasource_root, encoded_name, self._uid)

        _ObjectStore.set_string_object(
            bucket=bucket, key=key, string_data=self._uid)

    @staticmethod
    def load(bucket=None, uid=None, name=None):
        """ Load a DataSource from the object store either by name or UID

            Args:
                bucket (dict): Bucket to store object
                uid (str, default=None): UID of DataSource to load
                name (str, default=None): Name of DataSource
            Returns:
                DataSource: DataSource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        if uid is None and name is None:
            raise ValueError("Both uid and name cannot be None")

        if bucket is None:
            bucket = DataSource._get_bucket(bucket)
        if uid is None:
            uid = DataSource._get_uid_from_name(bucket=bucket, name=name)

        key = "%s/uid/%s" % (_datasource_root, uid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return DataSource.from_data(data)

    def get_values(self, bucket, datetime_begin, datetime_end):
        """ Get all values for this DataSource stored in the object store

            Args:  
                bucket (dict): Bucket holding data
                datetime_begin (datetime): Start of datetime range
                datetime_end (datetime): End of datetime range
            Returns:
                list: A list of Pandas.Dataframes

        """
        from Acquire.ObjectStore import datetime_to_datetime as _datetime_to_datetime

        # Ensure that datetime is valid
        datetime_begin = _datetime_to_datetime(datetime_begin)
        datetime_end = _datetime_to_datetime(datetime_end)

        year_begin = datetime_begin.year
        year_end = datetime_end.year

        keys = []

        # Find the keys that are valid
        for year in range(year_begin, year_end+1):
            prefix = "%s/%s/%s" % (_datavalues_root, self._uid, year)
            
            datakeys = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)
               
            # Check the end date of the data
            for datakey in datakeys:
                start, end = _string_to_daterange(datakey.split("_")[-1])

                if end.year < year_end:
                    keys.append(datakey)

        # List to store dataframes
        values = []

        for key in keys:
            values.append(get_dataframe(bucket=bucket, key=key))

        return values

    @staticmethod
    def _get_name_from_uid(bucket, uid):
        """ Returns the name of the DataSource associated with
            the passed UID

            Args:
                bucket (dict): Bucket holding data
                name (str): Name to search
            Returns:
                str: UUID for the DataSource
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded

        key = "%s/uid/%s" % (_datasource_root, uid)        

        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return data["name"]

    @staticmethod
    def _get_uid_from_name(bucket, name):
        """ Returns the UUID associated with this named DataSource

            Args:
                bucket (dict): Bucket holding data
                name (str): Name to search
            Returns:
                str: UUID for the DataSource
        """

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded

        encoded_name = _string_to_encoded(name)

        prefix = "%s/name/%s", (_datasource_root, encoded_name)

        uid = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

        if len(uid) > 1:
            raise ValueError("There should only be one DataSource associated with this name")
        
        return uid[0]

    
