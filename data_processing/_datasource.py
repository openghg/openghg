__all___ = ["Datasource"]




class Datasource:
    """ This class handles all data sources such as sensors

        Datasource objects should be created via Datasource.create()
    """
    _datasource_root = "datasource"
    _datavalues_root = "values"

    def __init__(self):
        """ Construct a null Datasource """
        self._uuid = None
        self._name = None
        self._site = None
        self._network = None
        self._height = None

    @staticmethod
    def create(name=None, instrument, site, network, height=None):
        """Create a new datasource
        
            Args:
                name (str, default=None): Name for Datasource
            Returns:
                Datasource

            TODO - add kwargs to this to allow extra (safely parsed)
            arguments to be added to the dictionary?
            This would allow some elasticity to the datasources
                
        """        
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        d = Datasource()
        d._uuid = _create_uuid()
        d._name = name
        d._site = site
        d._network = network
        d._creation_datetime = _get_datetime_now()

        return d

    def get_site():
        """ Returns the site with which this datasource is 

            Returns:
                str: Name of site
        """
        return self._site()

    def is_null(self):
        """Return whether this object is null
        
            Returns:
                bool: True if object is null
        """
        return self._uuid is None

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
        data["UUID"] = self._uuid
        data["name"] = self._name
        data["creation_datetime"] = _datetime_to_string(self._creation_datetime)

        return data

    @staticmethod
    def from_data(data):
        """Construct from a JSON-deserialised dictionary"""
        if data is None or len(data) == 0:
            return Datasource()

        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime
        d = Datasource()

        d._uuid = data["UUID"]
        d._name = data["name"]
        d._creation_datetime = _string_to_datetime(data["creation_datetime"])

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
        from hugs_objstore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        datasource_key = "%s/uuid/%s" % (Datasource._datasource_root, self._uuid)
        _ObjectStore.set_object_from_json(bucket=bucket, key=datasource_key, data=self.to_data())
        
        encoded_name = _string_to_encoded(self._name)
        name_key = "%s/name/%s/%s" % (Datasource._datasource_root,
                                      encoded_name, self._uuid)
        _ObjectStore.set_string_object(bucket=bucket, key=name_key, string_data=self._uuid)



    @staticmethod
    def load(bucket=None, uuid=None, name=None):
        """ Load a Datasource from the object store either by name or UID

            Args:
                bucket (dict, default=None): Bucket to store object
                uuid (str, default=None): UID of Datasource to load
                name (str, default=None): Name of Datasource
            Returns:
                Datasource: Datasource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from hugs_objstore import get_bucket as _get_bucket

        if uuid is None and name is None:
            raise ValueError("Both uuid and name cannot be None")

        if bucket is None:
            bucket = _get_bucket()
        if uuid is None:
            uuid = Datasource._get_uid_from_name(bucket=bucket, name=name)

        key = "%s/uuid/%s" % (Datasource._datasource_root, uuid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return Datasource.from_data(data)

    def get_values(self, bucket, datetime_begin, datetime_end):
        """ Get all values for this Datasource stored in the object store

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
            prefix = "%s/%s/%s" % (Datasource._datavalues_root, self._uuid, year)
            
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

        prefix = "%s/name/%s", (Datasource._datasource_root, encoded_name)

        uuid = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

        if len(uuid) > 1:
            raise ValueError("There should only be one Datasource associated with this name")
        
        return uuid[0]

    
