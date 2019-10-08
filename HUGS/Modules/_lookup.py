__all___ = ["Lookup"]


class Lookup:
    """ This class handles the lookup of Datasource names and UUIDs
        for processing and searching of data

    """
    _lookup_root = "lookup"
    _lookup_uuid = "46582ddb-look-upa0-a0f4-6017cd8ea0e0"

    def __init__(self):
        self._creation_datetime = None
        self._stored = False
        self._name_records = {}
        self._uuid_records = {}

    # This will be moved out to the template / util module
    def is_null():
        return self._records is None

    @staticmethod
    def exists(bucket=None):
        """ Check if a Lookup object is already saved in the object
            store

            Args:
                bucket (dict, default=None): Bucket for data storage
            Returns:
                bool: True if object exists
        """
        from HUGS.ObjectStore import exists as exists
        from HUGS.ObjectStore import get_bucket as get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = "%s/uuid/%s" % (Lookup._lookup_root, Lookup._lookup_uuid)
        return _exists(bucket=bucket, key=key)

    @staticmethod
    def create():
        """ This function should be used to create Lookup objects

            Returns:
                Lookup: Lookup object 
        """
        from Acquire.ObjectStore import get_datetime_now

        lookup = Lookup()
        lookup._creation_datetime = get_datetime_now()

        return lookup

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        if self.is_null():
            return {}

        from Acquire.ObjectStore import datetime_to_string

        data = {}
        data["creation_datetime"] = datetime_to_string(self._creation_datetime)
        data["stored"] = self._stored
        data["records"] = self._records

        return data

    @staticmethod
    def from_data(data, bucket=None):
        """ Create a Lookup object from data

            Args:
                data (dict): JSON data
                bucket (dict, default=None): Bucket for data storage
        """
        from Acquire.ObjectStore import string_to_datetime

        if data is None or len(data) == 0:
            return Lookup()

        lookup = Lookup()
        lookup._creation_datetime = string_to_datetime(data["creation_datetime"])
        lookup._records = data["records"]
        lookup._stored = False
        
        return lookup

    def save(self, bucket=None):
        """ Save this Lookup object in the object store

            Args:
                bucket (dict): Bucket for data storage
            Returns:
                None
        """
        if self.is_null():
            return

        from Acquire.ObjectStore import ObjectStore
        from Acquire.ObjectStore import string_to_encoded
        from HUGS.ObjectStore import get_bucket

        if bucket is None:
            bucket = get_bucket()

        self._stored = True
        lookup_key = "%s/uuid/%s" % (Lookup._lookup_root, Lookup._lookup_uuid)
        _ObjectStore.set_object_from_json(bucket=bucket, key=lookup_key, data=self.to_data())

    @staticmethod
    def load(bucket=None):
        """ Load a Lookup object from the object store

            Args:
                bucket (dict, default=None): Bucket to store object
            Returns:
                Datasource: Datasource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if not Lookup.exists():
            return Lookup.create()

        if bucket is None:
            bucket = _get_bucket()

        key = "%s/uuid/%s" % (Lookup._lookup_root, Lookup._lookup_uuid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return Lookup.from_data(data=data, bucket=bucket)

    def lookup(self, source_id=None, source_name=None):
        """ This function provides the interface to the underlying dict which stores the
            relationships between each Datasource

            Args:
                source_id (str): ID of source
                source_name (str): Name of source
            Returns:
                dict: Dictionary providing data on relationships between Datasources
        """
        results = {}

        if source_id is None and source_name is None:
            raise ValueError("source_id or source_name must be provided")

        results["id"] = self._uuid_records.get(source_id, False)
        results["name"] = self._name_records.get(source_name, False)
            
        return results
    
    def set_id(self, source_name, source_id=None):
        """ Set a source's ID and name

            Args:
                source_name (str): UUID for source

        """


        
        
    def build_structure(self):
        """ Build a relationship structure between the passed file/data ?

        """
        raise NotImplementedError()



    def ensure_sync(): 
        """ Ensures the two dictionary's are synced for fast lookup
            by key

            Returns:
                None
        """
        raise NotImplementedError()


                

        







