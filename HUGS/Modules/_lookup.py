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
        # Keyed by name - allows retrieval of UUID from name
        self._name_records = {}
        # Keyed by UUID - allows retrieval of name by UUID
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

        return Lookup.from_data(data=data, bucket=b ucket)

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

    def get_id(self, source_name):
        """ Returns the UUID of the Datasource with name given by source_name

            Args:
                source_name (str): Name of Datasource
            Returns:
                str or bool: UUID of Datasource if found, else False
        """
        self._name_records.get(source_name, False)

    def get_name(self, source_id):
        """ Returns the name of the Datasource with UUID given by source_id
        
            Args:
                source_id (str): UUID of Datasource
            Returns:
                str or bool: Name of Datasource if found, else False
        """
        self._uuid_records.get(source_id, False)

    def source_exists(self, source_name=None, source_id=None):
        """ Check if the source already exists on record

            Args:
                source_name (str): Name of Datasource
                source_id (str): UUID of Datasource
            Returns:
                bool: True if exists
        """
        if source_name is None and source_id is None:
            raise ValueError("source_name or source_id must be provided")
        
        if source_name:
            source_name = source_name.lower()
            return self._name_records.get(source_name, False)

        if source_id:
            return self._uuid_records.get(source_id, False)
    
    def set_id(self, source_name, source_id=None, overwrite=False):
        """ Set a source's ID and name

            Args:
                source_name (str): UUID for source
                source_id (str, default=None): UUID to assign to object
            Returns:
                str: UUID of record
        """
        if source_name in self._name_records and overwrite is False:
            raise ValueError("Cannot overwrite record")

        if source_id is None:
            import uuid
            source_id = uuid.uuid4()

        source_name = source_name.lower()
        self._uuid_records[source_id] = source_name
        self._name_records[source_name] = source_id

        return source_id

    def add_timeseries(self, source_name, species):
        """ Takes a source name and a list of species and creates
            a UUID for each Datasource

            Args:   
                source_name (str): Name of source
                species (list): List of species names
            Returns
                dict: Dictionary keyed by Datasource name where name is
                source_name + species. Values are the UUIDs for each Datasource
        """
        if not isinstance(species, list):
            species = [species]
        
        source_name = source_name
        records = {}

        for sp in species:
            datasource_name = source_name + "_" + sp
            uid = self.set_id(datasource_name)
            records[datasource_name.lower()] = uid

        return records

    def add_footprint(self, source_name):
        """ Takes a source name and creates a record for the footprint data

            Args:   
                source_name (str): Name of source
            Returns:
                dict: Dictionary keyed by Datasource name. Value is the UUID for 
                the Datasource
        """
        source_name = source_name.lower()
        uid = self.set_id(source_name)

        return {source_name: uid}

        


        
        

                

        







