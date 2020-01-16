""" This file contains the BaseModule class from which other processing
    modules inherit.
"""

class BaseModule:
    def is_null(self):
        return not self.datasources

    @classmethod
    def exists(cls, bucket=None):
        """ Check if a GC object is already saved in the object 
            store

            Args:
                bucket (dict, default=None): Bucket for data storage
            Returns:
                bool: True if object exists
        """
        from HUGS.ObjectStore import exists, get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = f"{cls._root}/uuid/{cls._uuid}"

        return exists(bucket=bucket, key=key)

    @classmethod
    def from_data(cls, data, bucket=None):
        """ Create an object from data

            Args:
                data (str): JSON data
                bucket (dict, default=None): Bucket for data storage
            Returns:
                cls: Class object of cls type
        """
        from Acquire.ObjectStore import string_to_datetime
        from HUGS.ObjectStore import get_bucket

        if not data:
            raise ValueError("Unable to create object with empty dictionary")

        if bucket is None:
            bucket = get_bucket()

        c = cls()
        c._creation_datetime = string_to_datetime(data["creation_datetime"])
        c._datasource_uuids = data["datasource_uuids"]
        c._datasource_names = data["datasource_names"]
        c._file_hashes = data["file_hashes"]
        c._stored = False

        return c

    @classmethod
    def load(cls, bucket=None):
        """ Load a CRDS object from the datastore using the passed
            bucket and UUID

            Args:
                inst (CRDS): CRDS instance
                bucket (dict, default=None): Bucket to store object
            Returns:
                Datasource: Datasource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore
        from HUGS.ObjectStore import get_bucket

        if not cls.exists():
            return cls()
        
        if bucket is None:
            bucket = get_bucket()

        key = f"{cls._root}/uuid/{cls._uuid}"
        data = ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return cls.from_data(data=data, bucket=bucket)
    
    @classmethod
    def uuid(cls):
        """ Return the UUID of this object

            Returns:
                str: UUID of object
        """
        return cls._uuid

    def add_datasources(self, datasource_uuids):
        """ Add the passed list of Datasources to the current list

            Args:
                datasource_uuids (dict): Dict of Datasource UUIDs
            Returns:
                None
        """
        self._datasource_names.update(datasource_uuids)
        # Invert the dictionary to update the dict keyed by UUID
        uuid_keyed = {v: k for k, v in datasource_uuids.items()}
        self._datasource_uuids.update(uuid_keyed)

    def datasources(self):
        """ Return the list of Datasources for this object

            Returns:
                list: List of Datasources
        """
        return list(self._datasource_uuids.keys())

    def remove_datasource(self, uuid):
        """ Remove the Datasource with the given uuid from the list 
            of Datasources

            Args:
                uuid (str): UUID of Datasource to be removed
        """
        del self._datasource_uuids[uuid]

    def clear_datasources(self):
        """ Remove all Datasources from the object

            Returns:
                None
        """
        self._datasource_uuids.clear()
        self._datasource_names.clear()
        self._file_hashes.clear()
