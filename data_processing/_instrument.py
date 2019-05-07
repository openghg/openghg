""" Describes a single instrument at a site
    An instrument contains DataSources

    DataSources could also be called sensors

"""

class Instrument:
    """


    """
    def __init__(self):
        """ This only creates a null Instrument
            Create should be used to create Instrument objects
        """
        self._uid = None
        self._instrument_root = "instruments"
    
    @staticmethod
    def create(self, name, site, network):
        """ Creates an Instrument object

            Args:
                name (str): Name of instrument
                site (str): Site at which instrument is based
                network (str): Network site associated with
            Returns:
                Instrument: Instrument object
        """
        from Acquire.ObjectStore import create_uuid as _create_uuid

        inst = Instrument()

        inst._uid = _create_uuid()
        inst._site = site
        inst._network = network

        return inst

    def is_null(self):
        """ Check if this is a null Instrument

            Returns:
                bool: True if null
        """
        return self._uid is None

    def save(self, bucket=None):
        """ Save this Instrument as a JSON object on the object store
    
            This function also saves the name and UID of the Instrument
            to the object store with a key

            {instrument_root}/name/{instrument_name}/{instrumen_uid}
            
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

        key = "%s/uid/%s", (self._instrument_root, self._uid)

        _ObjectStore.set_object_from_json(bucket=bucket, key=key, data=self.to_data())

        encoded_name = _string_to_encoded(self._name)

        key = "%s/name/%s/%s" % (self._instrument_root, encoded_name, self._uid)

        _ObjectStore.set_string_object(bucket=bucket, key=key, string_data=self._uid)

    @staticmethod
    def load(bucket=None, uid=None, name=None):
        """ Load instance of Instrument from JSON serialised data
            in the object store

            Returns:
                Instrument: Instrument object
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        if uid is None and name is None:
            raise ValueError("Both uid and name cannot be None")

        if bucket is None:
            bucket = DataSource._get_bucket(bucket)
        if uid is None:
            uid = DataSource._get_uid_from_name(bucket=bucket, name=name)

        key = "%s/uid/%s" % (self._instrument_root, uid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return DataSource.from_data(data)


    # Need the DataSources associated with this Instrument
    def get_datasources(self):
        """ Get all the DataSources associated with this Instrument

            Returns:
                list: List of UIDs of DataSources
        """        
        import Acquire.ObjectStore as _ObjectStore
        
        prefix = "%s/uid/%s/datasources" % (self._instrument_root, self._uid)
        # Get all keys that start with the prefix
        return _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)








        










        
