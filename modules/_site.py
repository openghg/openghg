""" At each site there are instruments, possibly multiple instruments

"""
__all__ = ["Site"]

class Site:
    """ Holds the data about a site at which
        there are instruments

        Instances of this class should be created using the
        Site.create() function

    """
    _site_root = "site"

    def __init__(self):
        """ This creates a null Site """
        self._name = None
        self._uuid = None
        self._creation_datetime = None
        self._location = None
        self._latlong = None
        self._network = None
        self._instruments = None

        # Type of site plane, boat etc
        # This can be used for output or reading in and processing of datsources
        self._type

    @staticmethod
    def create(name, location, latlong, network):
        """ Returns a Site object

            Args:
                name (str): Name of site
                location (str): Location of site
                latlong (tuple(str,str)): Latitude and longitude of site
                network (str): Network site attached to
            Returns:
                Site: Site object
        """

        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        s = Site()
        s._name = name
        s._uuid = _create_uuid()
        s._creation_datetime = _get_datetime_now()
        s._location = location
        s._latlong = latlong
        s._network = network
        s._instruments = {}

        return s

    def is_null(self):
        """ Check if this is a null object instance

            Returns:
                bool: True if object is null

        """
        return self._uuid is None

    def to_data(self):
        """ Creates a JSON serialisable dictionary for storing
            in the object store
            
            Returns:
                dict: Dictionary of object
        """ 
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        d = {}
        d["name"] = self._name
        d["UUID"] = self._uuid
        d["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        d["location"] = self._location
        d["latlong"] = self._latlong
        d["network"] = self._network
        d["instruments"] = self._instruments

        return d

    @staticmethod
    def from_data(data):
        """ Creates a Site object from data

            Args:
                data: JSON data
            Returns:
                Site: Site object
        """
        if data is None or len(data) == 0:
            return Site()

        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime

        s = Site()
        s._name = data["name"]
        s._uuid = data["UUID"]
        s._creation_datetime = _string_to_datetime(data["creation_datetime"])
        s._location = data["location"]
        s._latlong = data["latlong"]
        s._network = data["network"]
        s._instruments = data["instruments"]

        return s
        
    def save(self, bucket=None):
        """ Save this Site object as a JSON to the object store

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

        # Key at which to save this object
        site_key = "%s/uuid/%s" % (Site._site_root, self._uuid)
        _ObjectStore.set_object_from_json(bucket=bucket, key=site_key, data=self.to_data())

        # Save the UID of the object so its UID can be found by name
        encoded_name = _string_to_encoded(self._name)
        name_key = "%s/name/%s/%s" % (Site._site_root, encoded_name, self._uuid)
        _ObjectStore.set_string_object(bucket=bucket, key=name_key, string_data=self._uuid)

    @staticmethod
    def load(bucket=None, uuid=None, name=None):
        """ Load a Site from the object store either by name or by UID

            Args:
                bucket (dict, default=None): Bucket containing data
                uuid (str, default=None): UID for Site object
                name (str, default=None): Name of Site object
            Returns:
                Site: Site object
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from hugs_objstore import get_bucket as _get_bucket

        if uuid is None and name is None:
            raise ValueError("Both uuid and name cannot be None")

        if bucket is None:
            bucket = _get_bucket()
        if uuid is None:
            uuid = Site._get_uid_from_name(bucket=bucket, name=name)

        key = "%s/uuid/%s" % (Site._site_root, uuid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return Site.from_data(data)

    @staticmethod
    def _get_uid_from_name(bucket, name):
        """ Returns the UUID associated with the named Site

            Args:
                bucket (dict): Bucket holding data
                name (str): Name to search
            Returns:
                str: UUID for the Site
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded

        encoded_name = _string_to_encoded(name)

        prefix = "%s/name/%s" % (Site._site_root, encoded_name)

        uuid = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

        if len(uuid) > 1:
            raise ValueError("There should only be one Site associated with this name")
        
        return uuid[0]
        
    def create_Instrument(self, name, height=None):
        """ This function is used to create an Instrument at this Site

            Returns:
                Instrument: Instrument object at this site
        """
        from _instrument import Instrument

        instrument = Instrument.create(name=name, site=self._name, network=self._network, height=height)

        self._instruments[instrument._uuid] = {"name": name, "created": instrument._creation_datetime}

    def type_fn(self):
        """ Implement a type variable so we know what type of site this is? 
            Can be boat, land, satellite etc

        """

        pass
