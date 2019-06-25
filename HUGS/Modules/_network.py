""" For a whole network

"""

__all__ = ["Network"]

class Network:
    """ A Network has Sites

        Instances of this class should be created using the
        Network.create() function

    """
    _network_root = "network"

    def __init__(self):
        """ Creates a null network """
        self._uuid = None
        self._name = None
        self._description = None
        self._sites = None

    @staticmethod
    def create(name, description):
        """ Create a Network instance

            Args:
                name (str): Name of network
                description (str): Description of network
        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        n = Network()
        
        n._uuid = _create_uuid()
        n._name = name
        n._creation_datetime = _get_datetime_now()
        n._description = description
        n._sites = {}

        return n

    def is_null(self):
        """ Check if this is a null object instance

            Returns:
                bool: True if object null
        """
        return self._uuid is None
    
    def to_data(self):
        """ Creates a JSON serialisable dictionary for
            storing in the object store

            Returns:
                dict: Dictionary of object
        """
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        d = {}
        d["name"] = self._name
        d["UUID"] = self._uuid
        d["description"] = self._description
        d["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        d["sites"] = self._sites

        return d

    @staticmethod
    def from_data(data):
        """ Create a Network object from data

            Args:  
                data: JSON data
            Returns:
                Network: Network object
        """
        if data is None or len(data) == 0:
            return Network()

        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime

        n = Network()
        n._uuid = data["UUID"]
        n._name = data["name"]
        n._description = data["description"]
        n._creation_datetime = _string_to_datetime(data["creation_datetime"])
        n._sites = data["sites"]

        return n

    def save(self, bucket=None):
        """ Save this Site as a JSON object on the object store

            Args:
                bucket (dict, default=None): Bucket to hold data
            Returns:
                None
        """

        if self.is_null():
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()
        
        network_key = "%s/uuid/%s" % (Network._network_root, self._uuid)
        _ObjectStore.set_object_from_json(bucket=bucket, key=network_key, data=self.to_data())

        encoded_name = _string_to_encoded(self._name)
        string_key = "%s/name/%s/%s" % (Network._network_root, encoded_name, self._uuid)
        _ObjectStore.set_string_object(bucket=bucket, key=string_key, string_data=self._uuid)
            
    @staticmethod
    def load(bucket=None, uuid=None, name=None):
        """ Load an Network instance from JSON serialised data
            in the object store

             Args:
                bucket (dict, default=None): Bucket to hold data
                uuid (str, default=None): UUID of Network
                name (str, default=None): Name of Network
            Returns:
                Network: Network object
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if uuid is None and name is None:
            raise ValueError("Both uuid and name cannot be None")

        if bucket is None:
            bucket = _get_bucket()
        if uuid is None:
            uuid = Network._get_uid_from_name(bucket=bucket, name=name)

        key = "%as/uuid/%s" % (Network._network_root, uuid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return Network.from_data(data)

    @staticmethod
    def _get_uid_from_name(bucket, name):
        """ Returns the UUID associated with the named Network

            Args:
                bucket (dict): Bucket holding data
                name (str): Name to search
            Returns:
                str: UUID for site
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded

        encoded_name = _string_to_encoded(name)
        prefix = "%s/name/%s" % (Network._network_root, encoded_name)
        uuid = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

        if len(uuid) > 1:
            raise ValueError("There should only be one Network associated with this name")

        return uuid[0]

    def create_site(self, name, location, latlong):
        """ Used to create a Site on this Network

            Args:
                name (str): Name of site
                location (str): Location of site
                latlong (tuple(float,float)): Latitude and longitude of Site
            Returns:
                Site: Site object on this Network
        """
        from _site import Site

        site = Site.create(name=name, location=location, latlong=latlong, network=self._name)

        self._sites[site._uuid] = {"name": name, "created": site._creation_datetime}

        return site


        




