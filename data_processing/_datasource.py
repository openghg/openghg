__all___ = ["DataSource"]

_datasource_root = "datasources"
_datavalues_root = "values"


class DataSource:
    """Docs"""
    def __init__(self):
        """Construct a null DataSource"""
        self._uid = None

    @staticmethod
    def create(name=None):
        """Create a new datasource
        
            Args:
                name (str, default=None): Name for DataSource
            Returns:
                DataSource: Datasource object
                
        """        
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        d = DataSource()
        d._uid = _create_uuid()
        d._name = name
        d._creation_datetime = _get_datetime_now()

        return d

    def is_null(self):
        """Return whether this object is null
        
            Returns:
                bool: True if object is null
        """
        return self._uid is None

    def get_values(self, bucket, datetime_begin, datetime_end):
        """ Get all values for this DataSource stored in the object store

        """
        from Acquire.ObjectStore import datetime_to_datetime as _datetime_to_datetime

        datetime_begin = _datetime_to_datetime(datetime_begin)
        datetime_end = _datetime_to_datetime(datetime_end)

        year_begin = datetime_begin.year
        year_end = datetime_end.year

        keys = []

        for year in range(year_begin, year_end+1):
            prefix = "%s/%s" % (_datavalues_root, year)
            
            datakeys = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)
            
            for datakey in datakeys:
                start, end = _string_to_daterange(datakey.split("/")[-1])

                if in_range:
                    keys.append(datakey)

        values = []

        for key in keys:
            read hdf fle from object_store
            add data within required daterange to values

        return values

    @staticmethod
    def _get_bucket(bucket):
        if bucket is None:
            from Hugs import get_hugs_bucket as _get_hugs_import
            return _get_hugs_bucket()
        else:
            return bucket

    def save(self, bucket=None):
        """Save this datasource to the object store"""
        if self.is_null():
            return

        bucket = DataSource._get_bucket(bucket)

        key = "%s/uid/%s" % (_datasource_root, self._uid)

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        _ObjectStore.set_object_from_json(bucket=bucket, key=key, 
                                          data=self.to_data())

        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        encoded_name = _string_to_encoded(self._name)

        key = "%s/name/%s/%s" % (_datasource_root, encoded_name, self._uid)
        _ObjectStore.set_string_object(bucket=bucket, key=key, 
                                       string_data=self._uid)

    @staticmethod
    def _get_name_from_uid(bucket, name):
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        encoded_name = _string_to_encoded(name)

        prefix = "%s/name/%s" % (_datasource_root, encoded_name)
        keys = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

        if len(keys) > 1:
            raise KeyError(....)

        return keys.split("/")[-1]

    @staticmethod
    def load(uid=None, name=None, bucket=None):
        """Load a ds from objectstore either by name or UID"""
        bucket = DataSource._get_bucket(bucket)

        if uid is None:
            uid = DataSource._get_name_from_uid(bucket=bucket, name=name)

        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        key = "%s/uid/%s" % (_datasource_root, uid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return DataSource.from_data(data)

    def to_data(self):
        """Retrun a json-serialisable object..."""
        if self.is_null():
            return {}

        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        data = {}
        data["uid"] = self._uid
        data["name"] = self._name
        data["creation_datetime"] = _datetime_to_string(self._creation_datetime)

        return data

    @staticmethod
    def from_data(data):
        """Construct from a json-deserialised dictionary"""
        if data is None or len(data) == 0:
            return DataSource()

        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime
        d = DataSource()

        d._uid = data["uid"]
        d._name = data["name"]
        d._creation_datetime = _string_to_datetime(data["creation_datetime"])

        return d
