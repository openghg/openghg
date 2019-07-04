__all__ = ["Cache"]


class Cache:
    def __init__(self):
        self._uuid = None
        self._creation_datetime = None
        self._records = {}
    
    @staticmethod
    def create(label):
        """ Create a new Cache object

            Args:
                label: Label this cache. This label should
                describe which data type etc this cache belongs to
            Returns:
                Cache: Cache object
        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        c = Cache()
        c._uuid = _create_uuid()
        c._creation_datetime = _get_datetime_now()
        c._label = label

        return c

    def is_null(self):
        """Return whether this object is null
        
            Returns:
                bool: True if object is null
        """
        return self._uuid is None

    def lookup(self, search_string):
        """ Check the records

            Args:
                search_string (str): String to check for in records
            Returns:
                str or bool: If searching_string in keys, return the record at that
                key else return False
        """
        if search_string in self._labels:
            return self._labels[search_string]
        else:
            return False

    def add_record(self, key, value):
        """ Add record to the cache

            Args:
                key (str): Key for dict
                value (str): Value for dict
            Returns:
                None
        """
        self._records[key] = value

    def delete_record(self, key):
        """ Delete record from the cache

            Args:
                key (str): Key for dict
            Returns:
                str or bool: Value of key if key in dict else False
        """
        return self._records.pop(key, False)

    def records():
        """ Return the records stored in this Cache object

            Returns:
                dict: Records stored in cache
        """
        return self._records
