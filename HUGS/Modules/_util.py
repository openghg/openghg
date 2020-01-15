""" 
Functions that are useful to each of the processing classes
"""


def is_null(obj):
        """ Check if this object has any Datasources assigned to it, if
            it does not it is a null object.

            Returns:
                bool: True if object is null
        """
        return len(obj._datasource_uuids) == 0

def exists(module_name, bucket=None):
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

    key = "%s/uuid/%s" % (GC._gc_root, GC._gc_uuid)
    return exists(bucket=bucket, key=key)
