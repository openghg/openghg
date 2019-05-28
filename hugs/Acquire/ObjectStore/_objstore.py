
import io as _io
import datetime as _datetime
import uuid as _uuid
import json as _json
import os as _os

__all__ = ["ObjectStore", "set_object_store_backend",
           "use_testing_object_store_backend",
           "use_oci_object_store_backend"]

_objstore_backend = None


def use_testing_object_store_backend(backend):
    from ._testing_objstore import Testing_ObjectStore as _Testing_ObjectStore
    set_object_store_backend(_Testing_ObjectStore)
    bucket = "%s/testing_objstore" % backend

    try:
        # make sure that this directory exists if it doesn't already
        _os.mkdir(bucket)
    except:
        pass

    return bucket


def use_oci_object_store_backend():
    from ._oci_objstore import OCI_ObjectStore as _OCI_ObjectStore
    set_object_store_backend(_OCI_ObjectStore)


class ObjectStore:
    @staticmethod
    def create_bucket(bucket, bucket_name, compartment=None):
        """Create and return a new bucket in the object store called
           'bucket_name', optionally placing it into the compartment
           identified by 'compartment'. This will raise an
           ObjectStoreError if this bucket already exists

           Args:
                bucket (dict): Bucket to store data in
                bucket_name (str): Name for bucket
                compartment (str, default=None): Compartment to store
                the bucket

           Returns:
                dict: New bucket
        """
        return _objstore_backend.create_bucket(bucket, bucket_name,
                                               compartment)

    @staticmethod
    def get_bucket(bucket, bucket_name, compartment=None,
                   create_if_needed=True):
        """Find and return a new bucket in the object store called
           'bucket_name', optionally placing it into the compartment
           identified by 'compartment'. If 'create_if_needed' is True
           then the bucket will be created if it doesn't exist. Otherwise,
           if the bucket does not exist then an exception will be raised.
           
           Args:
                bucket (dict): Bucket to store data in
                bucket_name (str): Name for bucket
                compartment (str, default=None): Compartment to store
                the bucket
                create_if_needed (bool, default=True): Create new bucket if
                true, don't if False

            Returns:
                dict: Requested bucket

        """
        return _objstore_backend.get_bucket(bucket, bucket_name,
                                            compartment, create_if_needed)

    @staticmethod
    def get_bucket_name(bucket):
        """Return the name of the passed bucket"""
        return _objstore_backend.get_bucket_name(bucket)

    @staticmethod
    def is_bucket_empty(bucket):
        """Return whether or not the passed bucket is empty"""
        return _objstore_backend.is_bucket_empty(bucket)

    @staticmethod
    def delete_bucket(bucket, force=False):
        """Delete the passed bucket. This should be used with caution.
           Normally you can only delete a bucket if it is empty. If
           'force' is True then it will remove all objects/pars from
           the bucket first, and then delete the bucket. This
           can cause a LOSS OF DATA!
        """
        return _objstore_backend.delete_bucket(bucket=bucket, force=force)

    @staticmethod
    def create_par(bucket, encrypt_key, key=None, readable=True,
                   writeable=False, duration=3600, cleanup_function=None):
        """Create a pre-authenticated request for the passed bucket and
           key (if key is None then the request is for the entire bucket).
           This will return a PAR object that will contain a URL that can
           be used to access the object/bucket. If writeable is true, then
           the URL will also allow the object/bucket to be written to.
           PARs are time-limited. Set the lifetime in seconds by passing
           in 'duration' (by default this is one hour). Note that you must
           pass in a public key that will be used to encrypt this PAR. This is
           necessary as the PAR grants access to anyone who can decrypt
           the URL
        """
        from Acquire.Client import PAR as _PAR

        par = _objstore_backend.create_par(
                    bucket=bucket, encrypt_key=encrypt_key, key=key,
                    readable=readable, writeable=writeable,
                    duration=duration, cleanup_function=cleanup_function)

        if not isinstance(par, _PAR):
            raise TypeError("A create_par request should always return an "
                            "value of type PAR: %s is not correct!" % par)

        return par

    @staticmethod
    def close_par(par=None, par_uid=None, url_checksum=None):
        """Close the passed PAR, which provides access to data in the
           passed bucket
        """
        _objstore_backend.close_par(par=par,
                                    par_uid=par_uid,
                                    url_checksum=url_checksum)

    @staticmethod
    def get_object(bucket, key):
        """Return the binary data contained in the key 'key' in the
           passed bucket"""
        return _objstore_backend.get_object(bucket, key)

    @staticmethod
    def get_object_as_file(bucket, key, filename):
        """Get the object contained in the key 'key' in the passed 'bucket'
           and writing this to the file called 'filename'"""
        data = ObjectStore.get_object(bucket, key)

        with open(filename, "wb") as FILE:
            FILE.write(data)

    @staticmethod
    def get_string_object(bucket, key):
        """Return the string in 'bucket' associated with 'key'"""
        data = ObjectStore.get_object(bucket, key)
        return data.decode("utf-8")

    @staticmethod
    def get_object_from_json(bucket, key):
        """Return an object constructed from json stored at 'key' in
           the passed bucket. This returns None if there is no data
           at this key
        """
        data = ObjectStore.get_string_object(bucket, key)
        return _json.loads(data)

    @staticmethod
    def take_object(bucket, key):
        """Take (delete) the object from the object store, returning
           the object
        """
        return _objstore_backend.take_object(bucket, key)

    @staticmethod
    def take_string_object(bucket, key):
        """Take (delete) the string object from the object store, returning
           the object
        """
        data = ObjectStore.take_object(bucket, key)
        return data.decode("utf-8")

    @staticmethod
    def take_object_as_file(bucket, key, filename):
        """Take (delete) the object contained in the
           key 'key' in the passed 'bucket'
           and writing this to the file called 'filename'
        """
        data = ObjectStore.take_object(bucket, key)

        with open(filename, "wb") as FILE:
            FILE.write(data)

    @staticmethod
    def take_object_from_json(bucket, key):
        """Take (delete) the object from the object store, returning
           the json-deserialised object
        """
        data = ObjectStore.take_string_object(bucket, key)
        return _json.loads(data)

    @staticmethod
    def get_all_object_names(bucket, prefix=None):
        """Returns the names of all objects in the passed bucket"""
        return _objstore_backend.get_all_object_names(bucket, prefix)

    @staticmethod
    def get_all_objects(bucket, prefix=None):
        """Return all of the objects in the passed bucket"""
        objects = {}
        names = ObjectStore.get_all_object_names(bucket, prefix)

        for name in names:
            objects[name] = ObjectStore.get_object(bucket, name)

        return objects

    @staticmethod
    def get_all_objects_from_json(bucket, prefix=None):
        """Return all of the objects in the passed bucket as
           json-deserialised objects
        """
        objects = ObjectStore.get_all_objects(bucket, prefix)

        names = list(objects.keys())

        for name in names:
            try:
                s = objects[name].decode("utf-8")
                objects[name] = _json.loads(s)
            except:
                del objects[name]

        return objects

    @staticmethod
    def get_all_strings(bucket, prefix=None):
        """Return all of the strings in the passed bucket"""
        objects = ObjectStore.get_all_objects(bucket, prefix)

        names = list(objects.keys())

        for name in names:
            try:
                s = objects[name].decode("utf-8")
                objects[name] = s
            except:
                del objects[name]

        return objects

    @staticmethod
    def set_object(bucket, key, data):
        """Set the value of 'key' in 'bucket' to binary 'data'"""
        _objstore_backend.set_object(bucket, key, data)

    @staticmethod
    def set_object_from_file(bucket, key, filename):
        """Set the value of 'key' in 'bucket' to equal the contents
           of the file located by 'filename'"""
        ObjectStore.set_object(bucket, key,
                               open(filename, 'rb').read())

    @staticmethod
    def set_ins_object_from_json(bucket, key, data):
        """Set the value of 'key' in 'bucket' to equal to contents
           of 'data', which has been encoded to json, if (and only if)
           this key has not already been set (ins = 'if not set').
           This returns the object at this key after the operation
           (either the set object or the value that was previously
           set
        """
        from Acquire.ObjectStore import Mutex as _Mutex
        m = _Mutex(bucket=bucket, key=key)

        try:
            old_data = ObjectStore.get_object_from_json(bucket, key)
        except:
            old_data = None

        if old_data is not None:
            m.unlock()
            return old_data
        else:
            try:
                ObjectStore.set_object_from_json(bucket, key, data)
            except:
                m.unlock()
                raise

            return data

    @staticmethod
    def set_ins_string_object(bucket, key, string_data):
        """Set the value of 'key' in 'bucket' to the string 'string_data',
           if (and only if) this key has not already been set
           (ins = 'if not set'). This returns the object at this
           key after the operation (either the set string, or the value
           that was previously set)
        """
        from Acquire.ObjectStore import Mutex as _Mutex
        m = _Mutex(bucket=bucket, key=key)

        try:
            val = ObjectStore.get_string_object(bucket, key)
        except:
            val = None

        if val is not None:
            m.unlock()
            return val
        else:
            try:
                ObjectStore.set_string_object(bucket, key, string_data)
            except:
                m.unlock()
                raise

            return string_data

    @staticmethod
    def set_string_object(bucket, key, string_data):
        """Set the value of 'key' in 'bucket' to the string 'string_data'"""
        ObjectStore.set_object(bucket, key,
                               string_data.encode("utf-8"))

    @staticmethod
    def set_object_from_json(bucket, key, data):
        """Set the value of 'key' in 'bucket' to equal to contents
           of 'data', which has been encoded to json"""
        ObjectStore.set_string_object(bucket, key, _json.dumps(data))

    @staticmethod
    def delete_all_objects(bucket, prefix=None):
        """Deletes all objects..."""
        _objstore_backend.delete_all_objects(bucket, prefix)

    @staticmethod
    def delete_object(bucket, key):
        """Removes the object at 'key'"""
        _objstore_backend.delete_object(bucket, key)

    @staticmethod
    def clear_all_except(bucket, keys):
        """Removes all objects from the passed 'bucket' except those
           whose keys are or start with any key in 'keys'
        """
        names = ObjectStore.get_all_object_names(bucket)

        for name in names:
            remove = True

            for key in keys:
                if name.startswith(key):
                    remove = False
                    break

            if remove:
                ObjectStore.delete_object(bucket, key)

    @staticmethod
    def get_size_and_checksum(bucket, key):
        """Return the object size (in bytes) and checksum of the
           object in the passed bucket at the specified key
        """
        return _objstore_backend.get_size_and_checksum(bucket, key)


def set_object_store_backend(backend):
    """Set the backend that is used to actually connect to
       the object store. This can only be set once in the program!
    """
    global _objstore_backend

    if backend == _objstore_backend:
        return

    if _objstore_backend is not None:
        from Acquire.ObjectStore import ObjectStoreError
        raise ObjectStoreError("You cannot change the object store "
                               "backend once it has been already set!")

    _objstore_backend = backend
