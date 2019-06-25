
import os as _os
import shutil as _shutil
import datetime as _datetime
import uuid as _uuid
import json as _json
import glob as _glob
import threading
import uuid as _uuid

_rlock = threading.RLock()

__all__ = ["Testing_ObjectStore"]


def _get_driver_details_from_par(par):
    from Acquire.ObjectStore import datetime_to_string \
        as _datetime_to_string

    import copy as _copy
    details = _copy.copy(par._driver_details)

    if details is None:
        return {}
    else:
        # fix any non-string/number objects
        details["created_datetime"] = _datetime_to_string(
                                        details["created_datetime"])

    return details


def _get_driver_details_from_data(data):
    from Acquire.ObjectStore import string_to_datetime \
        as _string_to_datetime

    import copy as _copy
    details = _copy.copy(data)

    if "created_datetime" in details:
        details["created_datetime"] = _string_to_datetime(
                                            details["created_datetime"])

    return details


class Testing_ObjectStore:
    """This is a dummy object store that writes objects to
       the standard posix filesystem when running tests
    """
    @staticmethod
    def create_bucket(bucket, bucket_name, compartment=None):
        """Create and return a new bucket in the object store called
           'bucket_name', optionally placing it into the compartment
           identified by 'compartment'. This will raise an
           ObjectStoreError if this bucket already exists
        """
        bucket_name = str(bucket_name)

        if compartment is not None:
            if compartment.endswith("/"):
                bucket = compartment
            else:
                bucket = "%s/" % compartment

        full_name = _os.path.join(_os.path.split(bucket)[0], bucket_name)

        if not _os.path.exists(full_name):
            _os.makedirs(full_name)

        return full_name

    @staticmethod
    def get_bucket(bucket, bucket_name, compartment=None,
                   create_if_needed=True):
        """Find and return a new bucket in the object store called
           'bucket_name', optionally placing it into the compartment
           identified by 'compartment'. If 'create_if_needed' is True
           then the bucket will be created if it doesn't exist. Otherwise,
           if the bucket does not exist then an exception will be raised.
        """
        bucket_name = str(bucket_name)

        if compartment is not None:
            if compartment.endswith("/"):
                bucket = compartment
            else:
                bucket = "%s/" % compartment

        full_name = _os.path.join(_os.path.split(bucket)[0], bucket_name)

        if not _os.path.exists(full_name):
            if create_if_needed:
                _os.makedirs(full_name)
            else:
                from Acquire.ObjectStore import ObjectStoreError
                raise ObjectStoreError(
                    "There is no bucket available called '%s' in "
                    "compartment '%s'" % (bucket_name, compartment))

        return full_name

    @staticmethod
    def get_bucket_name(bucket):
        """Return the name of the passed bucket"""
        return _os.path.split(bucket)[1]

    @staticmethod
    def is_bucket_empty(bucket):
        """Return whether or not the passed bucket is empty"""
        return len(_os.listdir(bucket)) == 0

    @staticmethod
    def delete_bucket(bucket, force=False):
        """Delete the passed bucket. This should be used with caution.
           Normally you can only delete a bucket if it is empty. If
           'force' is True then it will remove all objects/pars from
           the bucket first, and then delete the bucket. This
           can cause a LOSS OF DATA!
        """
        is_empty = Testing_ObjectStore.is_bucket_empty(bucket=bucket)

        if not is_empty:
            if force:
                Testing_ObjectStore.delete_all_objects(bucket=bucket)
            else:
                raise PermissionError(
                    "You cannot delete the bucket %s as it is not empty" %
                    Testing_ObjectStore.get_bucket_name(bucket=bucket))

        # the bucket is empty - delete it
        _os.rmdir(bucket)

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
        from Acquire.Crypto import PublicKey as _PublicKey

        if not isinstance(encrypt_key, _PublicKey):
            from Acquire.Client import PARError
            raise PARError(
                "You must supply a valid PublicKey to encrypt the "
                "returned PAR")

        if key is not None:
            if not _os.path.exists("%s/%s._data" % (bucket, key)):
                from Acquire.Client import PARError
                raise PARError(
                    "The object '%s' in bucket '%s' does not exist!" %
                    (key, bucket))
        elif not _os.path.exists(bucket):
            from Acquire.Client import PARError
            raise PARError("The bucket '%s' does not exist!" % bucket)

        url = "file://%s" % bucket

        if key:
            url = "%s/%s" % (url, key)

        # get the time this PAR was created
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now
        created_datetime = _get_datetime_now()

        # get the UTC datetime when this PAR should expire
        expires_datetime = created_datetime + \
            _datetime.timedelta(seconds=duration)

        # mimic limitations of OCI - cannot have a bucket PAR with
        # read permissions!
        if (key is None) and readable:
            from Acquire.Client import PARError
            raise PARError(
                "You cannot create a Bucket PAR that has read permissions "
                "due to a limitation in the underlying platform")

        from Acquire.ObjectStore import OSPar as _OSPar
        from Acquire.ObjectStore import OSParRegistry as _OSParRegistry

        url_checksum = _OSPar.checksum(url)

        driver_details = {"driver": "testing_objstore",
                          "bucket": bucket,
                          "created_datetime": created_datetime}

        par = _OSPar(url=url, key=key, encrypt_key=encrypt_key,
                     expires_datetime=expires_datetime,
                     is_readable=readable, is_writeable=writeable,
                     driver_details=driver_details)

        _OSParRegistry.register(par=par, url_checksum=url_checksum,
                                details_function=_get_driver_details_from_par,
                                cleanup_function=cleanup_function)

        return par

    @staticmethod
    def close_par(par=None, par_uid=None, url_checksum=None):
        """Close the passed PAR, which provides access to data in the
           passed bucket
        """
        from Acquire.ObjectStore import OSParRegistry as _OSParRegistry

        if par is None:
            par = _OSParRegistry.get(
                        par_uid=par_uid,
                        url_checksum=url_checksum,
                        details_function=_get_driver_details_from_data)

        from Acquire.ObjectStore import OSPar as _OSPar
        if not isinstance(par, _OSPar):
            raise TypeError("The PAR must be of type OSPar")

        if par.driver() != "testing_objstore":
            raise ValueError("Cannot delete a PAR that was not created "
                             "by the testing object store")

        # delete the PAR (no need to do this on testing)

        # close the PAR - this will trigger any close_function(s)
        _OSParRegistry.close(par=par)

    @staticmethod
    def get_object(bucket, key):
        """Return the binary data contained in the key 'key' in the
           passed bucket"""

        with _rlock:
            filepath = "%s/%s._data" % (bucket, key)
            if _os.path.exists(filepath):
                return open(filepath, "rb").read()
            else:
                from Acquire.ObjectStore import ObjectStoreError
                raise ObjectStoreError("No object at key '%s'" % key)

    @staticmethod
    def take_object(bucket, key):
        """Take (delete) the object from the object store, returning
           the object
        """
        with _rlock:
            filepath = "%s/%s._data" % (bucket, key)
            if _os.path.exists(filepath):
                data = open(filepath, "rb").read()
                _os.remove(filepath)
                return data
            else:
                from Acquire.ObjectStore import ObjectStoreError
                raise ObjectStoreError("No object at key '%s'" % key)

    @staticmethod
    def get_all_object_names(bucket, prefix=None):
        """Returns the names of all objects in the passed bucket"""

        root = bucket

        if prefix is not None:
            root = "%s/%s" % (bucket, prefix)

        root_len = len(bucket) + 1

        subdir_names = _glob.glob("%s*" % root)

        object_names = []

        while True:
            names = subdir_names
            subdir_names = []

            for name in names:
                if name.endswith("._data"):
                    # remove the  ._data at the end
                    name = name[root_len:-6]
                    while name.endswith("/"):
                        name = name[0:-1]

                    while name.startswith("/"):
                        name = name[1:]

                    if len(name) > 0:
                        object_names.append(name)
                elif _os.path.isdir(name):
                    subdir_names += _glob.glob("%s/*" % name)

            if len(subdir_names) == 0:
                break

        return object_names

    @staticmethod
    def set_object(bucket, key, data):
        """Set the value of 'key' in 'bucket' to binary 'data'"""

        filename = "%s/%s._data" % (bucket, key)

        with _rlock:
            try:
                with open(filename, 'wb') as FILE:
                    if data is not None:
                        FILE.write(data)
                    FILE.flush()
            except:
                dir = "/".join(filename.split("/")[0:-1])
                _os.makedirs(dir, exist_ok=True)
                with open(filename, 'wb') as FILE:
                    if data is not None:
                        FILE.write(data)
                    FILE.flush()

    @staticmethod
    def delete_all_objects(bucket, prefix=None):
        """Deletes all objects..."""
        if prefix:
            _shutil.rmtree("%s/%s" % (bucket, prefix), ignore_errors=True)
        else:
            _shutil.rmtree(bucket, ignore_errors=True)

    @staticmethod
    def delete_object(bucket, key):
        """Removes the object at 'key'"""
        try:
            _os.remove("%s/%s._data" % (bucket, key))
        except:
            pass

    @staticmethod
    def get_size_and_checksum(bucket, key):
        """Return the object size (in bytes) and checksum of the
           object in the passed bucket at the specified key
        """
        filepath = "%s/%s._data" % (bucket, key)

        if not _os.path.exists(filepath):
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError("No object at key '%s'" % key)

        from Acquire.Access import get_filesize_and_checksum \
            as _get_filesize_and_checksum

        return _get_filesize_and_checksum(filepath)
