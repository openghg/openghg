
import io as _io
import datetime as _datetime
import uuid as _uuid
import json as _json
import os as _os
import copy as _copy
import uuid as _uuid

__all__ = ["OCI_ObjectStore"]


def _sanitise_bucket_name(bucket_name):
    """This function sanitises the passed bucket name. It will always
        return a valid bucket name. If "None" is passed, then a new,
        unique bucket name will be generated

        Args:
            bucket_name (str): Bucket name to clean
        Returns:
            str: Cleaned bucket name

        """

    if bucket_name is None:
        return str(_uuid.uuid4())

    return "_".join(bucket_name.split())


def _clean_key(key):
    """This function cleans and returns a key so that it is suitable
       for use both as a key and a directory/file path
       e.g. it removes double-slashes

       Args:
            key (str): Key to clean
       Returns:
            str: Cleaned key

    """
    key = _os.path.normpath(key)

    if len(key) > 1024:
        from Acquire.ObjectStore import ObjectStoreError
        raise ObjectStoreError(
            "The object store does not support keys with longer than "
            "1024 characters (%s) - %s" % (len(key), key))

        # if this becomes a problem then we will implement a 'tinyurl'
        # to shorten keys and use this function to lookup long keys

    return key


def _get_object_url_for_region(region, uri):
    """Internal function used to get the full URL to the passed PAR URI
       for the specified region. This has the format;

       https://objectstorage.{region}.oraclecloud.com/{uri}

       Args:
            region (str): Region for cloud service
            uri (str): URI for cloud service
       Returns:
            str: Full URL for use with cloud service
    """
    server = "https://objectstorage.%s.oraclecloud.com" % region

    while uri.startswith("/"):
        uri = uri[1:]

    return "%s/%s" % (server, uri)


def _get_driver_details_from_par(par):
    """Internal function used to get the OCI driver details from the
       passed OSPar (pre-authenticated request)

       Args:
            par (OSPar): PAR holding details
        Args:
            dict: Dictionary holding OCI driver details
    """
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
    """Internal function used to get the OCI driver details from the
       passed data

       Args:
            data (dict): Dict holding OCI driver details
       Returns:
            dict: Dict holding OCI driver details
    """
    from Acquire.ObjectStore import string_to_datetime \
        as _string_to_datetime

    import copy as _copy
    details = _copy.copy(data)

    if "created_datetime" in details:
        details["created_datetime"] = _string_to_datetime(
                                            details["created_datetime"])

    return details


class OCI_ObjectStore:
    """This is the backend that abstracts using the Oracle Cloud
       Infrastructure object store
    """

    @staticmethod
    def create_bucket(bucket, bucket_name, compartment=None):
        """Create and return a new bucket in the object store called
           'bucket_name', optionally placing it into the compartment
           identified by 'compartment'. This will raise an
           ObjectStoreError if this bucket already exists

           Args:
            bucket (dict): Bucket to hold data
            bucket_name (str): Name of bucket to create
            compartment (str): Compartment in which to create bucket

           Returns:
                dict: New bucket
        """
        new_bucket = _copy.copy(bucket)

        new_bucket["bucket_name"] = str(bucket_name)

        if compartment is not None:
            new_bucket["compartment_id"] = str(compartment)

        try:
            from oci.object_storage.models import CreateBucketDetails as \
                _CreateBucketDetails
        except:
            raise ImportError(
                "Cannot import OCI. Please install OCI, e.g. via "
                "'pip install oci' so that you can connect to the "
                "Oracle Cloud Infrastructure")

        try:
            request = _CreateBucketDetails()
            request.compartment_id = new_bucket["compartment_id"]
            client = new_bucket["client"]
            request.name = _sanitise_bucket_name(bucket_name)

            new_bucket["bucket"] = client.create_bucket(
                                        client.get_namespace().data,
                                        request).data
        except Exception as e:
            # couldn't create the bucket - likely because it already
            # exists - try to connect to the existing bucket
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "Unable to create the bucket '%s', likely because it "
                "already exists: %s" % (bucket_name, str(e)))

        return new_bucket

    @staticmethod
    def get_bucket(bucket, bucket_name, compartment=None,
                   create_if_needed=True):
        """Find and return a new bucket in the object store called
           'bucket_name', optionally placing it into the compartment
           identified by 'compartment'. If 'create_if_needed' is True
           then the bucket will be created if it doesn't exist. Otherwise,
           if the bucket does not exist then an exception will be raised.

           Args:
                bucket (dict): Bucket to hold data
                bucket_name (str): Name of bucket to create
                compartment (str, default=None): Compartment in which to
                create bucket
                create_if_needed (bool, default=None): If True, create bucket,
                else do
                not

           Returns:
                dict: New bucket

        """
        new_bucket = _copy.copy(bucket)

        new_bucket["bucket_name"] = _sanitise_bucket_name(bucket_name)

        if compartment is not None:
            new_bucket["compartment_id"] = str(compartment)

        try:
            from oci.object_storage.models import CreateBucketDetails as \
                _CreateBucketDetails
        except:
            raise ImportError(
                "Cannot import OCI. Please install OCI, e.g. via "
                "'pip install oci' so that you can connect to the "
                "Oracle Cloud Infrastructure")

        # try to get the existing bucket
        client = new_bucket["client"]
        namespace = client.get_namespace().data
        sanitised_name = _sanitise_bucket_name(bucket_name)

        try:
            existing_bucket = client.get_bucket(
                                namespace, sanitised_name).data
        except:
            existing_bucket = None

        if existing_bucket:
            new_bucket["bucket"] = existing_bucket
            return new_bucket

        if create_if_needed:
            try:
                request = _CreateBucketDetails()
                request.compartment_id = new_bucket["compartment_id"]
                request.name = sanitised_name

                client.create_bucket(namespace, request)
            except:
                pass

            try:
                existing_bucket = client.get_bucket(
                                    namespace, sanitised_name).data
            except:
                existing_bucket = None

        if existing_bucket is None:
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "There is not bucket called '%s'. Please check the "
                "compartment and access permissions." % bucket_name)

        new_bucket["bucket"] = existing_bucket

        return new_bucket

    @staticmethod
    def get_bucket_name(bucket):
        """Return the name of the passed bucket

           Args:
                bucket (dict): Bucket holding data
           Returns:
                str: Name of bucket
        """
        return bucket["bucket_name"]

    @staticmethod
    def is_bucket_empty(bucket):
        """Return whether or not the passed bucket is empty

           Args:
                bucket (dict): Bucket holding data
           Returns:
                bool: True if bucket empty, else False

        """
        objects = bucket["client"].list_objects(bucket["namespace"],
                                                bucket["bucket_name"],
                                                limit=1).data

        for _obj in objects.objects:
            return False

        return True

    @staticmethod
    def delete_bucket(bucket, force=False):
        """Delete the passed bucket. This should be used with caution.
           Normally you can only delete a bucket if it is empty. If
           'force' is True then it will remove all objects/pars from
           the bucket first, and then delete the bucket. This
           can cause a LOSS OF DATA!

           Args:
                bucket (dict): Bucket to delete
                force (bool, default=False): If True, delete even
                if bucket is not empty. If False and bucket not empty
                raise PermissionError
           Returns:
                None
        """
        is_empty = OCI_ObjectStore.is_bucket_empty(bucket=bucket)

        if not is_empty:
            if force:
                OCI_ObjectStore.delete_all_objects(bucket=bucket)
            else:
                raise PermissionError(
                    "You cannot delete the bucket %s as it is not empty" %
                    OCI_ObjectStore.get_bucket_name(bucket=bucket))

        # the bucket is empty - delete it
        client = bucket["client"]
        namespace = client.get_namespace().data
        bucket_name = bucket["bucket_name"]

        try:
            response = client.delete_bucket(namespace, bucket_name)
        except Exception as e:
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "Unable to delete bucket '%s'. Please check the "
                "compartment and access permissions: Error %s" %
                (bucket_name, str(e)))

        if response.status not in [200, 204]:
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "Unable to delete a bucket '%s' : Status %s, Error %s" %
                (bucket_name, response.status, str(response.data)))

    @staticmethod
    def create_par(bucket, encrypt_key, key=None, readable=True,
                   writeable=False, duration=3600, cleanup_function=None):
        """Create a pre-authenticated request for the passed bucket and
           key (if key is None then the request is for the entire bucket).
           This will return a OSPar object that will contain a URL that can
           be used to access the object/bucket. If writeable is true, then
           the URL will also allow the object/bucket to be written to.
           PARs are time-limited. Set the lifetime in seconds by passing
           in 'duration' (by default this is one hour)

           Args:
                bucket (dict): Bucket to create OSPar for
                encrypt_key (PublicKey): Public key to
                encrypt PAR
                key (str, default=None): Key
                readable (bool, default=True): If bucket is readable
                writeable (bool, default=False): If bucket is writeable
                duration (int, default=3600): Duration OSPar should be
                valid for in seconds
                cleanup_function (function, default=None): Cleanup
                function to be passed to PARRegistry

           Returns:
                OSPar: Pre-authenticated request for the bucket
        """
        from Acquire.Crypto import PublicKey as _PublicKey

        if not isinstance(encrypt_key, _PublicKey):
            from Acquire.Client import PARError
            raise PARError(
                "You must supply a valid PublicKey to encrypt the "
                "returned OSPar")

        # get the UTC datetime when this OSPar should expire
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now
        expires_datetime = _get_datetime_now() + \
            _datetime.timedelta(seconds=duration)

        is_bucket = (key is None)

        # Limitation of OCI - cannot have a bucket OSPar with
        # read permissions!
        if is_bucket and readable:
            from Acquire.Client import PARError
            raise PARError(
                "You cannot create a Bucket OSPar that has read permissions "
                "due to a limitation in the underlying platform")

        try:
            from oci.object_storage.models import \
                CreatePreauthenticatedRequestDetails as \
                _CreatePreauthenticatedRequestDetails
        except:
            raise ImportError(
                "Cannot import OCI. Please install OCI, e.g. via "
                "'pip install oci' so that you can connect to the "
                "Oracle Cloud Infrastructure")

        oci_par = None

        request = _CreatePreauthenticatedRequestDetails()

        if is_bucket:
            request.access_type = "AnyObjectWrite"
        elif readable and writeable:
            request.access_type = "ObjectReadWrite"
        elif readable:
            request.access_type = "ObjectRead"
        elif writeable:
            request.access_type = "ObjectWrite"
        else:
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "Unsupported permissions model for OSPar!")

        request.name = str(_uuid.uuid4())

        if not is_bucket:
            request.object_name = _clean_key(key)

        request.time_expires = expires_datetime

        client = bucket["client"]

        try:
            response = client.create_preauthenticated_request(
                                        client.get_namespace().data,
                                        bucket["bucket_name"],
                                        request)

        except Exception as e:
            # couldn't create the preauthenticated request
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "Unable to create the OSPar '%s': %s" %
                (str(request), str(e)))

        if response.status != 200:
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "Unable to create the OSPar '%s': Status %s, Error %s" %
                (str(request), response.status, str(response.data)))

        oci_par = response.data

        if oci_par is None:
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "Unable to create the preauthenticated request!")

        created_datetime = oci_par.time_created.replace(
                                tzinfo=_datetime.timezone.utc)

        expires_datetime = oci_par.time_expires.replace(
                                tzinfo=_datetime.timezone.utc)

        # the URI returned by OCI does not include the server. We need
        # to get the server based on the region of this bucket
        url = _get_object_url_for_region(bucket["region"],
                                         oci_par.access_uri)

        # get the checksum for this URL - used to validate the close
        # request
        from Acquire.ObjectStore import OSPar as _OSPar
        from Acquire.ObjectStore import OSParRegistry as _OSParRegistry
        url_checksum = _OSPar.checksum(url)

        driver_details = {"driver": "oci",
                          "bucket": bucket["bucket_name"],
                          "created_datetime": created_datetime,
                          "par_id": oci_par.id,
                          "par_name": oci_par.name}

        par = _OSPar(url=url, encrypt_key=encrypt_key,
                     key=oci_par.object_name,
                     expires_datetime=expires_datetime,
                     is_readable=readable,
                     is_writeable=writeable,
                     driver_details=driver_details)

        _OSParRegistry.register(par=par,
                                url_checksum=url_checksum,
                                details_function=_get_driver_details_from_par,
                                cleanup_function=cleanup_function)

        return par

    @staticmethod
    def close_par(par=None, par_uid=None, url_checksum=None):
        """Close the passed OSPar, which provides access to data in the
           passed bucket

           Args:
                par (OSPar, default=None): OSPar to close bucket
                par_uid (str, default=None): UID for OSPar
                url_checksum (str, default=None): Checksum to
                pass to PARRegistry
           Returns:
                None
        """
        from Acquire.ObjectStore import OSParRegistry as _OSParRegistry

        if par is None:
            par = _OSParRegistry.get(
                            par_uid=par_uid,
                            details_function=_get_driver_details_from_data,
                            url_checksum=url_checksum)

        from Acquire.ObjectStore import OSPar as _OSPar
        if not isinstance(par, _OSPar):
            raise TypeError("The OSPar must be of type OSPar")

        if par.driver() != "oci":
            raise ValueError("Cannot delete a OSPar that was not created "
                             "by the OCI object store")

        # delete the PAR
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        par_bucket = par.driver_details()["bucket"]
        par_id = par.driver_details()["par_id"]

        bucket = _get_service_account_bucket()

        # now get the bucket accessed by the OSPar...
        bucket = OCI_ObjectStore.get_bucket(bucket=bucket,
                                            bucket_name=par_bucket)

        client = bucket["client"]

        try:
            response = client.delete_preauthenticated_request(
                                            client.get_namespace().data,
                                            bucket["bucket_name"],
                                            par_id)
        except Exception as e:
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "Unable to delete a OSPar '%s' : Error %s" %
                (par_id, str(e)))

        if response.status not in [200, 204]:
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError(
                "Unable to delete a OSPar '%s' : Status %s, Error %s" %
                (par_id, response.status, str(response.data)))

        # close the OSPar - this will trigger any close_function(s)
        _OSParRegistry.close(par=par)

    @staticmethod
    def get_object(bucket, key):
        """Return the binary data contained in the key 'key' in the
           passed bucket

           Args:
                bucket (dict): Bucket containing data
                key (str): Key for data in bucket
           Returns:
                bytes: Binary data

        """

        key = _clean_key(key)

        try:
            response = bucket["client"].get_object(bucket["namespace"],
                                                   bucket["bucket_name"],
                                                   key)
            is_chunked = False
        except:
            try:
                response = bucket["client"].get_object(bucket["namespace"],
                                                       bucket["bucket_name"],
                                                       "%s/1" % key)
                is_chunked = True
            except:
                is_chunked = False
                pass

            # Raise the original error
            if not is_chunked:
                from Acquire.ObjectStore import ObjectStoreError
                raise ObjectStoreError("No data at key '%s'" % key)

        data = None

        for chunk in response.data.raw.stream(1024 * 1024,
                                              decode_content=False):
            if not data:
                data = chunk
            else:
                data += chunk

        if is_chunked:
            # keep going through to find more chunks
            next_chunk = 1

            while True:
                next_chunk += 1

                try:
                    response = bucket["client"].get_object(
                                        bucket["namespace"],
                                        bucket["bucket_name"],
                                        "%s/%d" % (key, next_chunk))
                except:
                    response = None
                    break

                for chunk in response.data.raw.stream(1024 * 1024,
                                                      decode_content=False):
                    if not data:
                        data = chunk
                    else:
                        data += chunk

        return data

    @staticmethod
    def take_object(bucket, key):
        """Take (delete) the object from the object store, returning
           the object

           Args:
                bucket (dict): Bucket containing data
                key (str): Key for data

           Returns:
                bytes: Binary data
        """
        # ideally the get and delete should be atomic... would like this API
        data = OCI_ObjectStore.get_object(bucket, key)

        try:
            OCI_ObjectStore.delete_object(bucket, key)
        except:
            pass

        return data

    @staticmethod
    def get_all_object_names(bucket, prefix=None):
        """Returns the names of all objects in the passed bucket

           Args:
                bucket (dict): Bucket containing data
                prefix (str): Prefix for data
           Returns:
                list: List of all objects in bucket

        """
        if prefix is not None:
            prefix = _clean_key(prefix)

        objects = bucket["client"].list_objects(bucket["namespace"],
                                                bucket["bucket_name"],
                                                prefix=prefix).data

        names = []

        for obj in objects.objects:
            if prefix:
                if obj.name.startswith(prefix):
                    name = obj.name
            else:
                name = obj.name

            while name.endswith("/"):
                name = name[0:-1]

            while name.startswith("/"):
                name = name[1:]

            if len(name) > 0:
                names.append(name)

        return names

    @staticmethod
    def set_object(bucket, key, data):
        """Set the value of 'key' in 'bucket' to binary 'data'

           Args:
                bucket (dict): Bucket containing data
                key (str): Key for data in bucket
                data (bytes): Binary data to store in bucket

           Returns:
                None
        """
        if data is None:
            data = b'0'

        f = _io.BytesIO(data)

        key = _clean_key(key)
        bucket["client"].put_object(bucket["namespace"],
                                    bucket["bucket_name"],
                                    key, f)

    @staticmethod
    def delete_all_objects(bucket, prefix=None):
        """Deletes all objects...

           Args:
                bucket (dict): Bucket containing data
                prefix (str, default=None): Prefix for data,
                currently unused
            Returns:
                None
        """

        for obj in OCI_ObjectStore.get_all_object_names(bucket):
            bucket["client"].delete_object(bucket["namespace"],
                                           bucket["bucket_name"],
                                           obj)

    @staticmethod
    def delete_object(bucket, key):
        """Removes the object at 'key'

           Args:
                bucket (dict): Bucket containing data
                key (str): Key for data
           Returns:
                None
        """
        try:
            key = _clean_key(key)
            bucket["client"].delete_object(bucket["namespace"],
                                           bucket["bucket_name"],
                                           key)
        except:
            pass

    @staticmethod
    def get_size_and_checksum(bucket, key):
        """Return the object size (in bytes) and MD5 checksum of the
           object in the passed bucket at the specified key

           Args:
                bucket (dict): Bucket containing data
                key (str): Key for object
           Returns:
                tuple (int, str): Size and MD5 checksum of object

        """
        key = _clean_key(key)

        try:
            response = bucket["client"].get_object(bucket["namespace"],
                                                   bucket["bucket_name"],
                                                   key)
        except:
            from Acquire.ObjectStore import ObjectStoreError
            raise ObjectStoreError("No data at key '%s'" % key)

        content_length = response.headers["Content-Length"]
        checksum = response.headers["Content-MD5"]

        # the checksum is a base64 encoded Content-MD5 header
        # described as standard part of HTTP RFC 2616. Need to
        # convert this back to a hexdigest
        import binascii as _binascii
        import base64 as _base64
        md5sum = _binascii.hexlify(_base64.b64decode(checksum)).decode("utf-8")

        return (int(content_length), md5sum)
