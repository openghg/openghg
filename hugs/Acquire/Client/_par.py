
import datetime as _datetime
import json as _json
import os as _os

__all__ = ["PAR", "BucketReader", "BucketWriter", "ObjectReader",
           "ObjectWriter", "ComputeRunner"]


class PAR:
    """This class holds the result of a pre-authenticated request
       (a PAR - also called a pre-signed request). This holds a
       pre-authenticated URL to access either;

       (1) A individual object in an object store (read or write)
       (2) An entire bucket in the object store (write only)
       (3) A calculation to be performed on the compute service (start or stop)

       The PAR is created encrypted, so can only be used by the
       person or service that has access to the decryption key

       Args:
            url (str, default=None): URL for access
            key (str, default=None): Key for request
            encrypt_key (str, default=None): Key used to encrypt
            the PAR
            expires_datatime (datetime, default=None): Datetime at 
            which the PAR expires
            is_readable (bool, default=False): If True read access granted
            is_writeable (bool, default=False): If True write access granted
            is_executable (bool, default=False): If True PAR triggers a calculation
            driver_details (str, default=None): Contains extra details for PAR
            creation

    """
    def __init__(self, url=None, key=None,
                 encrypt_key=None,
                 expires_datetime=None,
                 is_readable=False,
                 is_writeable=False,
                 is_executable=False,
                 driver_details=None):
        """Construct a PAR result by passing in the URL at which the
           object can be accessed, the UTC datetime when this expires,
           whether this is readable, writeable or executable, and
           the encryption key to use to encrypt the PAR.

           If this is an object store PAR, then optionally you can
           pass in the key for the object in the object store that
           this provides access to. If this is not supplied, then an
           entire bucket is accessed). If 'is_readable', then read-access
           has been granted, while if 'is_writeable' then write
           access has been granted.

           If 'is_executable' then this is a calculation PAR that triggers
           a calculation.

           Otherwise no access is possible.

           driver_details is provided by the machinery that creates
           the PAR, and supplies extra details that are used by the
           driver to create, register and manage PARs... You should
           not do anything with driver_details yourself
        """
        service_url = None

        if url is None:
            is_readable = True
            self._uid = None
        else:
            from Acquire.Crypto import PublicKey as _PublicKey
            from Acquire.Crypto import PrivateKey as _PrivateKey

            if isinstance(encrypt_key, _PrivateKey):
                encrypt_key = encrypt_key.public_key()

            if not isinstance(encrypt_key, _PublicKey):
                raise TypeError(
                    "You must supply a valid PublicKey to encrypt a PAR")

            url = encrypt_key.encrypt(url)

            from Acquire.ObjectStore import create_uuid as _create_uuid
            self._uid = _create_uuid()

            try:
                from Acquire.Service import get_this_service \
                    as _get_this_service
                service_url = _get_this_service().canonical_url()
            except:
                pass

        self._url = url
        self._key = key
        self._expires_datetime = expires_datetime
        self._service_url = service_url

        if driver_details is not None:
            if not isinstance(driver_details, dict):
                raise TypeError("The driver details must be a dictionary")

        self._driver_details = driver_details

        if is_readable:
            self._is_readable = True
        else:
            self._is_readable = False

        if is_writeable:
            self._is_writeable = True
        else:
            self._is_writeable = False

        if is_executable:
            self._is_executable = True
        else:
            self._is_executable = False

        if self._is_executable:
            self._is_readable = False
            self._is_writeable = False
        elif not (self._is_readable or self._is_writeable):
            from Acquire.Client import PARPermissionsError
            raise PARPermissionsError(
                "You cannot create a PAR that has no read or write "
                "or execute permissions!")
        else:
            self._is_executable = False

    def __str__(self):
        if self.seconds_remaining() < 1:
            return "PAR( expired )"
        elif self._is_executable:
            return "PAR( calculation, seconds_remaining=%s )" % \
                (self.seconds_remaining(buffer=0))
        if self._key is None:
            return "PAR( bucket=True, seconds_remaining=%s )" % \
                (self.seconds_remaining(buffer=0))
        else:
            return "PAR( key=%s, seconds_remaining=%s )" % \
                (self.key(), self.seconds_remaining(buffer=0))

    def _set_private_key(self, privkey):
        """Call this function to set the private key for this
           PAR. This is the private key that is used to
           decrypt the PAR, and is provided here if you want
           to use the PAR without having to always supply
           the key (by definition, you are the only person
           who has the key)

           Args:
                privkey (str): Private key to use for this PAR
           Returns:
                None
        """
        from Acquire.Crypto import PrivateKey as _PrivateKey

        if not isinstance(privkey, _PrivateKey):
            raise TypeError("The private key must be type PrivateKey")

        self._privkey = privkey

    def _get_privkey(self, decrypt_key=None):
        """Return the private key used to decrypt the PAR, passing in
           the user-supplied key if needed

           Args:
                decrypt_key (str): Key to decrypt the PAR
           Returns:
                str: Key for decryption of PAR

        """
        try:
            if self._privkey is not None:
                return self._privkey
        except:
            pass

        if decrypt_key is None:
            raise PermissionError(
                "You must supply a private key to decrypt this PAR")

        from Acquire.Crypto import PrivateKey as _PrivateKey
        if not isinstance(decrypt_key, _PrivateKey):
            raise TypeError("The supplied private key must be type PrivateKey")

        return decrypt_key

    def is_null(self):
        """Return whether or not this is null
        
           Returns:
                bool: True if PAR is null, else False
        """
        return self._uid is None

    @staticmethod
    def checksum(data):
        """Return the checksum of the passed data. This is used either
           for validating data, and is also used to create a checksum
           of the URL so that the user can demonstrate that they can
           decrypt this PAR

           Args:
                data (str): Data to checksum
           Returns:
                str: MD5 checksum of data
        """
        from hashlib import md5 as _md5
        md5 = _md5()

        if isinstance(data, str):
            data = data.encode("utf-8")

        md5.update(data)
        return md5.hexdigest()

    def url(self, decrypt_key=None):
        """Return the URL at which the bucket/object can be accessed. This
           will raise a PARTimeoutError if the url has less than 30 seconds
           of validity left. Note that you must pass in the key used to
           decrypt the PAR
           
           Args:
                decrypt_key (str, default=None): Key for decryption of data
           Returns:
                TODO - ensure this is correct
                str: Decrypted data

           """
        if self.seconds_remaining(buffer=30) <= 0:
            from Acquire.Client import PARTimeoutError
            raise PARTimeoutError(
                "The URL behind this PAR has expired and is no longer valid")

        return self._get_privkey(decrypt_key).decrypt(self._url)

    def service_url(self):
        """Return the URL of the service that created this PAR
        
           Returns:
                str: URL of service that created this PAR
        """
        if self.is_null():
            return None
        else:
            return self._service_url

    def service(self):
        """Return the service that created this PAR
        
           Returns:
                Service: Service object that created this PAR
        """
        service_url = self.service_url()

        if service_url is not None:
            from Acquire.Service import get_trusted_service \
                as _get_trusted_service

            print(service_url)
            return _get_trusted_service(service_url=service_url)
        else:
            return None

    def uid(self):
        """Return the UID of this PAR
        
           Returns:
                str: UID of this PAR
        """
        return self._uid

    def driver_details(self):
        """Return the driver details for this PAR. This is used only
           on the service that created the PAR, and returns an empty
           dictionary if the details are not available

           Returns:
                str: Driver details for this PAR
        """
        if self._driver_details is None:
            return {}
        else:
            return self._driver_details

    def driver(self):
        """Return the driver behind this PAR - this is only available on
           the service

           Returns:
                str: Driver behind this PAR
        """
        try:
            return self.driver_details()["driver"]
        except:
            return None

    def fingerprint(self):
        """Return a fingerprint for this PAR that can be used
           in authorisations

           TODO - Should this be a more detailed fingerprint?

           Returns:
                str: UID for this PAR
        """
        return self._uid

    def is_readable(self):
        """Return whether or not this PAR gives read access

           Returns:
                bool: True if PAR gives read access
        """
        return self._is_readable

    def is_writeable(self):
        """Return whether or not this PAR gives write access
        
           Returns:    
                bool: True if PAR gives write access
        
        """
        return self._is_writeable

    def is_executable(self):
        """Return whether or not this is an executable job
        
           Returns:
                bool: True if PAR is for an executable job
        """
        return self._is_executable

    def key(self):
        """Return the key for the object this accesses - this is None
           if the PAR grants access to the entire bucket
           
           Returns:
                str: Key for the object this accesses
           
           """
        return self._key

    def is_bucket(self):
        """Return whether or not this PAR is for an entire bucket
        
           Returns:
                bool: True if this PAR is for a bucket, else False    
    
        """
        return (self._key is None) and not (self.is_executable())

    def is_calculation(self):
        """Return whether or not this PAR is for a calculation
        
           Returns:
                bool: True if PAR is for calculation, else False
        """
        return self._is_executable

    def is_object(self):
        """Return whether or not this PAR is for a single object
        
           Returns:
                bool: True if PAR is for a single object, else False
        """
        return self._key is not None

    def expires_when(self):
        """Return when this PAR expires (or expired)
        
           Returns:
                datetime: Time at which this PAR expires
        
        """
        if self.is_null():
            return None
        else:
            return self._expires_datetime

    def seconds_remaining(self, buffer=30):
        """Return the number of seconds remaining before this PAR expires.
           This will return 0 if the PAR has already expired. To be safe,
           you should renew PARs if the number of seconds remaining is less
           than 60. This will subtract 'buffer' seconds from the actual
           validity to provide a buffer against race conditions (function
           says this is valid when it is not)

           Args:
                buffer (int, default=30): buffer PAR validity (seconds)
           Returns:
                datetime: Seconds remaining on PAR validity
        """
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        buffer = float(buffer)

        if buffer < 0:
            buffer = 0

        now = _get_datetime_now()

        delta = (self._expires_datetime - now).total_seconds() - buffer

        if delta < 0:
            return 0
        else:
            return delta

    def read(self, decrypt_key=None):
        """Return an object that can be used to read data from this PAR

           Args:
                decrypt_key (str, default=None): Key to decrypt this PAR
           Returns:
                BucketReader or ObjectReader: Object that can be used to 
                read this PAR
        """
        if not self.is_readable():
            from Acquire.Client import PARPermissionsError
            raise PARPermissionsError(
                "You do not have permission to read from this PAR: %s" % self)

        if self.is_bucket():
            return BucketReader(self, self._get_privkey(decrypt_key))
        else:
            return ObjectReader(self, self._get_privkey(decrypt_key))

    def write(self, decrypt_key=None):
        """Return an object that can be used to write data to this PAR
        
           Args:
                decrypt_key (str, default=None): Key to decrypt this PAR
           Returns:
                BucketReader or ObjectReader: Object that can be used to 
                write to this PAR
           
        """
        if not self.is_writeable():
            from Acquire.Client import PARPermissionsError
            raise PARPermissionsError(
                "You do not have permission to write to this PAR: %s" % self)

        if self.is_bucket():
            return BucketWriter(self, self._get_privkey(decrypt_key))
        else:
            return ObjectWriter(self, self._get_privkey(decrypt_key))

    def execute(self, decrypt_key=None):
        """Return an object that can be used to control execution of
           this PAR

           Args:
                decrypt_key (str, default=None): Key to decrypt this PAR
           Returns:
                ComputeRunner: Object that can be used to control execution
                of this PAR
        """
        if not self.is_executable():
            from Acquire.Client import PARPermissionsError
            raise PARPermissionsError(
                "You do not have permission to execute this PAR: %s" % self)

        return ComputeRunner(self, self._get_privkey(decrypt_key))

    def close(self, decrypt_key=None):
        """Close this PAR - this closes and deletes the PAR. You must
           pass in the decryption key so that you can validate that
           you have permission to read (and thus close) this PAR

           Args:
                decrypt_key (str, default=None): Key to decrypt this PAR
           Returns:
                None
        """
        if self.is_null():
            return

        service = self.service()

        if service is not None:
            # we confirm we have permission to close this PAR by sending
            # a checksum of the url (which only we would know)
            url = self.url(decrypt_key=decrypt_key)

            args = {"par_uid": self._uid,
                    "url_checksum": PAR.checksum(url)}

            service.call_function(function="close_par", args=args)

        # now that the PAR is closed, set it into a null state
        import copy as _copy
        par = PAR()
        self.__dict__ = _copy.copy(par.__dict__)

    def to_data(self, passphrase=None):
        """Return a json-serialisable dictionary that contains all data
           for this object

           Args:
                passphrase (str, default=None): Passphrase to use to 
                encrypt PAR
           Returns:
                dict: JSON serialisable dictionary
        """
        data = {}

        if self._url is None:
            return data

        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string
        from Acquire.ObjectStore import bytes_to_string \
            as _bytes_to_string

        data["url"] = _bytes_to_string(self._url)
        data["uid"] = self._uid
        data["key"] = self._key
        data["expires_datetime"] = _datetime_to_string(self._expires_datetime)
        data["is_readable"] = self._is_readable
        data["is_writeable"] = self._is_writeable
        data["is_executable"] = self._is_executable

        try:
            if self._service_url is not None:
                data["service_url"] = self._service_url
        except:
            pass

        try:
            privkey = self._privkey
        except:
            privkey = None

        if privkey is not None:
            if passphrase is not None:
                data["privkey"] = privkey.to_data(passphrase)

        # note that we don't save the driver details as these
        # are stored separately on the service

        return data

    @staticmethod
    def from_data(data, passphrase=None):
        """Return a PAR constructed from the passed json-deserliased
           dictionary

           Args:
                data (dict): JSON-deserialised dictionary from which to
                create PAR
            Returns:
                PAR: PAR object created from dict
        """
        if data is None or len(data) == 0:
            return PAR()

        from Acquire.ObjectStore import string_to_datetime \
            as _string_to_datetime
        from Acquire.ObjectStore import string_to_bytes \
            as _string_to_bytes

        par = PAR()

        par._url = _string_to_bytes(data["url"])
        par._key = data["key"]
        par._uid = data["uid"]

        if par._key is not None:
            par._key = str(par._key)

        par._expires_datetime = _string_to_datetime(data["expires_datetime"])
        par._is_readable = data["is_readable"]
        par._is_writeable = data["is_writeable"]
        par._is_executable = data["is_executable"]

        if "service_url" in data:
            par._service_url = data["service_url"]

        if "privkey" in data:
            if passphrase is not None:
                from Acquire.Crypto import PrivateKey as _PrivateKey
                par._privkey = _PrivateKey.from_data(data["privkey"],
                                                     passphrase)

        # note that we don't load the driver details as this
        # is stored and loaded separately on the service

        return par


def _url_to_filepath(url):
    """Internal function used to strip the "file://" from the beginning
       of a file url

       Args:
            url (str): URL to clean
       Returns:
            str: URL string with file:// removed
    """
    return url[7:]


def _read_local(url):
    """Internal function used to read data from the local testing object
       store

       Args:
            url (str): URL from which to read data
       Returns:
            bytes: Data read from file
    """
    with open("%s._data" % _url_to_filepath(url), "rb") as FILE:
        return FILE.read()


def _read_remote(url):
    """Internal function used to read data from a remote URL
    
       Args:
            url (str): Remote URL from which to read data
       Returns:
            str: HTTP request content

    """
    status_code = None
    response = None

    try:
        from Acquire.Stubs import requests as _requests
        response = _requests.get(url)
        status_code = response.status_code
    except Exception as e:
        from Acquire.Client import PARReadError
        raise PARReadError(
            "Cannot read the remote PAR URL '%s' because of a possible "
            "nework issue: %s" % (url, str(e)))

    output = response.content

    if status_code != 200:
        from Acquire.Client import PARReadError
        raise PARReadError(
            "Failed to read data from the PAR URL. HTTP status code = %s, "
            "returned output: %s" % (status_code, output))

    return output


def _list_local(url):
    """Internal function to list all of the objects keys below 'url'
    
       Args:
            url (str): URL from which to read data
       Returns:
            list: List of object keys
    """
    local_dir = _url_to_filepath(url)

    keys = []

    for dirpath, _, filenames in _os.walk(local_dir):
        local_path = dirpath[len(local_dir):]
        has_local_path = (len(local_path) > 0)

        for filename in filenames:
            if filename.endswith("._data"):
                filename = filename[0:-6]

                if has_local_path:
                    keys.append("%s/%s" % (local_path, filename))
                else:
                    keys.append(filename)

    return keys


def _list_remote(url):
    """Internal function to list all of the objects keys below 'url'

       Currently unimplemented
    
       Args:
            url (str): URL from which to read data
       Returns:
            list: Empty list
    """
    return []


def _write_local(url, data):
    """Internal function used to write data to a local file
    
       Args:
            url (str): URL to write data to
            data (bytes): Data to write
       Returns:
            None
    """
    filename = "%s._data" % _url_to_filepath(url)

    try:
        with open(filename, 'wb') as FILE:
            FILE.write(data)
            FILE.flush()
    except:
        dir = "/".join(filename.split("/")[0:-1])
        _os.makedirs(dir, exist_ok=True)
        with open(filename, 'wb') as FILE:
            FILE.write(data)
            FILE.flush()


def _write_remote(url, data):
    """Internal function used to write data to the passed remote URL
    
       Args:
            url (str): Remote URL to write data to
            data (bytes): Data to write
       Returns:
            None    
    """
    try:
        from Acquire.Stubs import requests as _requests
        response = _requests.put(url, data=data)
        status_code = response.status_code
    except Exception as e:
        from Acquire.Client import PARWriteError
        raise PARWriteError(
            "Cannot write data to the remote PAR URL '%s' because of a "
            "possible nework issue: %s" % (url, str(e)))

    if status_code != 200:
        from Acquire.Client import PARWriteError
        raise PARWriteError(
            "Cannot write data to the remote PAR URL '%s' because of a "
            "possible nework issue: %s" % (url, str(response.content)))


def _join_bucket_and_prefix(url, prefix):
    """Join together the passed url and prefix, returning the
       url directory and the remaining part which is the start
       of the file name

       Args:
            url (str): URL to combine
            prefix (str): Prefix to combine
       Returns:
            str: URL and prefix processed concatenated
    """
    if prefix is None:
        return url

    parts = prefix.split("/")

    return ("%s/%s" % (url, "/".join(parts[0:-2])), parts[-1])


class BucketReader:
    """This class provides functions to enable reading data from a
       bucket via a PAR

       Args:
            par (PAR, default=None): PAR to use for data access
            decrypt_key (str, default=None): Key to decrypt data in bucket

    """
    def __init__(self, par=None, decrypt_key=None):
        if par:
            if not isinstance(par, PAR):
                raise TypeError(
                    "You can only create a BucketReader from a PAR")
            elif not par.is_bucket():
                raise ValueError(
                    "You can only create a BucketReader from a PAR that "
                    "represents an entire bucket: %s" % par)
            elif not par.is_readable():
                from Acquire.Client import PARPermissionsError
                raise PARPermissionsError(
                    "You cannot create a BucketReader from a PAR without "
                    "read permissions: %s" % par)

            self._par = par
            self._url = par.url(decrypt_key)
        else:
            self._par = None

    def get_object(self, key):
        """Return the binary data contained in the key 'key' in the
           passed bucket
           
           Args:
                key (str): Key to access data in bucket
           Returns:
                bytes: Data referred to by key
        """
        if self._par is None:
            from Acquire.Client import PARError
            raise PARError("You cannot read data from an empty PAR")

        while key.startswith("/"):
            key = key[1:]

        url = self._url

        if url.endswith("/"):
            url = "%s%s" % (url, key)
        else:
            url = "%s/%s" % (url, key)

        if url.startswith("file://"):
            return _read_local(url)
        else:
            return _read_remote(url)

    def get_object_as_file(self, key, filename):
        """Get the object contained in the key 'key' in the passed 'bucket'
           and writing this to the file called 'filename'

           Args:
                key (str): Key to access data in bucket
           Returns:
                None          
        """
        objdata = self.get_object(key)

        with open(filename, "wb") as FILE:
            FILE.write(objdata)

    def get_string_object(self, key):
        """Return the string in 'bucket' associated with 'key'
        
           Args:
                key (str): Key to access data in bucket
           Returns:
                str: String referred to by key
        """
        data = self.get_object(key)

        try:
            return data.decode("utf-8")
        except Exception as e:
            raise TypeError(
                "The object behind this PAR cannot be converted to a string. "
                "Error is: %s" % str(e))

    def get_object_from_json(self, key):
        """Return an object constructed from json stored at 'key' in
           the passed bucket. This raises an exception if ther
           
           Args:
                key (str): Key to access object
            Returns:
                Object: Objet created from JSON
        """
        data = self.get_string_object(key)
        return _json.loads(data)

    def get_all_object_names(self, prefix=None):
        """Returns the names of all objects in the bucket at
           the URL for the PAR for this BucketWriter
        
           Args:
                prefix (str, default=None): Prefix to filename
           Returns:
                list: List of files in bucket at that URL
        
        """
        (url, part) = _join_bucket_and_prefix(self._url, prefix)

        if url.startswith("file://"):
            objnames = _list_local(url)
        else:
            objnames = _list_remote(url)

        # scan the object names returned and discard the ones that don't
        # match the prefix
        matches = []

        if len(part) > 0:
            for objname in objnames:
                if objname.startswith(part):
                    objname = objname[len(part):]

                    while objname.startswith("/"):
                        objname = objname[1:]

                    matches.append(objname)
        else:
            matches = objnames

        return matches

    def get_all_objects(self, prefix=None):
        """Return all of the objects in the bucket at the URL
           for this BucketWriter object

            Args:
                prefix (str): Prefix for filen
            Returns:
                dict: Dictionary of objects
        
        """
        names = self.get_all_object_names(prefix)

        objects = {}

        if prefix:
            for name in names:
                objects[name] = self.get_object(
                                    "%s/%s" % (prefix, name))
        else:
            for name in names:
                objects[name] = self.get_object(name)

        return objects

    def get_all_strings(self, prefix=None):
        """Return all of the strings in the passed bucket
        
            Args:
                prefix (str): Prefix for file
            Returns:
                dict: Dictionary of objects

        """
        objects = self.get_all_objects(prefix)

        names = list(objects.keys())

        for name in names:
            try:
                s = objects[name].decode("utf-8")
                objects[name] = s
            except:
                del objects[name]

        return objects


class BucketWriter:
    """This class provides functions to enable writing data to a
       bucket via a PAR

       Args:
            par (PAR, default=None): PAR for writing with this BucketWriter object
            decrypt_key (str, default=None): key to access URL at which the 
            bucket/object can be accessed
    """
    def __init__(self, par=None, decrypt_key=None):
        if par:
            if not isinstance(par, PAR):
                raise TypeError(
                    "You can only create a BucketReader from a PAR")
            elif not par.is_bucket():
                raise ValueError(
                    "You can only create a BucketReader from a PAR that "
                    "represents an entire bucket: %s" % par)
            elif not par.is_writeable():
                from Acquire.Client import PARPermissionsError
                raise PARPermissionsError(
                    "You cannot create a BucketWriter from a PAR without "
                    "write permissions: %s" % par)

            self._par = par
            self._url = par.url(decrypt_key)
        else:
            self._par = None

    def set_object(self, key, data):
        """Set the value of 'key' in 'bucket' to binary 'data'
        
            Args:
                key (str): Key to access object
                data (bytes): Data to write
            Returns:
                None

        """
        if self._par is None:
            from Acquire.Client import PARError
            raise PARError("You cannot write data to an empty PAR")

        while key.startswith("/"):
            key = key[1:]

        url = self._url

        if url.endswith("/"):
            url = "%s%s" % (url, key)
        else:
            url = "%s/%s" % (url, key)

        # TODO - Why is this function being returned instead
        # of being called here? It doesn't return anything itself
        if url.startswith("file://"):
            return _write_local(url, data)
        else:
            return _write_remote(url, data)

    def set_object_from_file(self, key, filename):
        """Set the value of 'key' in 'bucket' to equal the contents
           of the file located by 'filename'
           
            Args:
                key (str): Key for file
                filename (str): File to write
            Returns:
                None
        """
        with open(filename, "rb") as FILE:
            data = FILE.read()
            self.set_object(key, data)

    def set_string_object(self, key, string_data):
        """Set the value of 'key' in 'bucket' to the string 'string_data'
        
            Args:
                key (str): Key for object
                string_data (str): String data to write
            Returns:
                None
        """
        self.set_object(key, string_data.encode("utf-8"))

    def set_object_from_json(self, key, data):
        """Set the value of 'key' in 'bucket' to equal to contents
           of 'data', which has been encoded to json
           
            Args:
                key (str): Key for data
                data (str): JSON data

            Returns:
                None
           """
        self.set_string_object(key, _json.dumps(data))


class ObjectReader:
    """This class provides functions for reading an object via a PAR

        Args:
            par (PAR, default=None): PAR to use to reading the object
            decrypt_key (str, default=None): Key for accessing object  

    """
    def __init__(self, par=None, decrypt_key=None):
        if par:
            if not isinstance(par, PAR):
                raise TypeError(
                    "You can only create an ObjectReader from a PAR")
            elif par.is_bucket():
                raise ValueError(
                    "You can only create an ObjectReader from a PAR that "
                    "represents an object: %s" % par)
            elif not par.is_readable():
                from Acquire.Client import PARPermissionsError
                raise PARPermissionsError(
                    "You cannot create an ObjectReader from a PAR without "
                    "read permissions: %s" % par)

            self._par = par
            self._url = par.url(decrypt_key)
        else:
            self._par = None

    def get_object(self):
        """Return the binary data contained in this object
        
            Returns:
                bytes: Object as binary data
        """
        if self._par is None:
            from Acquire.Client import PARError
            raise PARError("You cannot read data from an empty PAR")

        url = self._url

        if url.startswith("file://"):
            return _read_local(url)
        else:
            return _read_remote(url)

    def get_object_as_file(self, filename):
        """Get the object contained in this PAR and write this to
           the file called 'filename'
           
           Args:
                filename (str): Filename to write object to
            Returns:
                None

           """
        objdata = self.get_object()

        with open(filename, "wb") as FILE:
            FILE.write(objdata)

    def get_string_object(self):
        """Return the object behind this PAR as a string (raises exception
           if it is not a string)'

            Returns:
                bytes: Object associated with this PAR as bytes   
        """
        data = self.get_object()

        try:
            return data.decode("utf-8")
        except Exception as e:
            raise TypeError(
                "The object behind this PAR cannot be converted to a string. "
                "Error is: %s" % str(e))

    def get_object_from_json(self):
        """Return an object constructed from json stored at behind
           this PAR. This raises an exception if there is no data
           or the PAR has expired

           Returns:
                Object: Object created from JSON data

        """
        data = self.get_string_object()
        return _json.loads(data)


class ObjectWriter(ObjectReader):
    """This is an extension of ObjectReader that also allows writing to
       the object via the PAR

       Args:
            par (PAR, default=None): PAR for writing  the object
            decrypt_key (str, default=None): Key for accessing object
    """
    def __init__(self, par=None, decrypt_key=None):
        if par:
            if not isinstance(par, PAR):
                raise TypeError(
                    "You can only create an ObjectReader from a PAR")
            elif par.is_bucket():
                raise ValueError(
                    "You can only create an ObjectReader from a PAR that "
                    "represents an object: %s" % par)
            elif not par.is_writeable():
                from Acquire.Client import PARPermissionsError
                raise PARPermissionsError(
                    "You cannot create an ObjectWriter from a PAR without "
                    "write permissions: %s" % par)

            self._par = par
            self._url = par.url(decrypt_key)
        else:
            self._par = None

    def set_object(self, data):
        """Set the value of the object behind this PAR to the binary 'data'
        
        """
        if self._par is None:
            from Acquire.Client import PARError
            raise PARError("You cannot write data to an empty PAR")

        url = self._url

        if url.startswith("file://"):
            return _write_local(url, data)
        else:
            return _write_remote(url, data)

    def set_object_from_file(self, filename):
        """Set the value of the object behind this PAR to equal the contents
           of the file located by 'filename'"""
        with open(filename, "rb") as FILE:
            data = FILE.read()
            self.set_object(data)

    def set_string_object(self, string_data):
        """Set the value of the object behind this PAR to the
           string 'string_data'
        """
        self.set_object(string_data.encode("utf-8"))

    def set_object_from_json(self, data):
        """Set the value of the object behind this PAR to equal to contents
           of 'data', which has been encoded to json"""
        self.set_string_object(_json.dumps(data))


class ComputeRunner:
    """This class provides functions for executing a calculation
       pre-authorised by the passed PAR
    """
    def __init__(self, par=None, decrypt_key=None):
        if par:
            if not isinstance(par, PAR):
                raise TypeError(
                    "You can only create a ComputeRunner from a PAR")
            elif not par.is_executable():
                raise ValueError(
                    "You can only create a ComputeRunner from a PAR that "
                    "represents an executable calculation: %s" % par)

            self._par = par
            self._url = par.url(decrypt_key)
        else:
            self._par = None
