

class PAR:
    """This class holds a pre-authenticated request to access
       a file, set of files or a drive. Anyone who holds a copy of this
       PAR can resolve it to recover a valid File or Drive object
       that will support access via the permissions of the PAR
    """
    def __init__(self, location=None, user=None, aclrule=None,
                 expires_datetime=None):
        """Construct a PAR for the specified location,
           authorised by the passed user, giving permissions
           according to the passed 'aclrule' (default is
           ACLRule.reader()).

           The passed 'expires_datetime' is the time at which
           this PAR will expire (by default within 24 hours)
        """
        self._location = None
        self._uid = None
        self._expires_datetime = None

        if location is None:
            return

        from Acquire.Client import Location as _Location
        if not isinstance(location, _Location):
            raise TypeError("The location must be type Location")

        if location.is_null():
            return

        from Acquire.Client import User as _User
        if not isinstance(user, _User):
            raise TypeError("The user must be type User")

        if not user.is_logged_in():
            raise PermissionError("The passed User must be logged in!")

        from Acquire.Client import ACLRule as _ACLRule

        if aclrule is None:
            aclrule = _ACLRule.reader()
        elif not isinstance(aclrule, _ACLRule):
            raise TypeError("The aclrule must be type ACLRule")

        if expires_datetime is None:
            from Acquire.ObjectStore import get_datetime_future \
                as _get_datetime_future
            expires_datetime = _get_datetime_future(days=1)
        else:
            from Acquire.ObjectStore import datetime_to_datetime \
                as _datetime_to_datetime
            expires_datetime = _datetime_to_datetime(expires_datetime)

        self._location = location
        self._expires_datetime = expires_datetime
        self._aclrule = aclrule

        from Acquire.Client import Authorisation as _Authorisation
        auth = _Authorisation(user=user,
                              resource="create_par %s" % self.fingerprint())

        from Acquire.Crypto import PrivateKey as _PrivateKey
        self._secret = _PrivateKey.random_passphrase()

        args = {"authorisation": auth.to_data(),
                "par": self.to_data(),
                "secret": self._secret}

        service = location.service()

        result = service.call_function(function="create_par",
                                       args=args)

        self._set_uid(result["par_uid"])

    def __str__(self):
        if self.is_null():
            return "PAR::null"
        elif not self.is_authorised():
            return "PAR::unauthorised"
        else:
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            return "PAR( %s, %s, expires %s )" % (
                    self._location.to_string(),
                    self._aclrule, _datetime_to_string(self._expires_datetime))

    def is_null(self):
        """Return whether or not this is null"""
        return self._location is None

    def is_authorised(self):
        """Return whether or not this has been authorised"""
        return self._uid is not None

    def assert_valid(self):
        """Assert that this is a valid and authorised PAR that
           has not yet expired
        """
        is_valid = True

        if self.is_null():
            is_valid = False
        elif not self.is_authorised():
            is_valid = False
        elif self.expired():
            is_valid = False

        if not is_valid:
            raise PermissionError("The PAR is not valid")

    def _set_uid(self, uid):
        """Internal function to set the UID of this PAR"""
        if self._uid is not None:
            raise PermissionError("You cannot set the UID twice!")

        self._uid = uid

    def uid(self):
        """Return the UID for this PAR"""
        if self.is_authorised():
            return self._uid
        else:
            return None

    def aclrule(self):
        """Return the ACL rule associated with this PAR"""
        if self.is_authorised():
            return self._aclrule
        else:
            from Acquire.Client import ACLRule as _ACLRule
            return _ACLRule.denied()

    def location(self):
        """Return the location for the Drive/File that is resolvable
           from this PAR
        """
        if self.is_authorised():
            return self._location
        else:
            return None

    def fingerprint(self):
        """Return a fingerprint that can be used to show that
           the user authorised the request to create this PAR
        """
        if self.is_null():
            return None
        else:
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            return "%s:%s:%s" % (self._location.fingerprint(),
                                 self._aclrule.fingerprint(),
                                 _datetime_to_string(self._expires_datetime))

    def service(self):
        """Return the service that authorised this PAR"""
        if self.is_null():
            return None
        else:
            return self._location.service()

    def service_url(self):
        """Return the URL of the service that authorised this PAR"""
        if self.is_null():
            return None
        else:
            return self._location.service_url()

    def service_uid(self):
        """Return the UID of the service that authorised this PAR"""
        if self.is_null():
            return None
        else:
            return self._location.service_uid()

    def resolve(self, secret=None):
        """Resolve this PAR into the authorised Drive or File object, ready
           for download, upload etc.

           If 'secret' is supplied, then this will use the supplied
           secret to unlock the PAR (sometimes they may be locked
           by a simple secret)
        """
        if not self.is_authorised():
            raise PermissionError(
                "You cannot resolve a PAR that has not been authorised!")

        service = self.service()

        if secret is None:
            try:
                secret = self._secret
            except:
                pass

        if secret is not None and len(secret) > 0:
            from Acquire.Crypto import Hash
            secret = Hash.multi_md5(self._uid, secret)
        else:
            secret = None

        result = service.call_function(function="resolve_par",
                                       args={"par_uid": self.uid(),
                                             "secret": secret})

        from Acquire.Client import StorageCreds as _StorageCreds
        creds = _StorageCreds(par=self, secret=secret)

        if result["type"] == "DriveMeta":
            from Acquire.Client import DriveMeta as _DriveMeta
            from Acquire.Client import Drive as _Drive
            return _Drive.open(_DriveMeta.from_data(result["data"]),
                               creds=creds)
        elif result["type"] == "FileMeta":
            from Acquire.Client import FileMeta as _FileMeta
            from Acquire.Client import File as _File
            return _File.open(_FileMeta.from_data(result["data"]),
                              creds=creds)
        elif result["type"] == "FileMetas":
            from Acquire.Client import FileMeta as _FileMeta
            from Acquire.Client import File as _File
            from Acquire.ObjectStore import string_to_list \
                as _string_to_list
            filemetas = _string_to_list(result["data"], _FileMeta)

            files = []
            for filemeta in filemetas:
                files.append(_File.open(filemeta, creds=creds))

            if len(files) == 1:
                return files[0]
            else:
                return files
        else:
            raise PermissionError("Returned wrong type? %s" % result["type"])

    def expires_when(self):
        """Return when this PAR expires (or expired)"""
        if not self.is_authorised():
            return None
        else:
            return self._expires_datetime

    def expired(self, buffer=30):
        """Return whether or not this PAR has expired"""
        return self.seconds_remaining(buffer=buffer) <= 0

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
        if not self.is_authorised():
            return 0

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

    def to_data(self):
        """Return a json-serialisable dictionary of this PAR"""
        if self.is_null():
            return None

        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string

        data = {}

        data["location"] = self._location.to_data()
        data["aclrule"] = self._aclrule.to_data()
        data["expires_datetime"] = _datetime_to_string(self._expires_datetime)
        data["uid"] = self._uid

        return data

    @staticmethod
    def from_data(data):
        """Return a PAR constructed from the json-deserialised passed
           dictionary
        """
        if data is None or len(data) == 0:
            return PAR()

        f = PAR()

        from Acquire.Client import Location as _Location
        from Acquire.Client import ACLRule as _ACLRule
        from Acquire.ObjectStore import string_to_datetime \
            as _string_to_datetime

        f._location = _Location.from_data(data["location"])
        f._aclrule = _ACLRule.from_data(data["aclrule"])
        f._expires_datetime = _string_to_datetime(data["expires_datetime"])
        f._uid = data["uid"]

        return f
