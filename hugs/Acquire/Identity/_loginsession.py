
__all__ = ["LoginSession"]

_sessions_key = "identity/sessions"


class LoginSession:
    """This class holds all details of a single login session"""
    def __init__(self, username=None,
                 public_key=None, public_cert=None, ipaddr=None,
                 hostname=None, login_message=None, scope=None,
                 permissions=None):
        """Start a new login session for the user with specified
           username, passing in the additional data needed to
           request a login
        """
        if public_key is not None:
            from Acquire.Crypto import PublicKey as _PublicKey
            if not isinstance(public_key, _PublicKey):
                raise TypeError("The public key must be of type PublicKey")

            if not isinstance(public_cert, _PublicKey):
                raise TypeError("The public certificate must be of "
                                "type PublicKey")

            self._username = username
            self._pubkey = public_key
            self._pubcert = public_cert

            from Acquire.ObjectStore import get_datetime_now \
                as _get_datetime_now
            from Acquire.ObjectStore import create_uuid as _create_uuid

            self._uid = _create_uuid()
            self._request_datetime = _get_datetime_now()
            self._status = None

            self._ipaddr = ipaddr
            self._hostname = hostname
            self._login_message = login_message
            self._scope = scope
            self._permissions = permissions

            # make sure this session is saved to the object store
            self._set_status("pending")
        else:
            self._uid = None

    def __str__(self):
        if self.is_null():
            return "LoginSession::null"
        else:
            return "LoginSession(uid=%s, status=%s)" % \
                            (self.uid(), self.status())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._uid == other._uid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_null(self):
        """Return whether or not this object is null"""
        return self._uid is None

    def username(self):
        """Return the username for this login session"""
        try:
            return self._username
        except:
            return None

    def encoded_username(self):
        """Return a safely-encoded version of the username. This is used
           to create safe keys in the object store
        """
        if self.is_null():
            raise PermissionError(
                "You cannot get the encoded username of a null LoginSession")

        from Acquire.Identity import _encode_username
        return _encode_username(self._username)

    def public_key(self):
        """Return the public key"""
        status = self.status()

        if status != "approved":
            from Acquire.Identity import LoginSessionError
            raise LoginSessionError(
                "You cannot get a public key from "
                "a session that is not fully approved (status = %s)" % status)

        return self._pubkey

    def public_certificate(self):
        """Return the public certificate"""
        status = self.status()

        if status != "approved":
            from Acquire.Identity import LoginSessionError
            raise LoginSessionError(
                "You cannot get a public certificate from "
                "a session that is not fully approved (status = %s)" % status)

        return self._pubcert

    def request_source(self):
        """Return the IP address of the source of
           this request. This could be used to rate limit someone
           who is maliciously requesting logins...
        """
        if self.is_null():
            return None

        return self._ipaddr

    def uid(self):
        """Return the UID of this request"""
        if self.is_null():
            return None

        return self._uid

    @staticmethod
    def to_short_uid(long_uid):
        """Return the short UID version of the passed long uid"""
        return long_uid[:8]

    def short_uid(self):
        """Return a short UUID that will be used to
           provide a more human-readable session ID
        """
        if self._uid:
            return LoginSession.to_short_uid(self._uid)
        else:
            return None

    def login_url(self):
        """Return the login URL to login to this session. This is
           the URL of this identity service plus the
           short UID of the session
        """
        from Acquire.Service import get_this_service as _get_this_service
        service = _get_this_service(need_private_access=False)
        service_uid = service.uid()
        short_uid = self.short_uid()
        short_uid = ".".join([short_uid[i:i+2]
                             for i in range(0, len(short_uid), 2)])
        # eventually allow the service provider to configure this url
        url = "https://login.acquire-aaai.com"

        return "%s?id=%s/%s" % (url, service_uid, short_uid)

    def regenerate_uid(self):
        """Regenerate the UUID as there has been a clash"""
        if not self.is_null():
            from Acquire.ObjectStore import create_uuid as _create_uuid
            self._uid = _create_uuid()

    def creation_time(self):
        """Return the date and time when this was created"""
        if self.is_null():
            return None

        return self._request_datetime

    def login_time(self):
        """Return the date and time when the user logged in. This
           returns None if the user has not yet logged in
        """
        try:
            return self._login_datetime
        except:
            return None

    def logout_time(self):
        """Return the date and time when the user logged out. This
           returns None if the user has not yet logged out
        """
        try:
            return self._logout_datetime
        except:
            return None

    def seconds_since_creation(self):
        """Return the number of seconds since this request was
           created
        """
        if self.is_null():
            return None

        from Acquire.ObjectStore import get_datetime_now \
            as _get_datetime_now
        delta = _get_datetime_now() - self._request_datetime
        return delta.total_seconds()

    def status(self):
        """Return the status of this login session"""
        if self.is_null():
            return "null"
        else:
            return self._status

    @staticmethod
    def get_status(uid):
        """Return the status of the LoginSession with specified UID"""
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        bucket = _get_service_account_bucket()

        try:
            key = "%s/status/%s" % (_sessions_key, uid)
            status = _ObjectStore.get_string_object(bucket=bucket, key=key)
        except:
            status = None

        if status is None:
            from Acquire.Identity import LoginSessionError
            raise LoginSessionError(
                "Cannot find a session with UID '%s'" % uid)

        return status

    def _set_status(self, status):
        """Internal function to set the status of the session.
           This ensures that the data for the session is saved
           into the correct part of the object store
        """
        if self.is_null():
            raise PermissionError(
                "Cannot set the status of a null LoginSession")

        if status not in ["approved", "pending", "denied",
                          "suspicious", "logged_out"]:
            raise ValueError("Cannot set an invalid status '%s'" % status)

        if status == self._status:
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        bucket = _get_service_account_bucket()
        key = self._get_key()

        try:
            _ObjectStore.delete_object(bucket=bucket, key=key)
        except:
            pass

        self._status = status
        key = self._get_key()
        _ObjectStore.set_object_from_json(bucket=bucket, key=key,
                                          data=self.to_data())
        key = "%s/status/%s" % (_sessions_key, self._uid)
        _ObjectStore.set_string_object(bucket=bucket, key=key,
                                       string_data=status)

    def set_suspicious(self):
        """Put this login session into a suspicious state. This
           will be because weird activity has been detected which indicates
           that the session may be have been cracked. A login session
           in a suspicious state should not be granted any permissions.
        """
        if not self.is_null():
            self._set_status("suspicious")

    def set_approved(self, user_uid=None, device_uid=None):
        """Register that this request has been approved, optionally
           providing data about the user who approved the session
           and the device from which the session was approved
        """
        if self.is_null():
            raise PermissionError(
                "You cannot approve a null LoginSession!")

        if self.status() != "pending":
            from Acquire.Identity import LoginSessionError
            raise LoginSessionError(
                "You cannot approve a login session "
                "that is not in the 'unapproved' state. This login "
                "session is in the '%s' state." % self.status())

        from Acquire.ObjectStore import get_datetime_now \
            as _get_datetime_now
        self._login_datetime = _get_datetime_now()
        self._user_uid = user_uid
        self._device_uid = device_uid

        self._set_status("approved")

    def _clear_keys(self):
        """Function called to remove all keys
           from this session, as it has now been terminated
           (and so the keys are no longer valid)
        """
        self._pubkey = None

    def set_denied(self):
        """Register that this request has been denied"""
        if self.is_null():
            raise PermissionError(
                "You cannot deny a null LoginSession!")

        self._clear_keys()
        self._pubcert = None
        self._set_status("denied")

    def set_logged_out(self, authorisation=None, signature=None):
        """Register that this request has been closed as
           the user has logged out. If an authorisation is
           passed then verify that this is correct
        """
        if self.is_null():
            raise PermissionError(
                "You cannot logout from a null LoginSession!")

        if authorisation is not None:
            from Acquire.Identity import Authorisation as _Authorisation
            if not isinstance(authorisation, _Authorisation):
                raise TypeError("The authorisation must be type Authorisation")

            authorisation.verify(resource="logout %s" % self.uid())

            if authorisation.user_uid() != self.user_uid():
                raise PermissionError(
                    "The user '%s' does not have permission to logout "
                    "a session owned by %s" % (authorisation.user_uid(),
                                               self.user_uid()))

        if signature is not None:
            message = "logout %s" % self.uid()
            self._pubcert.verify(signature=signature, message=message)

        from Acquire.ObjectStore import get_datetime_now \
            as _get_datetime_now

        self._logout_datetime = _get_datetime_now()
        self._clear_keys()
        self._set_status("logged_out")

    def login(self, user_uid=None, device_uid=None):
        """Convenience function to set the session into the logged in state"""
        self.set_approved(user_uid=user_uid, device_uid=device_uid)

    def logout(self, authorisation=None, signature=None):
        """Convenience function to set the session into the logged out state"""
        self.set_logged_out(authorisation=authorisation,
                            signature=signature)

    def is_suspicious(self):
        """Return whether or not this session is suspicious"""
        return self.status() == "suspicious"

    def is_approved(self):
        """Return whether or not this session is open and
           approved by the user"""
        return self.status() == "approved"

    def is_logged_out(self):
        """Return whether or not this session has logged out"""
        return self.status() == "logged_out"

    def login_message(self):
        """Return any login message that has been set by the user"""
        try:
            return self._login_message
        except:
            return None

    def scope(self):
        """Return the scope requested for this login session"""
        try:
            return self._localised_scope
        except:
            try:
                return self._scope
            except:
                return None

    def permissions(self):
        """Return the permissions requested for this login session"""
        try:
            return self._localised_permissions
        except:
            try:
                return self._permissions
            except:
                return None

    def hostname(self):
        """Return the user-supplied hostname of the host making the
           login request
        """
        try:
            return self._hostname
        except:
            return None

    def ipaddr(self):
        """Return the user-supplied IP address of the host making the
           login request
        """
        try:
            return self._ipaddr
        except:
            return None

    def user_uid(self):
        """If known, return the UID of the user who approved this
           session
        """
        try:
            return self._user_uid
        except:
            return None

    def device_uid(self):
        """If known, return the UID of the device from which the
           user approved this session
        """
        try:
            return self._device_uid
        except:
            return None

    def _get_key(self):
        return "%s/%s/%s/%s" % (_sessions_key, self.status(),
                                self.short_uid(), self.uid())

    def save(self):
        """Save the current state of this LoginSession to the
           object store
        """
        if self.is_null():
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        bucket = _get_service_account_bucket()
        key = self._get_key()

        _ObjectStore.set_object_from_json(bucket=bucket, key=key,
                                          data=self.to_data())

    def _localise(self, scope, permissions):
        """Localise this session for the specified scope and permissions.
           This will raise an exception if the scope or permissions are
           greater than those embedded into the session
        """
        # DO SOME WORK HERE TO ASSERT THAT THE SCOPE AND PERMISSIONS
        # ARE ACCEPTABLE

        if scope is None:
            self._localised_scope = self.scope()
        else:
            self._localised_scope = scope

        if permissions is None:
            self._localised_permissions = self.permissions()
        else:
            self._localised_permissions = permissions

    @staticmethod
    def load(status=None, short_uid=None, uid=None,
             scope=None, permissions=None):
        """Load and return a LoginSession specified from either a
           short_uid or a long uid. Note that if more than one
           session matches the short_uid then you will get a list
           of LoginSessions returned
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        if short_uid is None and uid is None:
            raise PermissionError(
                "You must supply one of the short_uid or uid to load "
                "a LoginSession!")

        if status is None:
            if uid is None:
                raise PermissionError(
                    "You must supply the full UID to get the status "
                    "of a specific login session")

            status = LoginSession.get_status(uid=uid)

        bucket = _get_service_account_bucket()

        if uid is not None:
            short_uid = LoginSession.to_short_uid(uid)

            key = "%s/%s/%s/%s" % (_sessions_key, status, short_uid, uid)
            try:
                data = _ObjectStore.get_object_from_json(bucket=bucket,
                                                         key=key)
                session = LoginSession.from_data(data)
            except:
                from Acquire.Identity import LoginSessionError
                raise LoginSessionError(
                    "There is no valid session with UID %s in "
                    "state %s" % (uid, status))

            session._localise(scope=scope, permissions=permissions)
            return session

        # the user may pass in the short_uid as YY.YY.YY.YY
        # so remove all dots
        short_uid = short_uid.replace(".", "")

        prefix = "%s/%s/%s/" % (_sessions_key, status, short_uid)

        print(prefix)

        try:
            keys = _ObjectStore.get_all_objects_from_json(bucket=bucket,
                                                          prefix=prefix)
        except:
            keys = {}

        sessions = []

        for data in keys.values():
            try:
                session = LoginSession.from_data(data)
                session._localise(scope=scope, permissions=permissions)
                sessions.append(session)
            except:
                pass

        if len(sessions) == 0:
            from Acquire.Identity import LoginSessionError
            raise LoginSessionError(
                "There is no valid session with short UID %s "
                "in state %s" % (short_uid, status))
        elif len(sessions) == 1:
            sessions = sessions[0]

        return sessions

    def to_data(self):
        """Return a data version (dictionary) of this LoginSession
           that can be serialised to json
        """
        if self.is_null():
            return {}

        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string

        data = {}
        data["uid"] = self._uid
        data["username"] = self._username
        data["request_datetime"] = _datetime_to_string(self._request_datetime)
        data["public_certificate"] = self._pubcert.to_data()
        data["status"] = self._status

        if self._pubkey is not None:
            data["public_key"] = self._pubkey.to_data()

        try:
            data["login_datetime"] = _datetime_to_string(self._login_datetime)
        except:
            pass

        try:
            data["logout_datetime"] = _datetime_to_string(
                                            self._logout_datetime)
        except:
            pass

        try:
            data["ipaddr"] = self._ipaddr
        except:
            pass

        try:
            data["hostname"] = self._hostname
        except:
            pass

        try:
            data["login_message"] = self._login_message
        except:
            pass

        try:
            data["scope"] = self._scope
        except:
            pass

        try:
            data["permissions"] = self._permissions
        except:
            pass

        try:
            data["user_uid"] = self._user_uid
        except:
            pass

        try:
            data["device_uid"] = self._device_uid
        except:
            pass

        return data

    @staticmethod
    def from_data(data):
        """Return a LoginSession constructed from the passed data
           (dictionary)
        """
        l = LoginSession()

        if data is not None and len(data) > 0:
            from Acquire.ObjectStore import string_to_datetime \
                as _string_to_datetime
            from Acquire.Crypto import PublicKey as _PublicKey

            l._uid = data["uid"]
            l._username = data["username"]
            l._request_datetime = _string_to_datetime(
                                        data["request_datetime"])
            l._pubcert = _PublicKey.from_data(data["public_certificate"])
            l._status = data["status"]

            try:
                l._pubkey = _PublicKey.from_data(data["public_key"])
            except:
                l._pubkey = None

            try:
                l._login_datatime = _string_to_datetime(
                                            data["login_datetime"])
            except:
                pass

            try:
                l._logout_datetime = _string_to_datetime(
                                            data["logout_datetime"])
            except:
                pass

            try:
                l._ipaddr = data["ipaddr"]
            except:
                pass

            try:
                l._hostname = data["hostname"]
            except:
                pass

            try:
                l._login_message = data["login_message"]
            except:
                pass

            try:
                l._scope = data["scope"]
            except:
                pass

            try:
                l._permissions = data["permissions"]
            except:
                pass

            try:
                l._user_uid = data["user_uid"]
            except:
                pass

            try:
                l._device_uid = data["device_uid"]
            except:
                pass

        return l
