
__all__ = ["Authorisation"]


class Authorisation:
    """This class holds the information needed to show that a user
       has authorised an action. This contains a signed token that
       records the time that the authorisation that was signed, together
       with an extra key (or secret) that can be used by the user
       and provider to verify that the authorisation is for the
       correct resource
    """
    def __init__(self, resource=None, user=None,
                 testing_key=None, testing_user_guid=None):
        """Create an authorisation for the passed resource
           that is authorised by the passed user (who must be authenticated)

           If testing_key is passed, then this authorisation is being
           tested as part of the unit tests
        """

        if resource is not None:
            resource = str(resource)

        self._signature = None
        self._last_validated_datetime = None
        self._scope = None
        self._permissions = None
        self._pubcert = None

        if resource is not None:
            if user is None and testing_key is None:
                raise ValueError(
                    "You must pass in an authenticated user who will "
                    "provide authorisation for resource '%s'" % resource)

        from Acquire.ObjectStore import get_datetime_now \
            as _get_datetime_now
        from Acquire.ObjectStore import create_uuid as _create_uuid

        if user is not None:
            from Acquire.Client import User as _User

            if not isinstance(user, _User):
                raise TypeError("The passed user must be of type User")

            elif not user.is_logged_in():
                raise PermissionError(
                    "The passed user '%s' must be authenticated to enable "
                    "you to generate an authorisation for the account")

            self._user_uid = user.uid()
            self._session_uid = user.session_uid()
            self._identity_url = user.identity_service().canonical_url()
            self._identity_uid = user.identity_service_uid()
            self._auth_datetime = _get_datetime_now()
            self._uid = _create_uuid(short_uid=True,
                                     include_date=self._auth_datetime)
            self._siguid = user.signing_key().sign(self._uid)

            message = self._get_message(resource)
            self._signature = user.signing_key().sign(message)

            self._last_validated_datetime = _get_datetime_now()
            self._last_verified_resource = resource
            self._last_verified_key = None

            if user.guid() != self.user_guid():
                # interesting future case when we allow individual users
                # to be identified by multiple identity services...
                raise PermissionError(
                    "We do not yet support a single user being identified "
                    "by multiple identity services: %s versus %s" %
                    (user.guid(), self.user_guid()))

        elif testing_key is not None:
            self._user_uid = "some user uid"
            self._session_uid = "some session uid"
            self._identity_url = "some identity_url"
            self._identity_uid = "some identity uid"
            self._auth_datetime = _get_datetime_now()
            self._uid = _create_uuid(short_uid=True,
                                     include_date=self._auth_datetime)
            self._is_testing = True
            self._testing_key = testing_key

            if testing_user_guid is not None:
                parts = testing_user_guid.split("@")
                self._user_uid = parts[0]
                self._identity_uid = parts[1]

            message = self._get_message(resource)
            self._signature = testing_key.sign(message)
            self._siguid = testing_key.sign(self._uid)

            self._last_validated_datetime = _get_datetime_now()
            self._last_verified_resource = resource
            self._last_verified_key = testing_key.public_key()

    def is_null(self):
        """Return whether or not this authorisation is null"""
        return self._signature is None

    def _get_message(self, resource=None, matched_resource=False):
        """Internal function that is used to generate the message for
           the resource that is signed. This message
           encodes information about the user and identity service that
           signed the message, as well as the resource. This helps
           prevent tamporing with the data in this authorisation.

           If 'matched_resource' is True then this will return the
           message based on the previously-verified resource
           (as we have already determined that the user knows what
           the resource is)
        """
        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string

        if matched_resource:
            resource = self._last_verified_resource

        if resource is None:
            return "%s|%s|%s|%s" % (
                self._user_uid, self._session_uid,
                self._identity_uid,
                _datetime_to_string(self._auth_datetime))
        else:
            return "%s|%s|%s|%s|%s" % (
                self._user_uid, self._session_uid,
                self._identity_uid, str(resource),
                _datetime_to_string(self._auth_datetime))

    def __str__(self):
        try:
            return "Authorisation(signature=%s)" % self._signature
        except:
            return "Authorisation()"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._signature == other._signature
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def _fix_integer(self, value, max_value):
        max_value = int(max_value)

        if value is None:
            return max_value
        else:
            try:
                value = int(value)
            except:
                return max_value

        if value <= 0 or value > max_value:
            return max_value
        else:
            return value

    def from_user(self, user_uid, service_uid):
        """Return whether or not this authorisation comes from the user
           with passed user_uid registered on the passed service_uid
        """
        return (user_uid == self._user_uid) and \
               (service_uid == self._identity_uid)

    def uid(self):
        """Return the UID of this authorisation. This will be signed by
           the user and can be used a use-once record by a service to
           validate that they have not seen this authorisation before
        """
        if self.is_null():
            return None
        else:
            return self._uid

    def user_uid(self):
        """Return the UID of the user who created this authorisation"""
        if self.is_null():
            return None
        else:
            return self._user_uid

    def user_guid(self):
        """Return the global UID for this user"""
        return "%s@%s" % (self.user_uid(), self.identity_uid())

    def identifiers(self):
        """Return a dictionary of the full set of identifiers attached
           to this authorisation (e.g. user_guid, group_guid(s) etc.)
        """
        return {"user_guid": self.user_guid()}

    def session_uid(self):
        """Return the login session that authenticated the user"""
        if self.is_null():
            return None
        else:
            return self._session_uid

    def identity_url(self):
        """Return the URL of the identity service that authenticated
           the user
        """
        if self.is_null():
            return None
        else:
            return self._identity_url

    def identity_uid(self):
        """Return the UID of the identity service that authenticated
           the user
        """
        if self.is_null():
            return None
        else:
            return self._identity_uid

    def signature_time(self):
        """Return the time when the authentication was signed"""
        if self.is_null():
            return None
        else:
            return self._auth_datetime

    def last_verification_time(self):
        """Return the last time this authorisation was verified. Note that
           you should re-verify authorisations periodically, to ensure that
           they identity service is still happy that the login session was
           not suspicious
        """
        if self.is_null():
            return None
        else:
            return self._last_validated_datetime

    def last_verified_resource(self):
        """Return the resource that was used for the last successful
           verification of this authorisation. This returns None
           if this has not been verified before
        """
        try:
            return self._last_verified_resource
        except:
            return None

    def signature(self):
        """Return the actual signature"""
        if self.is_null():
            return None
        else:
            return self._signature

    def is_stale(self, stale_time=7200):
        """Return whether or not this authorisation is stale.
           'stale_time' is the number of seconds after which
           the authorisation is considered stale (and thus
           no longer valid)
        """
        stale_time = self._fix_integer(stale_time, 365*24*7200)

        from Acquire.ObjectStore import get_datetime_now \
            as _get_datetime_now

        now = _get_datetime_now()

        return ((now - self._auth_datetime).seconds > stale_time)

    def _get_user_public_cert(self, scope=None, permissions=None):
        """Internal function that returns the public certificate
           of the user who signed this authorisation. This will
           check that the authorisation was not signed after the
           user logged out, as well as validating the services
           that provide the user session keys etc.
        """
        must_fetch = False

        try:
            if scope != self._scope or permissions != self._permissions:
                must_fetch = True
        except:
            must_fetch = True

        if self._pubcert is not None:
            if not must_fetch:
                try:
                    return self._pubcert
                except:
                    pass

        try:
            testing_key = self._testing_key
        except:
            testing_key = None

        if testing_key is not None:
            if not self._is_testing:
                raise PermissionError(
                    "You cannot pass a test key to a non-testing "
                    "Authorisation")

            return testing_key

        # we need to get the public signing key for this session
        from Acquire.Service import get_trusted_service \
            as _get_trusted_service
        from Acquire.ObjectStore import get_datetime_now \
            as _get_datetime_now

        try:
            identity_service = _get_trusted_service(self._identity_url)
        except:
            raise PermissionError(
                "Unable to verify the authorisation as we do not trust "
                "the identity service at %s" % self._identity_url)

        if not identity_service.can_identify_users():
            raise PermissionError(
                "Cannot verify an Authorisation that does not use a "
                "valid identity service")

        if identity_service.uid() != self._identity_uid:
            raise PermissionError(
                "Cannot auth_once this Authorisation as the actual UID of "
                "the identity service at '%s' (%s) does not match "
                "the UID of the service that signed this authorisation "
                "(%s)" % (self._identity_url, identity_service.uid(),
                          self._identity_uid))

        response = identity_service.get_session_info(
                                session_uid=self._session_uid,
                                scope=scope, permissions=permissions)

        try:
            user_uid = response["user_uid"]
        except:
            pass

        if self._user_uid != user_uid:
            raise PermissionError(
                "Cannot verify the authorisation as there is "
                "disagreement over the UID of the user who signed "
                "the authorisation. %s versus %s" %
                (self._user_uid, user_uid))

        try:
            logout_datetime = _string_to_datetime(
                                    response["logout_datetime"])
        except:
            logout_datetime = None

        if logout_datetime:
            # the user has logged out from this session - ensure that
            # the authorisation was created before the user logged out
            if logout_datetime < self.signature_time():
                raise PermissionError(
                    "This authorisation was signed after the user logged "
                    "out. This means that the authorisation is not valid. "
                    "Please log in again and create a new authorisation.")

        from Acquire.Crypto import PublicKey as _PublicKey
        pubcert = _PublicKey.from_data(response["public_cert"])

        self._pubcert = pubcert
        self._scope = scope
        self._permissions = permissions
        return pubcert

    def assert_once(self, stale_time=7200, scope=None,
                    permissions=None):
        """Assert that this is in the one and only time that this
           service has seen this authorisation. This records the
           UID of the authorisation to the object store and then
           verifies that the signature of the UID is correct.

           There is a small race condition if the service asserts
           the authorisation at the exact same time, but this is
           a highly unlikely occurance. The aim is to prevent
           replay attacks.
        """
        if self.is_null():
            raise PermissionError("Cannot assert_once a null Authorisation")

        if self.is_stale(stale_time):
            raise PermissionError("Cannot assert_once a stale Authorisation")

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket
        from Acquire.ObjectStore import get_datetime_now_to_string \
            as _get_datetime_now_to_string

        bucket = _get_service_account_bucket()
        authkey = "auth_once/%s" % self._uid
        now = _get_datetime_now_to_string()

        try:
            data = _ObjectStore.get_string_object(bucket=bucket, key=authkey)
        except:
            data = None

        if data is not None:
            raise PermissionError(
                "Cannot auth_once the authorisation as it has been used "
                "before on this service!")

        # This is the first time this authorisation has been seen.
        # Record this to the object store to prevent anyone else
        # from using this authorisation on this service. There is a
        # small race condition here, but this would be extremely
        # challenging to exploit, and mitigating it would be a
        # significant performance problem. Ideally the object store
        # would have a "test_and_set" to enable us to set only if
        # the previous value is None
        _ObjectStore.set_string_object(bucket=bucket, key=authkey,
                                       string_data=now)

        # Now validate that the signature of the UID is correct
        public_cert = self._get_user_public_cert(scope=scope,
                                                 permissions=permissions)

        if public_cert is None:
            raise PermissionError(
                "There is no public certificate for this user in "
                "scope '%s' with permissions '%s'" % (scope, permissions))

        try:
            public_cert.verify(self._siguid, self._uid)
        except Exception as e:
            raise PermissionError(
                "Cannot auth_once the authorisation as the signature "
                "is invalid! % s" % str(e))

    def is_verified(self, refresh_time=3600, stale_time=7200):
        """Return whether or not this authorisation has been verified. Note
           that this will cache any verification for 'refresh_time' (in
           seconds)

           'stale_time' gives the time (in seconds) beyond which the
           authorisation will be considered stale (and thus not valid).
           By default this is 7200 seconds (2 hours), meaning that the
           authorisation must be used within 2 hours to be valid.
        """
        refresh_time = self._fix_integer(refresh_time, 24*3600)

        from Acquire.ObjectStore import get_datetime_now \
            as _get_datetime_now

        now = _get_datetime_now()

        if self._last_validated_datetime is not None:
            if (now - self._last_validated_datetime).seconds < refresh_time:
                # no need to re-validate
                return not self.is_stale(stale_time)

        return False

    def verify(self, resource=None, refresh_time=3600, stale_time=7200,
               force=False, accept_partial_match=False,
               scope=None, permissions=None, return_identifiers=True):
        """Verify that this is a valid authorisation provided by the
           user for the passed 'resource'. This will
           cache the verification for 'refresh_time' (in seconds), but
           re-verification can be forced if 'force' is True.

           'stale_time' gives the time (in seconds) beyond which the
           authorisation will be considered stale (and thus not valid).
           By default this is 7200 seconds (2 hours), meaning that the
           authorisation must be used within 2 hours to be valid.

           If 'accept_partial_match' is True, then if this Authorisation
           has been previously validated, then this previous authorisation
           is valid if the previously-verified resource contains
           'resource', e.g. if you have previously verified that
           "create ABC123" is the verified resource, then this will
           still verify if "ABC123" if the partially-accepted match

           If 'scope' is passed, then verify that the user logged in
           and signed the authorisation with the required 'scope'.

           If 'permissions' is passed, then verify that the user
           logged in and signed the authorisation with at least
           the specified 'permissions'

           If 'testing_key' is passed, then this object is being
           tested as part of the unit tests

           If the authorisation was verified, then if 'return_identifiers'
           is True then this will return the full set of identifiers
           associated with the user who provided the authorisation
        """
        if self.is_null():
            raise PermissionError("Cannot verify a null Authorisation")

        if self.is_stale(stale_time):
            raise PermissionError("Cannot verify a stale Authorisation")

        matched_resource = False

        try:
            last_resource = self._last_verified_resource
        except:
            last_resource = None

        if last_resource is not None:
            if accept_partial_match:
                if resource is None:
                    matched_resource = True
                else:
                    matched_resource = (last_resource.find(resource) != -1)
            else:
                matched_resource = (resource == last_resource)

        if not force:
            if self.is_verified(refresh_time=refresh_time,
                                stale_time=stale_time):
                if matched_resource:
                    if return_identifiers:
                        return self.identifiers()
                    else:
                        return

        # Now validate that the signature of the UID is correct
        public_cert = self._get_user_public_cert(scope=scope,
                                                 permissions=permissions)

        message = self._get_message(resource=resource,
                                    matched_resource=matched_resource)

        try:
            public_cert.verify(self._signature, message)
        except:
            raise PermissionError(
                "Cannot verify the authorisation as the signature "
                "for resource '%s' is invalid!" % resource)

        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now
        self._last_validated_datetime = _get_datetime_now()
        self._last_verified_resource = resource
        self._last_verified_key = public_cert

        if return_identifiers:
            return self.identifiers()
        else:
            return

    @staticmethod
    def from_data(data):
        """Return an authorisation created from the json-decoded dictionary"""
        auth = Authorisation()

        if (data and len(data) > 0):
            from Acquire.ObjectStore import string_to_datetime \
                as _string_to_datetime
            from Acquire.ObjectStore import string_to_bytes \
                as _string_to_bytes

            auth._user_uid = data["user_uid"]
            auth._session_uid = data["session_uid"]
            auth._identity_url = data["identity_url"]
            auth._identity_uid = data["identity_uid"]
            auth._uid = data["uid"]
            parts = auth._uid.split("/")
            auth._auth_datetime = _string_to_datetime(parts[0])
            auth._signature = _string_to_bytes(data["signature"])
            auth._siguid = _string_to_bytes(data["siguid"])
            auth._last_validated_datetime = None

            if "is_testing" in data:
                auth._is_testing = data["is_testing"]

        return auth

    def to_data(self):
        """Return this object serialised to a json-encoded dictionary"""
        data = {}

        if self.is_null():
            return data

        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string
        from Acquire.ObjectStore import bytes_to_string \
            as _bytes_to_string

        data["user_uid"] = str(self._user_uid)
        data["session_uid"] = str(self._session_uid)
        data["identity_url"] = str(self._identity_url)
        data["identity_uid"] = str(self._identity_uid)
        data["uid"] = self._uid
        data["signature"] = _bytes_to_string(self._signature)
        data["siguid"] = _bytes_to_string(self._siguid)

        try:
            data["is_testing"] = self._is_testing
        except:
            pass

        return data
