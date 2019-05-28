
import uuid as _uuid
import json as _json
import datetime as _datetime

from cachetools import LRUCache as _LRUCache
from cachetools import cached as _cached

__all__ = ["Service"]

_cache_service_user = _LRUCache(maxsize=5)


class ServiceError(Exception):
    pass


def _create_service_user(service_type, service_uid, service_public_key):
    """This function is called to create the service user account for
       this service. The service user is the actual user who manages
       and authorises everything for this service. It it not possible
       to login as this user from outside the service. Instead, you
       login as one of the admin accounts, and then instruct the
       service user to perform the various tasks. There is one, and
       only one service user account for each service. It is as
       unchanging as the service UID. If the service user account
       already exists, then this function will raise an exception.

       If successful, this will return the username, UID and login secrets
       of the new service account
    """
    from Acquire.Identity import UserAccount as _UserAccount
    from Acquire.Crypto import PrivateKey as _PrivateKey
    from Acquire.Client import Credentials as _Credentials

    if service_type is None:
        username = "service_principal"
    else:
        username = "%s_principal" % service_type

    password = _PrivateKey.random_passphrase()

    encoded_password = _Credentials.encode_password(
                                    identity_uid=service_uid,
                                    password=password,
                                    device_uid=None)

    (user_uid, otp) = _UserAccount.create(
                                    username=username,
                                    password=encoded_password,
                                    _service_uid=service_uid,
                                    _service_public_key=service_public_key)

    user_secret = {"password": password,
                   "otpsecret": otp._secret}

    return (username, user_uid, user_secret)


@_cached(_cache_service_user)
def _login_service_user(service_uid):
    """Login to the service user account for the service with
       UID 'service_uid'. The service user account is an
       account that provides full control for this service. The "admin"
       accounts invoke actions by logging into the service account and
       authorising actions using that account. It is not possible to
       login as the service user from outside the service. It is an
       account that is internal to the service.
    """
    from Acquire.Service import get_this_service as _get_this_service
    from Acquire.Client import User as _User
    from Acquire.Crypto import OTP as _OTP
    from Acquire.Identity import LoginSession as _LoginSession
    from Acquire.Client import Credentials as _Credentials

    service = _get_this_service(need_private_access=True)

    if service.uid() != service_uid:
        from Acquire.Service import ServiceAccountError
        raise ServiceAccountError(
            "You cannot login to the service account for '%s' from "
            "the service running at '%s'" % (service.uid(),
                                             service_uid))

    secrets = service.skeleton_key().decrypt(service.service_user_secrets())
    secrets = _json.loads(secrets)

    password = secrets["password"]
    otpsecret = secrets["otpsecret"]

    user = _User(username=service.service_user_name(),
                 identity_url=service.canonical_url())

    user.request_login()
    short_uid = _LoginSession.to_short_uid(user.session_uid())

    credentials = _Credentials(username=user.username(),
                               short_uid=short_uid,
                               device_uid=None,
                               password=password,
                               otpcode=_OTP(otpsecret).generate())

    login_args = {"short_uid": short_uid,
                  "credentials": credentials.to_data(
                                        identity_uid=service.uid())}

    secrets = None
    credentials = None

    service.call_function(function="login", args=login_args)

    login_args = None

    user.wait_for_login()

    return user


class Service:
    """This class represents a service in the system. Services
       will either be identity services, access services,
       storage services or accounting services.
    """
    def __init__(self):
        """Construct a new service of the specified type, with
           the specified URL."""
        self._uid = None

    @staticmethod
    def get_canonical_url(service_url, service_type=None):
        """Return the canonical URL from the passed service_url.
           If 'service_type' is specified, then this will also
           add on extra paths needed for the services. For
           example,

           get_canonical_url("fn.acquire-aaai.com", service_type="identity")

           would return

           "fn.acquire-aaai.com/identity"
        """
        from urllib.parse import urlparse as _urlparse

        if not service_url.startswith("http"):
            service_url = "https://%s" % service_url

        # break the URL up into parts
        p = _urlparse(service_url)
        parts = p.netloc.split(":")
        domain = parts[0]
        path = p.path

        if domain.find(".") == -1:
            # this is a testing domain
            return domain

        if path is None or len(path) == 0:
            if service_type is None:
                canonical_url = domain
            else:
                canonical_url = "%s/t/%s" % (domain, service_type)
        else:
            canonical_url = "%s%s" % (domain, path)

        while canonical_url.endswith("/"):
            canonical_url = canonical_url[0:-1]

        return canonical_url

    @staticmethod
    def create(service_type, service_url, _testing=False):
        """Conduct stage1 of the construction of a new service. This
           creates the initial setup, creating a service with sufficient
           info to survive registration with a Registry. The second stage
           is performed automatically when the Registry confirms
           registration
        """
        if service_type not in ["identity", "access", "compute",
                                "registry", "accounting", "storage"]:
            raise ServiceError("Services of type '%s' are not allowed!" %
                               service_type)

        from Acquire.Crypto import PrivateKey as _PrivateKey

        service = Service()

        from urllib.parse import urlparse as _urlparse

        if not service_url.startswith("http"):
            service_url = "https://%s" % service_url

        # break the URL up into parts
        p = _urlparse(service_url)
        parts = p.netloc.split(":")
        service._domain = parts[0]
        service._schemes = [p.scheme]
        service._ports = {}

        if len(parts) == 1:
            service._ports[p.scheme] = None
        elif len(parts) == 2:
            service._ports[p.scheme] = parts[1]
        else:
            raise ValueError("Cannot interpret URL %s" % service_url)

        service._path = p.path

        service._service_type = service_type
        service._canonical_url = Service.get_canonical_url(service_url)

        while service._canonical_url.endswith("/"):
            service._canonical_url = service._canonical_url[0:-1]

        service._uid = "STAGE1 %s" % _PrivateKey.random_passphrase()

        service._privkey = _PrivateKey()
        service._privcert = _PrivateKey()

        service._pubkey = service._privkey.public_key()
        service._pubcert = service._privcert.public_key()

        # generate 'dummy' old keys - these will be replaced as
        # the real keys are updated
        service._lastkey = service._privkey
        service._lastcert = service._privcert

        from Acquire.ObjectStore import get_datetime_now as \
            _get_datetime_now
        service._last_key_update = _get_datetime_now()
        service._key_update_interval = 3600 * 24 * 7  # update keys weekly

        service._skeleton_key = _PrivateKey()
        service._public_skeleton_key = service._skeleton_key.public_key()

        service._service_user_name = None
        service._service_user_uid = None
        service._service_user_secrets = None

        return service

    def create_stage2(self, service_uid, response):
        """Perform stage 2 of construction. This is called by the registry
           that registered this service, which will provide the
           service_uid
        """
        if not self._uid.startswith("STAGE1"):
            raise PermissionError(
                "You cannot enter stage2 until stage1 has been completed!")

        if response != self._uid:
            raise PermissionError(
                "Invalid response from the registry service. Not trusted!")

        from Acquire.Crypto import PrivateKey as _PrivateKey
        self._uid = service_uid

        skelkey = self._skeleton_key.public_key()

        (username, uid, secrets) = _create_service_user(
                                        service_type=self._service_type,
                                        service_uid=self._uid,
                                        service_public_key=skelkey)

        self._service_user_name = username
        self._service_user_uid = uid
        self._service_user_secrets = self._skeleton_key.encrypt(
                                                _json.dumps(secrets))

    def __str__(self):
        if self.is_null():
            return "%s(NULL)" % self.__class__.__name__
        else:
            return "%s(url=%s, uid=%s)" % (self.__class__.__name__,
                                           self.canonical_url(),
                                           self.uid())

    def uid(self):
        """Return the uuid of this service. This MUST NEVER change, as
           the UID uniquely identifies this service to all other
           services
        """
        if self.is_null():
            return None
        else:
            return self._uid

    def registry_uid(self):
        """Return the uid of the registry service that registered
           this service
        """
        if self.is_null():
            return None
        else:
            root = self.uid().split("-")[0]
            return "%s-%s" % (root, root)

    def is_null(self):
        """Return whether or not this is null"""
        return self._uid is None

    def service_type(self):
        """Return the type of this service"""
        if self.is_null():
            return None
        else:
            return self._service_type

    def bucket(self):
        """Return the bucket you can use to read/write data to the
           object store associated with this service account
        """
        if self.is_null():
            return None

        from Acquire.Service import get_service_account_bucket as \
            _get_service_account_bucket
        return _get_service_account_bucket()

    def is_locked(self):
        """Return whether or not this service object is locked. Locked
           service objects don't contain copies of any private keys,
           and can be safely shared as a means of distributing public
           keys and certificates
        """
        if self.is_null():
            return True
        else:
            return self._skeleton_key is None

    def is_unlocked(self):
        """Return whether or not this service object is unlocked. Unlocked
           service objects have access to the skeleton key and other private
           keys. They should only run on the service. Locked service objects
           are what are returned by services to provide public keys and
           public certificates
        """
        if self.is_null():
            return False
        else:
            return self._skeleton_key is not None

    def get_trusted_service(self, service_url=None, service_uid=None):
        """Return the trusted service info for the service with specified
           service_url or service_uid"""
        if self.is_null():
            return None

        from ._get_services import get_trusted_service as \
            _get_trusted_service

        return _get_trusted_service(service_url=service_url,
                                    service_uid=service_uid)

    def get_session_info(self, session_uid,
                         scope=None, permissions=None):
        """Return information about the passed session,
           optionally limited to the provided scope and permissions
        """
        if self.is_null():
            return None

        from Acquire.Service import get_session_info as _get_session_info
        return _get_session_info(identity_url=self.canonical_url(),
                                 session_uid=session_uid,
                                 scope=scope, permissions=permissions)

    def assert_unlocked(self):
        """Assert that this service object is unlocked"""
        if self.is_locked():
            raise ServiceError(
                "Cannot complete operation as the service account '%s' "
                "is locked" % str(self))

    def assert_admin_authorised(self, authorisation, resource=None):
        """Validate that the passed authorisation is valid for the
           (optionally) specified resource, and that this has been
           authorised by one of the admin accounts of this service
        """
        if self.is_null() or authorisation.identity_uid() != self.uid():
            from Acquire.Identity import AuthorisationError
            raise AuthorisationError(
                "The authorisation has not been signed by one of the "
                "admin accounts on service '%s'" % str(self))

        from Acquire.Service import get_admin_users as _get_admin_users
        admin_users = _get_admin_users()

        if authorisation.user_uid() not in admin_users:
            raise AuthorisationError(
                "The authorisation has not been signed by one of the "
                "admin accounts on service '%s'" % str(self))

        authorisation.verify(resource)

    def last_key_update(self):
        """Return the datetime when the key and certificate of this
           service were last updated
        """
        try:
            return self._last_key_update
        except:
            return None

    def key_update_interval(self):
        """Return the time delta between server key updates"""
        if self.is_null():
            return None
        else:
            try:
                return _datetime.timedelta(seconds=self._key_update_interval)
            except:
                return _datetime.timedelta(seconds=1)

    def should_refresh_keys(self):
        """Return whether the keys and certificates need to be refreshed
           - i.e. more than 'key_update_interval' has passed since the last
           key update, or the service has changed in some marked way
        """
        if self.is_null():
            return False
        elif self._uid == "STAGE1":
            return True
        else:
            if self._service_user_uid is None or self._pubkey is None:
                return True

            try:
                from Acquire.ObjectStore import get_datetime_now as \
                    _get_datetime_now

                return _get_datetime_now() > (self._last_key_update +
                                              self.key_update_interval())
            except:
                return True

    def refresh_keys(self):
        """Refresh the keys and certificates"""
        if self.is_null():
            return

        if self.is_unlocked():
            # actually regenerate keys for the service - first save
            # the old private key (so we can decrypt data decrypted using
            # the old public key) and the old public certificate, so we
            # can verify data signed using the old private certificate
            self._lastkey = self._privkey
            self._lastcert = self._privcert

            # now generate a new key and certificate
            from Acquire.Crypto import PrivateKey as _PrivateKey
            self._privkey = _PrivateKey()
            self._privcert = _PrivateKey()
            self._pubkey = self._privkey.public_key()
            self._pubcert = self._privcert.public_key()

            # update the refresh time
            from Acquire.ObjectStore import get_datetime_now as \
                _get_datetime_now

            self._last_key_update = _get_datetime_now()
        else:
            from Acquire.Crypto import get_private_key as _get_private_key
            from ._function import call_function as _call_function

            # if our keys are old then pull the new ones from the server
            if self._pubcert is None:
                # we are initialising from scratch - hope this is over https
                response = _call_function(
                    self.service_url(),
                    response_key=_get_private_key("function"))
            else:
                # ask for an updated Service, ensuring the service responds
                # with a signature that we know was (once) valid
                response = _call_function(
                    self.service_url(),
                    response_key=_get_private_key("function"),
                    public_cert=self._pubcert)

            service = Service.from_data(response["service_info"],
                                        verify_data=True)

            if service.uid() != self.uid():
                raise ServiceError(
                    "Cannot update the service as the UID has changed. We "
                    "cannot move from %s to %s. Contact an administrator." %
                    (str(self), str(service)))

            # everything should be ok. Update this object with the new
            # keys and data
            from copy import copy as _copy
            self.__dict__ = _copy(service.__dict__)

    def can_identify_users(self):
        """Return whether or not this service can identify users.
           Most services can, at a minimum, identify their admin
           users. However, only true Identity Services can register
           and manage normal users
        """
        if self.is_null():
            return False
        else:
            return True

    def is_registry_service(self):
        """Return whether or not this is a registry service"""
        if self.is_null():
            return False
        else:
            return self._service_type == "registry"

    def is_identity_service(self):
        """Return whether or not this is an identity service"""
        if self.is_null():
            return False
        else:
            return self._service_type == "identity"

    def is_access_service(self):
        """Return whether or not this is an access service"""
        if self.is_null():
            return False
        else:
            return self._service_type == "access"

    def is_accounting_service(self):
        """Return whether or not this is an accounting service"""
        if self.is_null():
            return False
        else:
            return self._service_type == "accounting"

    def is_compute_service(self):
        """Return whether or not this is a compute service"""
        if self.is_null():
            return False
        else:
            return self._service_type == "compute"

    def is_storage_service(self):
        """Return whether or not this is a storage service"""
        if self.is_null():
            return False
        else:
            return self._service_type == "storage"

    def service_url(self, prefer_https=True):
        """Return the URL used to access this service. This includes
           the scheme, port etc. This is in contrast to the canonical
           url which just includes the URL
        """
        if self.is_null():
            return None
        else:
            if prefer_https and "https" in self._schemes:
                scheme = "https"
            else:
                scheme = self._schemes[0]

            port = self._ports[scheme]

            if port is None or len(port) == 0:
                return "%s://%s%s" % (scheme, self._domain, self._path)
            else:
                return "%s://%s:%s%s" % (scheme, self._domain,
                                         port, self._path)

    def canonical_url(self):
        """Return the canonical URL for this service (this is the URL the
           service thinks it has, and which it has used to register itself
           with all other services)
        """
        if self.is_null():
            return None
        else:
            return self._canonical_url

    def hostname(self):
        """Return the hostname of the canonical URL that provides
           this service
        """
        if self.is_null():
            return None
        else:
            return self._domain

    def uses_https(self):
        """Return whether or not the canonical URL of this service
           is connected to via https
        """
        if self.is_null():
            return False
        else:
            return self._schemes[0] == "https"

    def service_user_uid(self):
        """Return the UID of the service user account for this service"""
        if self.is_null():
            return None
        else:
            return self._service_user_uid

    def service_user_name(self):
        """Return the name of the service user account for this service"""
        if self.is_null():
            return None
        else:
            return self._service_user_name

    def service_user_secrets(self):
        """Return the (encrypted) secrets for the service user account.
           These will only be returned if you have unlocked this service.
           You need access to the skeleton key to decrypt these secrets
        """
        self.assert_unlocked()
        return self._service_user_secrets

    def login_service_user(self):
        """Return a logged in Acquire.Client.User for the service user.
           This can only be called inside the service, and when you
           have unlocked this service object
        """
        self.assert_unlocked()
        return _login_service_user(self.uid())

    def service_user_account_uid(self, accounting_service_url=None,
                                 accounting_service=None):
        """Return the UID of the financial account associated with
           this service on the passed accounting service
        """
        if self.is_null():
            return None

        from Acquire.Service import get_service_user_account_uid as \
            _get_service_user_account_uid

        if accounting_service is None:
            if accounting_service_url is None:
                raise ValueError(
                    "You must supply either an accounting service or "
                    "the URL of a valid accounting service!")

            accounting_service = self.get_trusted_service(
                                        accounting_service_url)

        if not accounting_service.is_accounting_service():
            raise TypeError(
                "The service '%s' is not an accounting service!"
                % str(accounting_service))

        return _get_service_user_account_uid(
                    accounting_service_uid=accounting_service.uid())

    def public_skeleton_key(self):
        """Return the public skeleton key. This is the public
           key for the matching private skeleton key, which is
           stored internally, and which should never be shared
           outside this service. It is used to encrypt data that
           should only be decryptable by the skeleton key
        """
        if self.is_null():
            return None
        else:
            return self._public_skeleton_key

    def skeleton_key(self):
        """Return the skeleton key used by this service. This is an
           unchanging key which is stored internally, should never be
           shared outside the service, and which is used to decrypt
           all data. Unlocking the service involves loading and
           decrypting this skeleton key
        """
        self.assert_unlocked()
        return self._skeleton_key

    def private_key(self):
        """Return the private key (if it has been unlocked)"""
        self.assert_unlocked()
        return self._privkey

    def private_certificate(self):
        """Return the private signing certificate (if it has been unlocked)"""
        self.assert_unlocked()
        return self._privcert

    def public_key(self):
        """Return the public key for this service"""
        if self.is_null():
            return None
        else:
            return self._pubkey

    def public_certificate(self):
        """Return the public signing certificate for this service"""
        if self.is_null():
            return None
        else:
            return self._pubcert

    def last_key(self):
        """Return the old private key for this service (if it has
           been unlocked). This was the key used before the last
           key update, and we store it in case we have to decrypt
           data that was recently encrypted using the old public key
        """
        self.assert_unlocked()
        return self._lastkey

    def last_certificate(self):
        """Return the old public certificate for this service. This was the
           certificate used before the last key update, and we store it
           in case we need to verify data signed using the old private
           certificate
        """
        if self.is_null():
            return None
        else:
            return self._lastcert

    def call_function(self, function, args=None):
        """Call the function 'func' on this service, optionally passing
           in the arguments 'args'. This is a simple wrapper around
           Acquire.Service.call_function which automatically
           gets the correct URL, encrypts the arguments using the
           service's public key, and supplies a key to encrypt
           the response (and automatically then decrypts the
           response)
        """
        if self.is_null():
            from Acquire.Service import RemoteFunctionCallError
            raise RemoteFunctionCallError(
                "You cannot call the function '%s' on a null service!" %
                function)

        from Acquire.Crypto import get_private_key as _get_private_key
        from ._function import call_function as _call_function

        if self.should_refresh_keys():
            self.refresh_keys()

        return _call_function(service_url=self.service_url(),
                              function=function,
                              args=args,
                              args_key=self.public_key(),
                              public_cert=self.public_certificate(),
                              response_key=_get_private_key("function"))

    def sign(self, message):
        """Sign the specified message"""
        if self.is_null():
            raise PermissionError("You cannot sign using a null service!")

        return self.private_certificate().sign(message)

    def verify(self, signature, message):
        """Verify that this service signed the message"""
        if self.is_null():
            raise PermissionError("You cannot verify using a null service!")

        self.public_certificate().verify(signature, message)

    def encrypt(self, message):
        """Encrypt the passed message"""
        if self.is_null():
            raise PermissionError("You cannot encrypt using a null service!")

        return self.public_key().encrypt(message)

    def decrypt(self, message):
        """Decrypt the passed message"""
        if self.is_null():
            raise PermissionError("You cannot decrypt using a null service!")

        return self.private_key().decrypt(message)

    def sign_data(self, data):
        """Sign the passed data, ready for transport. Data should be
           a json-serialisable dictionary. This will return a new
           json-serialisable dictionary, which will contain the
           signature and json-serialised original data, e.g. as;

           data = {"service_uid" : "SERVICE_UID",
                   "fingerprint" : "KEY_FINGERPRINT",
                   "signed_data" : "JSON_ENCODED_DATA",
                   "signature" : "SIG OF JSON_ENCODED_DATA"}
        """
        if self.is_null():
            raise PermissionError("You cannot sign using a null service!")

        from Acquire.ObjectStore import bytes_to_string as _bytes_to_string

        data = _json.dumps(data)

        return {"service_uid": str(self.uid()),
                "canonical_url": str(self.canonical_url()),
                "fingerprint": str(self.private_certificate().fingerprint()),
                "signed_data": data,
                "signature": _bytes_to_string(self.sign(data))
                }

    def verify_data(self, data):
        """Verify the passed data has been signed by this service. The
           passed data should have the same format as that produced
           by 'sign_data'. If the data is verified then this will
           return a json-deserialised dictionary of the verified data.
           Note that the 'service_uid' should match the UID of this
           service. The data should also contain the fingerprint of the
           key used to encrypt the data, enabling the service to
           perform key rotation and management.
        """
        if self.is_null():
            raise PermissionError("You cannot verify using a null service!")

        try:
            from Acquire.ObjectStore import string_to_bytes as _string_to_bytes

            service_uid = data["service_uid"]
            fingerprint = data["fingerprint"]
            signature = _string_to_bytes(data["signature"])
            data = data["signed_data"]
        except Exception as e:
            raise ServiceError(
                "The signed data is not of the correct format: %s" % str(e))

        if service_uid != self.uid():
            raise ServiceError(
                "Cannot verify the data as it wasn't signed for this "
                "service - unmatched service UID: %s versus %s" %
                (service_uid, self.uid()))

        if fingerprint != self.public_certificate().fingerprint():
            raise ServiceError(
                "Cannot verify the data as we don't recognise the "
                "fingerprint of the signing key: %s versus %s" %
                (fingerprint, self.public_certificate().fingerprint()))

        self.verify(signature, data)
        return _json.loads(data)

    def encrypt_data(self, data):
        """Encrypt the passed data, ready for transport to the service.
           Data should be a json-serialisable dictionary. This will
           return a new json-serialisable dictionary, which will contain
           the UID of the service this should be sent to (together with
           the canonical URL, which enables this data to be forwarded
           to where it needs to go), and the encrypted
           data, e.g. as;

           data = {"service_uid" : "SERVICE_UID",
                   "canonical_url" : "CANONICAL_URL",
                   "fingerprint" : "KEY_FINGERPRINT",
                   "encrypted_data" : "ENCRYPTED_DATA"}
        """
        if self.is_null():
            raise PermissionError("You cannot encrypt using a null service!")

        from Acquire.ObjectStore import bytes_to_string as _bytes_to_string
        return {"service_uid": str(self.uid()),
                "canonical_url": str(self.canonical_url()),
                "fingerprint": str(self.public_key().fingerprint()),
                "encrypted_data": _bytes_to_string(
                                    self.encrypt(_json.dumps(data)))
                }

    def decrypt_data(self, data):
        """Decrypt the passed data that has been encrypted and sent to
           this service (encrypted via the 'encrypt_data' function).
           This will return a json-deserialisable dictionary. Note that
           the 'service_uid' should match the UID of this
           service. The data should also contain the fingerprint of the
           key used to encrypt the data, enabling the service to
           perform key rotation and management.
        """
        if self.is_null():
            raise PermissionError("You cannot decrypt using a null service!")

        if isinstance(data, str):
            data = _json.loads(data)

        try:
            from Acquire.ObjectStore import string_to_bytes as _string_to_bytes
            service_uid = data["service_uid"]
            fingerprint = data["fingerprint"]
            data = _string_to_bytes(data["encrypted_data"])
        except Exception as e:
            raise ServiceError(
                "The encrypted data is not of the correct format: %s" % str(e))

        if service_uid != self.uid():
            raise ServiceError(
                "Cannot decrypt the data as it wasn't encrypted for this "
                "service - unmatched service UID: %s versus %s" %
                (service_uid, self.uid()))

        if fingerprint != self.private_key().fingerprint():
            raise ServiceError(
                "Cannot decrypt the data as we don't recognise the "
                "fingerprint of the encryption key: %s versus %s" %
                (fingerprint, self.private_key().fingerprint()))

        data = self.decrypt(data)
        return _json.loads(data)

    def dump_keys(self, include_old_keys=False):
        """Return a dump of the current key and certificate, so that
           we can keep a record of all keys that have been used. The
           returned json-serialisable dictionary contains the keys,
           their fingerprints, and the datetime when they were
           generated. If this is run on the service, then the keys
           are encrypted the password which is encrypted using the
           master key
        """
        if self.is_null():
            return {}

        dump = {}

        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string
        dump["datetime"] = _datetime_to_string(self._last_key_update)

        if self.is_unlocked():
            from Acquire.Crypto import PrivateKey as _PrivateKey
            from Acquire.ObjectStore import bytes_to_string as _bytes_to_string
            ranpas = _PrivateKey.random_passphrase()

            key = self.private_key()
            cert = self.private_certificate()

            dump[key.fingerprint()] = key.to_data(ranpas)
            dump[cert.fingerprint()] = cert.to_data(ranpas)

            if include_old_keys:
                key = self.last_key()
                cert = self.last_certificate()
                dump[key.fingerprint()] = key.to_data(ranpas)
                dump[cert.fingerprint()] = cert.to_data(ranpas)

            ranpas = _bytes_to_string(self._skeleton_key.encrypt(ranpas))
            dump["encrypted_passphrase"] = ranpas
        else:
            key = self.public_key()
            cert = self.public_certificate()

            dump[key.fingerprint()] = key.to_data()
            dump[cert.fingerprint()] = cert.to_data()

            if include_old_keys:
                cert = self.last_certificate()
                dump[cert.fingerprint()] = cert.to_data()

        return dump

    def load_keys(self, data):
        """Return the keys that were dumped by 'self.dump_keys()'.
           This returns a dictionary of the keys and datetime that
           they were created, indexed by their key fingerprints
        """
        if self.is_null():
            return

        # get all of the key fingerprints in this dictionary
        fingerprints = []
        for key in data.keys():
            if key not in ["datetime", "encrypted_passphrase"]:
                fingerprints.append(key)

        # now unpack everything
        result = {}

        from Acquire.ObjectStore import string_to_datetime as \
            _string_to_datetime
        result["datetime"] = _string_to_datetime(data["datetime"])

        if self.is_unlocked():
            from Acquire.Crypto import PrivateKey as _PrivateKey
            from Acquire.ObjectStore import string_to_bytes as _string_to_bytes
            ranpas = self._skeleton_key.decrypt(
                            _string_to_bytes(data["encrypted_passphrase"]))

            for fingerprint in fingerprints:
                result[fingerprint] = _PrivateKey.from_data(data[fingerprint],
                                                            ranpas)
        else:
            from Acquire.Crypto import PublicKey as _PublicKey
            for fingerprint in fingerprints:
                result[fingerprint] = _PublicKey.from_data(data[fingerprint])

        return result

    def get_key(self, fingerprint):
        """Return the key matching the passed fingerprint"""
        if self.is_null():
            return None

        if self.is_unlocked():
            if self._privkey.fingerprint() == fingerprint:
                return self._privkey
            elif self._privcert.fingerprint() == fingerprint:
                return self._privcert

            try:
                if self._lastkey.fingerprint() == fingerprint:
                    return self._lastkey
                elif self._lastcert.fingerprint() == fingerprint:
                    return self._lastcert
            except:
                pass
        else:
            if self._pubkey.fingerprint() == fingerprint:
                return self._pubkey
            elif self._pubcert.fingerprint() == fingerprint:
                return self._pubcert
            else:
                try:
                    if self._lastkey.fingerprint() == fingerprint:
                        key = self._lastkey
                    elif self._lastcert.fingerprint() == fingerprint:
                        key = self._lastcert
                except:
                    pass

        # we need to load the key from objstore
        unlocked = self.is_unlocked()
        from Acquire.Service import load_service_key_from_objstore \
            as _load_service_key_from_objstore

        key = _load_service_key_from_objstore(fingerprint)

        if key is None:
            from Acquire.Crypto import KeyManipulationError
            raise KeyManipulationError(
                "Unable to load the key or certificate with "
                "fingerprint '%s'" % fingerprint)

        if unlocked:
            from Acquire.Crypto import PrivateKey as _PrivateKey
            if type(key) is not _PrivateKey:
                from Acquire.Crypto import KeyManipulationError
                raise KeyManipulationError(
                    "Unable to load the private key or certificate with "
                    "fingerprint '%s'" % fingerprint)
        else:
            from Acquire.Crypto import PublicKey as _PublicKey
            try:
                key = key.public_key()
            except:
                pass

            if type(key) is not _PublicKey:
                from Acquire.Crypto import KeyManipulationError
                raise KeyManipulationError(
                    "Unable to load the public key or certificate with "
                    "fingerprint '%s'" % fingerprint)

        return key

    def is_evolution_of(self, other):
        """Return whether or not this service is an evolution of 'other'.
           Evolving means that this service is the same service as 'other',
           but at a later point in time with newer keys
        """
        if self.is_null():
            return False

        if self.validation_string() == other.validation_string():
            return True
        elif self.canonical_url() != other.canonical_url():
            return False
        elif self.uid() != other.uid():
            return False

        print("WARNING - NEED TO IMPLEMENT KEY EVOLUTION COMPARISON")

        return False

    def validation_string(self):
        """Return a string created from this object that can be signed
           to verify that all information was transmitted correctly
        """
        if self.is_null():
            return None

        return "%s:%s:%s:%s:%s:%s:%s:%s:%s" % (
            self._uid, self.canonical_url(), self._service_type,
            self._pubcert.fingerprint(), self._pubkey.fingerprint(),
            self._lastcert.fingerprint(), self._service_user_uid,
            self._last_key_update.isoformat(), self._key_update_interval)

    def to_data(self, password=None):
        """Serialise this key to a dictionary, using the supplied
           password to encrypt the private key and certificate"""

        data = {}

        if self.is_null():
            return data

        if self._uid.startswith("STAGE1"):
            data["uid"] = "STAGE1"
        else:
            data["uid"] = self._uid

        data["service_type"] = self._service_type
        data["domain"] = self._domain
        data["ports"] = self._ports
        data["schemes"] = self._schemes
        data["path"] = self._path
        data["canonical_url"] = self._canonical_url

        data["public_certificate"] = self._pubcert.to_data()
        data["public_key"] = self._pubkey.to_data()

        from Acquire.Crypto import PublicKey as _PublicKey
        from Acquire.ObjectStore import datetime_to_string as \
            _datetime_to_string

        if isinstance(self._lastcert, _PublicKey):
            data["last_certificate"] = self._lastcert.to_data()
        else:
            data["last_certificate"] = self._lastcert.public_key().to_data()

        data["last_key_update"] = _datetime_to_string(self._last_key_update)
        data["key_update_interval"] = self._key_update_interval

        data["service_user_name"] = self._service_user_name
        data["service_user_uid"] = self._service_user_uid

        from Acquire.ObjectStore import bytes_to_string as _bytes_to_string

        if (self.is_unlocked()) and (password is not None):
            # only serialise private data if a password was provided
            from Acquire.Crypto import PrivateKey as _PrivateKey

            secret_passphrase = _PrivateKey.random_passphrase()
            key_data = self._privkey.to_data(secret_passphrase)
            lastkey_data = self._lastkey.to_data(secret_passphrase)
            cert_data = self._privcert.to_data(secret_passphrase)

            secret_data = {"passphrase": secret_passphrase,
                           "private_key": key_data,
                           "last_key": lastkey_data,
                           "private_certificate": cert_data}

            secret_data = self._skeleton_key.encrypt(_json.dumps(secret_data))

            data["secret_data"] = _bytes_to_string(secret_data)
            data["skeleton_key"] = self._skeleton_key.to_data(password)
            data["public_skeleton_key"] = self._public_skeleton_key.to_data()

            # the service user secrets are already encrypted
            if self._service_user_secrets is not None:
                data["service_user_secrets"] = _bytes_to_string(
                                                self._service_user_secrets)
            else:
                data["service_user_secrets"] = None
        elif self.is_unlocked():
            # sign a validation string so that people can
            # check it has not been tampered with in transit
            v = self.validation_string()
            data["validation_string"] = v
            data["public_signature"] = _bytes_to_string(
                self._privcert.sign(message=v))
            data["last_signature"] = _bytes_to_string(
                self._lastcert.sign(message=v))

        return data

    @staticmethod
    def from_data(data, password=None, verify_data=False):
        """Deserialise this object from the passed data. This will
           only deserialise the private key and private certificate
           if the password is supplied.

           If 'verify_data' is True, then extract the signature of the
           data and verify that that signature is correct. You should
           always verify data that has been transmitted over a network.
        """
        service = Service()

        if data is None or len(data) == 0:
            return service

        from Acquire.Crypto import PublicKey as _PublicKey
        from Acquire.ObjectStore import string_to_datetime as \
            _string_to_datetime

        if password:
            from Acquire.Crypto import PrivateKey as _PrivateKey
            from Acquire.ObjectStore import string_to_bytes as _string_to_bytes

            # get the private info...
            try:
                service._service_user_secrets = _string_to_bytes(
                                                data["service_user_secrets"])
            except:
                service._service_user_secrets = None

            service._skeleton_key = _PrivateKey.from_data(data["skeleton_key"],
                                                          password)

            secret = service._skeleton_key.decrypt(_string_to_bytes(
                                                   data["secret_data"]))

            secret = _json.loads(secret)

            passphrase = secret["passphrase"]
            service._privkey = _PrivateKey.from_data(secret["private_key"],
                                                     passphrase)
            service._lastkey = _PrivateKey.from_data(secret["last_key"],
                                                     passphrase)
            service._privcert = _PrivateKey.from_data(
                                            secret["private_certificate"],
                                            passphrase)

            try:
                service._lastcert = _PrivateKey.from_data(
                                                secret["last_certificate"],
                                                passphrase)
            except:
                service._lastcert = service._privcert
        else:
            service._skeleton_key = None
            service._public_skeleton_key = None
            service._privkey = None
            service._privcert = None
            service._lastkey = None
            service._lastcert = _PublicKey.from_data(data["last_certificate"])
            service._service_user_secrets = None

        service._uid = data["uid"]
        service._service_type = data["service_type"]
        service._canonical_url = data["canonical_url"]
        service._domain = data["domain"]
        service._ports = data["ports"]
        service._schemes = data["schemes"]
        service._path = data["path"]

        service._service_user_uid = data["service_user_uid"]
        service._service_user_name = data["service_user_name"]

        try:
            service._public_skeleton_key = _PublicKey.from_data(
                                            data["public_skeleton_key"])
        except:
            service._public_skeleton_key = None

        service._pubkey = _PublicKey.from_data(data["public_key"])
        service._pubcert = _PublicKey.from_data(data["public_certificate"])

        service._last_key_update = _string_to_datetime(data["last_key_update"])
        service._key_update_interval = float(data["key_update_interval"])

        if service.is_identity_service():
            from Acquire.Identity import IdentityService as _IdentityService
            service = _IdentityService(service)
        elif service.is_access_service():
            from Acquire.Access import AccessService as _AccessService
            service = _AccessService(service)
        elif service.is_compute_service():
            from Acquire.Compute import ComputeService as _ComputeService
            service = _ComputeService(service)
        elif service.is_storage_service():
            from Acquire.Storage import StorageService as _StorageService
            service = _StorageService(service)
        elif service.is_registry_service():
            from Acquire.Registry import RegistryService as _RegistryService
            service = _RegistryService(service)
        elif service.is_accounting_service():
            from Acquire.Accounting import AccountingService \
                                        as _AccountingService
            service = _AccountingService(service)

        if verify_data:
            # the service was transmitted with a signature from both
            # certificates - make sure that the signature was correct.
            # This stops someone changing the certificates, keys or
            # any other data about the service while in transit
            try:
                from Acquire.ObjectStore import string_to_bytes \
                    as _string_to_bytes

                vr = data["validation_string"]
                v = service.validation_string()
                assert(v == vr)

                sig = _string_to_bytes(data["public_signature"])
                service._pubcert.verify(signature=sig, message=v)
                sig = _string_to_bytes(data["last_signature"])
                service._lastcert.verify(signature=sig, message=v)
            except Exception as e:
                from Acquire.Crypto import SignatureVerificationError
                raise SignatureVerificationError(
                    "Cannot verify that the returned data for service '%s' "
                    "has not been tampered with - the signature is "
                    "incorrect: Error = %s" %
                    (service.canonical_url(), str(e)))

            service._verified_data = True

        return service
