
__all__ = ["Service"]


class Service:
    """This class represents a service in the system. Services
       will either be identity services, access services,
       storage services or accounting services.

       This class provides a client-side wrapper to fetch or set-up
       a service. This is a metamorphic (shape-shifting) class,
       i.e. during construction it will transform into the class
       of the type of service, e.g. Acquire.Identity.IdentityService
    """
    def __init__(self, service_url=None, service_uid=None,
                 service_type=None):
        """Construct the service that is accessed at the remote
           URL 'service_url'. This will fetch and return the
           details of the remote service. This wrapper is a
           chameleon class, and will transform into the
           class type of the fetched service, e.g.

            service = Acquire.Client.Service("https://identity_service_url")
            service.__class__ == Acquire.Identity.IdentityService

            Args:
                service_url (str): URL of service
                service_uid (str): UID of service
        """
        try:
            from Acquire.Client import Wallet as _Wallet
            service = _Wallet().get_service(service_url=service_url,
                                            service_uid=service_uid,
                                            service_type=service_type)

            from copy import copy as _copy
            self.__dict__ = _copy(service.__dict__)
            self.__class__ = service.__class__
        except Exception as e:
            self._failed = True
            raise e

    def _fail(self):
        """This is called by all functions as this Service
           has failed to be initialised
        """
        if self._failed:
            from Acquire.Service import ServiceError
            raise ServiceError(
                "Cannot do anything with a null service")

    def __str__(self):
        return "Service(failed setup!)"

    def uuid(self):
        """Synonym for uid"""
        return self.uid()

    def uid(self):
        """Return the uuid of this service. This MUST NEVER change, as
           the UID uniquely identifies this service to all other
           services
        """
        self._fail()
        return None  # pylint: disable=W1111

    def service_type(self):
        """Return the type of this service"""
        self._fail()
        return None

    def is_locked(self):
        """Return whether or not this service object is locked. Locked
           service objects don't contain copies of any private keys,
           and can be safely shared as a means of distributing public
           keys and certificates
        """
        self._fail()
        return None

    def is_unlocked(self):
        """Return whether or not this service object is unlocked. Unlocked
           service objects have access to the skeleton key and other private
           keys. They should only run on the service. Locked service objects
           are what are returned by services to provide public keys and
           public certificates
        """
        self._fail()
        return None

    def get_trusted_service(self, service_url=None, service_uid=None):
        """Return the trusted service info for the service with specified
           service_url or service_uid"""
        self._fail()
        return None

    def assert_unlocked(self):
        """Assert that this service object is unlocked"""
        self._fail()
        return None

    def assert_admin_authorised(self, authorisation, resource=None):
        """Validate that the passed authorisation is valid for the
           (optionally) specified resource, and that this has been
           authorised by one of the admin accounts of this service
        """
        self._fail()
        return None

    def last_key_update(self):
        """Return the datetime when the key and certificate of this
           service were last updated
        """
        self._fail()
        return None

    def key_update_interval(self):
        """Return the time delta between server key updates"""
        self._fail()
        return None

    def should_refresh_keys(self):
        """Return whether the keys and certificates need to be refreshed
           - i.e. more than 'key_update_interval' has passed since the last
           key update
        """
        self._fail()
        return None

    def refresh_keys(self):
        """Refresh the keys and certificates"""
        self._fail()
        return None

    def can_identify_users(self):
        """Return whether or not this service can identify users.
           Most services can, at a minimum, identify their admin
           users. However, only true Identity Services can register
           and manage normal users
        """
        self._fail()
        return None

    def is_identity_service(self):
        """Return whether or not this is an identity service"""
        self._fail()
        return None

    def is_access_service(self):
        """Return whether or not this is an access service"""
        self._fail()
        return None

    def is_accounting_service(self):
        """Return whether or not this is an accounting service"""
        self._fail()
        return None

    def is_storage_service(self):
        """Return whether or not this is a storage service"""
        self._fail()
        return None

    def service_url(self):
        """Return the URL used to access this service"""
        self._fail()
        return None

    def canonical_url(self):
        """Return the canonical URL for this service (this is the URL the
           service thinks it has, and which it has used to register itself
           with all other services)
        """
        self._fail()
        return None

    def hostname(self):
        """Return the hostname of the canonical URL that provides
           this service
        """
        self._fail()
        return None

    def uses_https(self):
        """Return whether or not the canonical URL of this service
           is connected to via https
        """
        self._fail()
        return None

    def service_user_uid(self):
        """Return the UID of the service user account for this service"""
        self._fail()
        return None

    def service_user_name(self):
        """Return the name of the service user account for this service"""
        self._fail()
        return None

    def service_user_secrets(self):
        """Return the (encrypted) secrets for the service user account.
           These will only be returned if you have unlocked this service.
           You need access to the skeleton key to decrypt these secrets
        """
        self._fail()
        return None

    def login_service_user(self):
        """Return a logged in Acquire.Client.User for the service user.
           This can only be called inside the service, and when you
           have unlocked this service object
        """
        self._fail()
        return None

    def service_user_account_uid(self, accounting_service_url=None,
                                 accounting_service=None):
        """Return the UID of the financial account associated with
           this service on the passed accounting service
        """
        self._fail()
        return None

    def skeleton_key(self):
        """Return the skeleton key used by this service. This is an
           unchanging key which is stored internally, should never be
           shared outside the service, and which is used to encrypt
           all data. Unlocking the service involves loading and
           decrypting this skeleton key
        """
        self._fail()
        return None

    def private_key(self):
        """Return the private key (if it has been unlocked)"""
        self._fail()
        return None

    def private_certificate(self):
        """Return the private signing certificate (if it has been unlocked)"""
        self._fail()
        return None

    def public_key(self):
        """Return the public key for this service"""
        self._fail()
        return None

    def public_certificate(self):
        """Return the public signing certificate for this service"""
        self._fail()
        return None

    def last_key(self):
        """Return the old private key for this service (if it has
           been unlocked). This was the key used before the last
           key update, and we store it in case we have to decrypt
           data that was recently encrypted using the old public key
        """
        self._fail()
        return None

    def last_certificate(self):
        """Return the old public certificate for this service. This was the
           certificate used before the last key update, and we store it
           in case we need to verify data signed using the old private
           certificate
        """
        self._fail()
        return None

    def call_function(self, function, args=None):
        """Call the function 'func' on this service, optionally passing
           in the arguments 'args'. This is a simple wrapper around
           Acquire.Service.call_function which automatically
           gets the correct URL, encrypts the arguments using the
           service's public key, and supplies a key to encrypt
           the response (and automatically then decrypts the
           response)
        """
        self._fail()
        return {}

    def sign(self, message):
        """Sign the specified message"""
        self._fail()
        return None

    def verify(self, signature, message):
        """Verify that this service signed the message"""
        self._fail()
        return None

    def encrypt(self, message):
        """Encrypt the passed message"""
        self._fail()
        return None

    def decrypt(self, message):
        """Decrypt the passed message"""
        self._fail()
        return None

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
        self._fail()
        return None

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
        self._fail()
        return None

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
        self._fail()
        return None

    def decrypt_data(self, data):
        """Decrypt the passed data that has been encrypted and sent to
           this service (encrypted via the 'encrypt_data' function).
           This will return a json-deserialisable dictionary. Note that
           the 'service_uid' should match the UID of this
           service. The data should also contain the fingerprint of the
           key used to encrypt the data, enabling the service to
           perform key rotation and management.
        """
        self._fail()
        return None

    def dump_keys(self):
        """Return a dump of the current key and certificate, so that
           we can keep a record of all keys that have been used. The
           returned json-serialisable dictionary contains the keys,
           their fingerprints, and the datetime when they were
           generated. If this is run on the service, then the keys
           are encrypted the password which is encrypted using the
           master key
        """
        self._fail()
        return None

    def load_keys(self, data):
        """Return the keys that were dumped by 'self.dump_keys()'.
           This returns a dictionary of the keys and datetime that
           they were created, indexed by their key fingerprints
        """
        self._fail()
        return None

    def get_session_info(self, session_uid,
                         scope=None, permissions=None):
        """Return information about the passed session,
           optionally limited to the provided scope and permissions
        """
        self._fail()
        return None

    def to_data(self, password=None):
        """Serialise this key to a dictionary, using the supplied
           password to encrypt the private key and certificate"""
        self._fail()
        return None

    @staticmethod
    def from_data(data, password=None):
        """Deserialise this object from the passed data. This will
           only deserialise the private key and private certificate
           if the password is supplied
        """
        from Acquire.Service import Service as _Service
        return _Service.from_data(data, password)
