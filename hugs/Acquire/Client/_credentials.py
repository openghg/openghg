
__all__ = ["Credentials"]


class Credentials:
    """This class holds user credentials that are sent to the
       service to authenticate users
    """
    def __init__(self, username=None,
                 short_uid=None, device_uid=None,
                 password=None, otpcode=None):
        """Construct to contain the passed credentials"""
        self._username = username

        if username is not None:
            self._short_uid = short_uid
            self._device_uid = device_uid
            self._password = password
            self._otpcode = otpcode

    def is_null(self):
        """Return whether or not these credentials are null

           Returns:
                bool: True if null, else False
        """
        return self._username is None

    def username(self):
        """Return the decoded username

           Returns:
                str: Username held for these credentials

        """
        if self.is_null():
            return None
        else:
            return self._username

    def short_uid(self):
        """Return the decoded session short UID

           Returns:
                str: Short UID of session
        """
        if self.is_null():
            return None
        else:
            return self._short_uid

    def device_uid(self):
        """Return the decoded device UID

           Returns:
                str: UID for device
        """
        if self.is_null():
            return None
        else:
            return self._device_uid

    def password(self):
        """Return the decoded password

           Returns:
                str: Decoded password
        """
        if self.is_null():
            return None
        else:
            return self._password

    def otpcode(self):
        """Return the decoded one time password code (otpcode)

           Returns:
                str: OTP code for session
        """
        if self.is_null():
            return None
        else:
            return self._otpcode

    def to_data(self, identity_uid):
        """Package these credentials into a secure package that
           can be encoded to json and sent to the service.
           Note that you must supply the UID of the identity
           service that you will send this package to...

           Args:
                identity_uid (str): UID of identity service
           Returns:
                str: String containing credential data
        """
        if self.is_null():
            return None

        return Credentials.package(identity_uid=identity_uid,
                                   short_uid=self._short_uid,
                                   username=self._username,
                                   password=self._password,
                                   otpcode=self._otpcode,
                                   device_uid=self._device_uid)

    @staticmethod
    def from_data(data, username, short_uid, random_sleep=150):
        """Unpackage the passed data that has been deserialised from
           json and return the credentials. You need to pass in
           the username and short_uid that you expect to see.
           The random_sleep adds a random sleep to disrupt
           timing attacks

           Args:
                data (str): Data to create credentials from
                username (str): Username for credentials
                short_uid (str): Short UID to use
                random_sleep (int, default=150): Integer used
                to generate a random sleep time
           Returns:
                Credentials: Credentials object created from data
        """
        result = Credentials.unpackage(data=data, username=username,
                                       short_uid=short_uid,
                                       random_sleep=random_sleep)

        return Credentials(username=result["username"],
                           short_uid=result["short_uid"],
                           device_uid=result["device_uid"],
                           password=result["password"],
                           otpcode=result["otpcode"])

    def assert_matching_username(self, username):
        """Assert that the passed username matches that stored
           in these credentials

           Args:
                username (str): Username to compare
           Returns:
                None
        """
        if self.is_null() or self._username != username:
            raise PermissionError(
                "Disagreement for the username for the matched "
                "credentials")

    @staticmethod
    def encode_device_uid(encoded_password, device_uid):
        """Simple function that takes an existing encoded password,
           and then additionally encodes this using the device_uid

           Args:
                encoded_password (str): Encoded password
                device_uid (str): UID for device
           Returns:
                str: Password encoded with device UID

        """
        if device_uid is None or encoded_password is None:
            return encoded_password

        from Acquire.Crypto import Hash as _Hash
        result = _Hash.md5(encoded_password + device_uid)
        return result

    @staticmethod
    def encode_password(password, identity_uid, device_uid=None):
        """Simple function that creates an MD5 hash of the password,
           salted using the passed identity_uid and (optionally)
           the device_uid

           Args:
                password (str): Password to hash
                identity_uid (str): UID to use as salt
                device_uid (str, default=None): Device UID to use
                as additional salt
           Returns:
                str: Hashed and salted password
        """
        from Acquire.Crypto import Hash as _Hash

        encoded_password = _Hash.multi_md5(identity_uid, password)

        encoded_password = Credentials.encode_device_uid(
                                        encoded_password=encoded_password,
                                        device_uid=device_uid)

        return encoded_password

    @staticmethod
    def package(identity_uid, short_uid, username, password, otpcode,
                device_uid=None):
        """Package up the passed credentials so that they can be sent
           to a server for verification. We employ the following
           steps to make it harder for someone to steal the user's
           password:

            1. An MD5 of the password ("password") is generated, salted with
               the UID of the identity service ("identity_uid"), and,
               optionally, the UID of this device ("device_uid")

            2. A symmetric key is generated from the combined MD5s
               of the user's login name (username) and the short UID of
               this login session (short_uid). This is used to encrypt
               the MD5's password and one-time password code ("otpcode").
               The username and session UID are not sent to the server,
               so an attacker must know what these are to extract
               this information.

            3. Also remember that all communication with a service is
               encrypted using the service's public key, and tranmission
               of data should also be sent over HTTPS.

           Args:
                identity_uid (str): UID of the identity service
                short_uid (str): UID of the login session
                username (str): Username for this session
                password (str): Password for user
                otpcode (str): OTP code for session
                device_uid (str): UID for device
           Returns:
                str: JSON serialisable string
        """
        from Acquire.Crypto import Hash as _Hash
        from Acquire.Crypto import SymmetricKey as _SymmetricKey
        from Acquire.ObjectStore import bytes_to_string as _bytes_to_string

        if username is None or password is None or otpcode is None:
            raise PermissionError(
                "You must supply a username, password and otpcode "
                "to be able to log in!")

        encoded_password = Credentials.encode_password(
                                            identity_uid=identity_uid,
                                            device_uid=device_uid,
                                            password=password)

        # if the device_uid is not set, then create a random one
        # so that an attacker does not know...
        if device_uid is None:
            from Acquire.ObjectStore import create_uuid as _create_uuid
            device_uid = _create_uuid()

        data = [encoded_password, device_uid, otpcode]
        string_data = "|".join(data)

        uname_shortid = _Hash.md5(username) + _Hash.md5(short_uid)

        data = _SymmetricKey(symmetric_key=uname_shortid).encrypt(string_data)
        result = _bytes_to_string(data)
        return result

    @staticmethod
    def unpackage(data, username, short_uid, random_sleep=150):
        """Unpackage the credentials data packaged using "package" above,
           assuming that this data was packaged for the user login
           name "username" and for the session with short UID "short_uid".

           This will return a dictionary containing:

           username: Login name of the user
           short_uid: Short UID of the login session
           device_uid: UID of the login device (this will be random if it
                       was not set by the user)
           password: The MD5 of the password, salted using the UID of the
                     identity service, and optionally the device_uid
           otpcode: The one-time-password code for this login

           To make timing-based attacks harder, you can set 'random_sleep'
           to add an additional random sleep of up to 'random_sleep'
           milliseconds onto the end of the unpackage function

           Args:
                data (str): String of data containing credentials
                username (str): Username for session
                short_uid (str): UID for session
                random_sleep (int, default=150): Integer used to
                generate a random sleep to prevent timing attacks
           Returns:
                dict: Dictionary containing credentials

        """
        from Acquire.Crypto import Hash as _Hash
        from Acquire.Crypto import SymmetricKey as _SymmetricKey
        from Acquire.ObjectStore import string_to_bytes as _string_to_bytes
        from Acquire.ObjectStore import bytes_to_string as _bytes_to_string

        uname_shortid = _Hash.md5(username) + _Hash.md5(short_uid)

        data = _string_to_bytes(data)

        try:
            data = _SymmetricKey(symmetric_key=uname_shortid).decrypt(data)
        except:
            data = None

        if data is None:
            raise PermissionError("Cannot unpackage/decrypt the credentials")

        data = data.split("|")

        if len(data) < 3:
            raise PermissionError("Invalid credentials! %s" % data)

        result = {"username": username,
                  "short_uid": short_uid,
                  "device_uid": data[1],
                  "password": data[0],
                  "otpcode": data[2]}

        if random_sleep is not None:
            import random as _random
            import time as _time
            random_sleep = _random.randint(0, random_sleep)
            _time.sleep(0.001 * random_sleep)

        return result
