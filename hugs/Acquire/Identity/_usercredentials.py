
__all__ = ["UserCredentials"]

_user_root = "identity/users"


class UserCredentials:
    """This class is used to store user credentials in the object
       store, and to verify that user credentials are correct.

       The user credentials are used to ultimately store a
       primary password for the user, which unlocks the user's
       primary private key
    """
    @staticmethod
    def hash(username, password, service_uid=None):
        """Return a secure hash of the passed username and password"""
        from Acquire.Crypto import Hash as _Hash
        from Acquire.Service import get_this_service as _get_this_service

        if service_uid is None:
            service_uid = _get_this_service(need_private_access=False).uid()

        result = _Hash.multi_md5(service_uid, username+password)

        return result

    @staticmethod
    def create(user_uid, password, primary_password,
               device_uid=None):
        """Create the credentials for the user with specified
           user_uid, optionally logging in via the specified
           device_uid, using the specified password, to protect
           the passed "primary_password"

           This returns the OTP that has been created to be
           associated with these credentials
        """
        from Acquire.Crypto import PrivateKey as _PrivateKey
        from Acquire.Crypto import OTP as _OTP
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket
        from Acquire.ObjectStore import bytes_to_string as _bytes_to_string

        if device_uid is None:
            device_uid = user_uid

        privkey = _PrivateKey(auto_generate=True)
        otp = _OTP()
        otpsecret = otp.encrypt(privkey.public_key())
        primary_password = privkey.encrypt(primary_password)

        data = {"primary_password": _bytes_to_string(primary_password),
                "private_key": privkey.to_data(passphrase=password),
                "otpsecret": _bytes_to_string(otpsecret)
                }

        key = "%s/credentials/%s/%s" % (_user_root, user_uid, device_uid)

        bucket = _get_service_account_bucket()
        _ObjectStore.set_object_from_json(bucket=bucket,
                                          key=key,
                                          data=data)

        return otp

    @staticmethod
    def login(credentials, user_uids, remember_device=False):
        """Verify the passed credentials are correct.
           This will find the account
           that matches these credentials. If one does, then this
           will validate the credentials are correct, and then return
           a tuple of the (user_uid, primary_password) for that
           user
        """
        from Acquire.Crypto import PrivateKey as _PrivateKey
        from Acquire.Crypto import OTP as _OTP
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket
        from Acquire.ObjectStore import string_to_bytes as _string_to_bytes
        from Acquire.Client import Credentials as _Credentials
        from Acquire.Crypto import RepeatedOTPCodeError \
            as _RepeatedOTPCodeError

        if not isinstance(credentials, _Credentials):
            raise TypeError("The passed credentials must be type Credentials")

        username = credentials.username()
        short_uid = credentials.short_uid()
        device_uid = credentials.device_uid()
        password = credentials.password()
        otpcode = credentials.otpcode()

        bucket = _get_service_account_bucket()

        # now try to find a matching user_uid
        for user_uid in user_uids:
            data = None

            try:
                key = "%s/credentials/%s/%s" % (_user_root, user_uid,
                                                device_uid)
                data = _ObjectStore.get_object_from_json(bucket=bucket,
                                                         key=key)
                verified_device_uid = device_uid
            except:
                pass

            if data is None:
                # unknown device_uid. Try using the user_uid
                key = "%s/credentials/%s/%s" % (_user_root, user_uid,
                                                user_uid)
                try:
                    data = _ObjectStore.get_object_from_json(bucket=bucket,
                                                             key=key)
                    verified_device_uid = None
                except:
                    pass

            if data is not None:
                # verify the credentials
                try:
                    return UserCredentials.validate_password(
                                            user_uid=user_uid,
                                            device_uid=verified_device_uid,
                                            secrets=data,
                                            password=password,
                                            otpcode=otpcode,
                                            remember_device=remember_device)
                except _RepeatedOTPCodeError as e:
                    # if the OTP code is entered twice, then we need
                    # to invalidate the other session
                    raise e
                except:
                    # this is not the matching user...
                    pass

        # only get here if there are no matching users (or the
        # user-supplied password etc. are wrong)
        from Acquire.Identity import UserValidationError
        raise UserValidationError(
            "Invalid credentials logging into session '%s' "
            "with username '%s'" % (short_uid, username))

    @staticmethod
    def validate_password(user_uid, device_uid, secrets, password,
                          otpcode, remember_device):
        """Validate that the passed password and one-time-code are valid.
           If they are, then return a tuple of the UserAccount of the unlocked
           user, the OTP that is used to generate secrets, and the
           device_uid of the login device

           If 'remember_device' is True and 'device_uid' is None, then
           this creates a new OTP for the login device, which is returned,
           and a new device_uid for that device. The password needed to
           match this device is a MD5 of the normal user password.
        """
        from Acquire.Crypto import PrivateKey as _PrivateKey
        from Acquire.Crypto import OTP as _OTP
        from Acquire.ObjectStore import string_to_bytes as _string_to_bytes

        privkey = _PrivateKey.from_data(data=secrets["private_key"],
                                        passphrase=password)

        # decrypt and validate the OTP code
        data = _string_to_bytes(secrets["otpsecret"])

        otpsecret = privkey.decrypt(data)
        otp = _OTP(secret=otpsecret)
        otp.verify(code=otpcode, once_only=True)

        # everything is ok - we can load the user account via the
        # decrypted primary password
        primary_password = _string_to_bytes(secrets["primary_password"])
        primary_password = privkey.decrypt(primary_password)

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import get_service_account_bucket \
            as _get_service_account_bucket

        data = None
        secrets = None
        key = "%s/uids/%s" % (_user_root, user_uid)

        bucket = _get_service_account_bucket()

        try:
            data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)
        except:
            pass

        if data is None:
            from Acquire.Identity import UserValidationError
            raise UserValidationError(
                "Unable to validate user as no account data is present!")

        from Acquire.Identity import UserAccount as _UserAccount
        user = _UserAccount.from_data(data=data, passphrase=primary_password)

        if user.uid() != user_uid:
            from Acquire.Identity import UserValidationError
            raise UserValidationError(
                "Unable to validate user as mismatch in user_uids!")

        if device_uid is None and remember_device:
            # create a new OTP that is unique for this device
            from Acquire.ObjectStore import create_uuid as _create_uuid
            from Acquire.Client import Credentials as _Credentials
            device_uid = _create_uuid()
            device_password = _Credentials.encode_device_uid(
                                                encoded_password=password,
                                                device_uid=device_uid)

            otp = UserCredentials.create(user_uid=user_uid,
                                         password=device_password,
                                         primary_password=primary_password,
                                         device_uid=device_uid)

        return {"user": user, "otp": otp, "device_uid": device_uid}
