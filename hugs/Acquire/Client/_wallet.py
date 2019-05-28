
# use a variable so we can monkey-patch while testing
_input = input

__all__ = ["Wallet"]


def _get_wallet_dir():
    """Function that can be mocked in testing to ensure we don't
       mess with the user's main wallet
    """
    import os as _os
    home = _os.path.expanduser("~")
    return "%s/.acquire_wallet" % home


def _get_wallet_password(confirm_password=False):
    """Function that can be mocked in testing to ensure we don't
       mess with the user's main wallet
    """
    import getpass as _getpass
    password = _getpass.getpass(
                prompt="Please enter a password to encrypt your wallet: ")

    if not confirm_password:
        return password

    password2 = _getpass.getpass(
                    prompt="Please confirm the password: ")

    if password != password2:
        _output("The passwords don't match. Please try again.")
        return _get_wallet_password(confirm_password=confirm_password)

    return password


def _output(s, end=None):
    """Simple output function that can be replaced at testing
       to silence the wallet
    """
    if end is None:
        print(s)
    else:
        print(s, end=end)


def _flush_output():
    """Flush STDOUT"""
    try:
        import sys as _sys
        _sys.stdout.flush()
    except:
        pass


def _read_json(filename):
    """Return a json-decoded dictionary from the data written
       to 'filename'
    """
    import json as _json
    with open(filename, "rb") as FILE:
        s = FILE.read().decode("utf-8")

        return _json.loads(s)


def _write_json(data, filename):
    """Write the passed json-encodable dictionary to 'filename'"""
    import json as _json
    s = _json.dumps(data)
    with open(filename, "wb") as FILE:
        FILE.write(s.encode("utf-8"))


def _read_service(filename):
    """Read and return the service written to 'filename'"""
    from Acquire.Client import Service as _Service
    return _Service.from_data(_read_json(filename))


def _write_service(service, filename):
    """Write the passed service to 'filename'"""
    _write_json(service.to_data(), filename)


def _could_match(userinfo, username, password):
    if username is None:
        return True

    if "username" not in userinfo:
        return False

    if userinfo["username"] == username:
        if password is None:
            return True

        if "password" in userinfo:
            if userinfo["password"] == password:
                return True

    return False


class Wallet:
    """This class holds a wallet that can be used to simplify
       sending passwords and one-time-password (OTP) codes
       to an acquire identity service.

       This holds a wallet of passwords and (optionally)
       OTP secrets that are encrypted using a local keypair
       that is unlocked by a password supplied by the user locally.

       By default this will create the wallet in your home
       directory ($HOME/.acquire_wallet). If you want the wallet
       to be saved in a different directory, specify that
       as "wallet_dir".
    """
    def __init__(self):
        """Construct a wallet to hold all user credentials"""
        from Acquire.Service import is_running_service \
            as _is_running_service

        if _is_running_service():
            from Acquire.Service import get_this_service \
                as _get_this_service
            service = _get_this_service(need_private_access=False)
            raise PermissionError(
                "You cannot open a Wallet on a running Service (%s)" %
                service)

        self._wallet_key = None
        self._service_info = {}

        import os as _os

        wallet_dir = _get_wallet_dir()

        if not _os.path.exists(wallet_dir):
            _os.makedirs(wallet_dir, mode=0o700, exist_ok=False)
        elif not _os.path.isdir(wallet_dir):
            raise TypeError("The wallet directory must be a directory!")

        self._wallet_dir = wallet_dir

    def _create_wallet_key(self, filename):
        """Create a new wallet key for the user"""
        password = _get_wallet_password(confirm_password=True)

        from Acquire.Client import PrivateKey as _PrivateKey
        key = _PrivateKey()

        bytes = key.bytes(password)

        with open(filename, "wb") as FILE:
            FILE.write(bytes)

        return key

    def _get_wallet_key(self):
        """Return the private key used to encrypt everything in the wallet.
           This will ask for the users password
        """
        if self._wallet_key:
            return self._wallet_key

        wallet_dir = self._wallet_dir

        keyfile = "%s/wallet_key.pem" % wallet_dir

        import os as _os

        if not _os.path.exists(keyfile):
            self._wallet_key = self._create_wallet_key(filename=keyfile)
            return self._wallet_key

        # read the keyfile and decrypt
        with open(keyfile, "rb") as FILE:
            bytes = FILE.read()

        wallet_key = None

        from Acquire.Client import PrivateKey as _PrivateKey

        # get the user password
        for _ in range(0, 5):
            password = _get_wallet_password()

            try:
                wallet_key = _PrivateKey.read_bytes(bytes, password)
            except:
                _output("Invalid password. Please try again.")

            if wallet_key:
                break

        if wallet_key is None:
            raise PermissionError("Too many failed password attempts...")

        self._wallet_key = wallet_key
        return wallet_key

    def _get_userinfo_filename(self, user_uid, identity_uid):
        """Return the filename for the passed user_uid logging into the
           passed identity service with identity_uid
        """
        assert(user_uid is not None)
        assert(identity_uid is not None)

        return "%s/user_%s_%s_encrypted" % (
            self._wallet_dir, user_uid, identity_uid)

    def _set_userinfo(self, userinfo, user_uid, identity_uid):
        """Save the userfile for the passed user_uid logging into the
           passed identity service with identity_uid
        """
        from Acquire.ObjectStore import bytes_to_string as _bytes_to_string
        import json as _json
        filename = self._get_userinfo_filename(user_uid=user_uid,
                                               identity_uid=identity_uid)
        key = self._get_wallet_key().public_key()
        data = _bytes_to_string(key.encrypt(_json.dumps(userinfo)))

        userinfo = {"username": userinfo["username"],
                    "user_uid": user_uid,
                    "data": data}

        _write_json(data=userinfo, filename=filename)

    def _unlock_userinfo(self, userinfo):
        """Function used to unlock (decrypt) the passed userinfo"""
        from Acquire.ObjectStore import string_to_bytes as _string_to_bytes
        import json as _json

        key = self._get_wallet_key()

        data = _string_to_bytes(userinfo["data"])
        result = _json.loads(key.decrypt(data))
        result["user_uid"] = userinfo["user_uid"]
        return result

    def _get_userinfo(self, user_uid, identity_uid):
        """Read all info for the passed user at the identity service
           reached at 'identity_url'
        """
        filename = self._get_userinfo_filename(user_uid=user_uid,
                                               identity_uid=identity_uid)
        userinfo = _read_json(filename=filename)
        return self._unlock_userinfo(userinfo)

    def _find_userinfo(self, username=None, password=None):
        """Function to find a user_info automatically, of if that fails,
           to ask the user
        """
        wallet_dir = self._wallet_dir

        import glob as _glob

        userfiles = _glob.glob("%s/user_*_encrypted" % wallet_dir)

        userinfos = []

        for userfile in userfiles:
            try:
                userinfo = _read_json(userfile)
                if _could_match(userinfo, username, password):
                    userinfos.append((userinfo["username"], userinfo))
            except:
                pass

        userinfos.sort(key=lambda x: x[0])

        if len(userinfos) == 1:
            return self._unlock_userinfo(userinfos[0][1])

        if len(userinfos) == 0:
            if username is None:
                username = _input("Please type your username: ")

            userinfo = {"username": username}

            if password is not None:
                userinfo["password"] = password

            return userinfo

        _output("Please choose the account by typing in its number, "
                "or type a new username if you want a different account.")

        for (i, (username, userinfo)) in enumerate(userinfos):
            _output("[%d] %s {%s}" % (i+1, username, userinfo["user_uid"]))

        max_tries = 5

        for i in range(0, max_tries):
            reply = _input(
                    "\nMake your selection (1 to %d) " %
                    (len(userinfos))
                )

            try:
                idx = int(reply) - 1
            except:
                idx = None

            if idx is None:
                # interpret this as a username
                return self._find_userinfo(username=reply, password=password)
            elif idx < 0 or idx >= len(userinfos):
                _output("Invalid account.")
            else:
                return self._unlock_userinfo(userinfos[idx][1])

            if i < max_tries-1:
                _output("Try again...")

        userinfo = {}

        if username is not None:
            userinfo["username"] = username

        return userinfo

    def _get_user_password(self, userinfo):
        """Get the user password for the passed user on the passed
           identity_url
        """
        if "password" in userinfo:
            return userinfo["password"]
        else:
            import getpass as _getpass
            password = _getpass.getpass(
                            prompt="Please enter the login password: ")
            userinfo["password"] = password
            return password

    def _get_otpcode(self, userinfo):
        """Get the OTP code"""
        if "otpsecret" in userinfo:
            from Acquire.Client import OTP as _OTP
            otp = _OTP(userinfo["otpsecret"])
            return otp.generate()
        else:
            import getpass as _getpass
            return _getpass.getpass(
                        prompt="Please enter the one-time-password code: ")

    def get_services(self):
        """Return all of the trusted services known to this wallet"""
        import glob as _glob
        service_files = _glob.glob("%s/service_*" % self._wallet_dir)

        services = []

        for service_file in service_files:
            services.append(_read_service(service_file))

        return services

    def get_service(self, service_url=None, service_uid=None,
                    service_type=None, autofetch=True):
        """Return the service at either 'service_url', or that
           has UID 'service_uid'. This will return the
           cached service if it exists, or will add a new service if
           we are able to validate it from a trusted registry
        """
        from Acquire.ObjectStore import string_to_safestring \
            as _string_to_safestring

        service = None

        if service_url is None:
            if service_uid is None:
                raise PermissionError(
                    "You need to specify one of service_uid or service_url")

            # we need to look up the name...
            import glob as _glob
            service_files = _glob.glob("%s/service_*" % self._wallet_dir)

            for service_file in service_files:
                s = _read_service(service_file)
                if s.uid() == service_uid:
                    service = s
                    break
        else:
            from Acquire.Service import Service as _Service
            service_url = _Service.get_canonical_url(service_url,
                                                     service_type=service_type)

            service_file = "%s/service_%s" % (
                self._wallet_dir,
                _string_to_safestring(service_url))

            try:
                service = _read_service(service_file)
            except:
                pass

        must_write = False

        if service is None:
            if not autofetch:
                from Acquire.Service import ServiceError
                raise ServiceError("No service at %s:%s" %
                                   (service_url, service_uid))

            # we need to look this service up from the registry
            from Acquire.Registry import get_trusted_registry_service \
                as _get_trusted_registry_service

            _output("Connecting to registry...")
            _flush_output()

            registry = _get_trusted_registry_service(service_uid=service_uid,
                                                     service_url=service_url)

            _output("...connected to registry %s" % registry)
            _flush_output()

            if service_url is not None:
                _output("Securely fetching keys for %s..." % service_url)
                _flush_output()
            else:
                _output("Securely fetching keys for UID %s..." % service_uid)
                _flush_output()

            service = registry.get_service(service_url=service_url,
                                           service_uid=service_uid)

            _output("...success.\nFetched %s" % service)
            _flush_output()

            must_write = True

        # check if the keys need rotating - if they do, load up
        # the new keys and save them to the service file...
        if service.should_refresh_keys():
            service.refresh_keys()
            must_write = True

        if service_uid is not None:
            if service.uid() != service_uid:
                raise PermissionError(
                    "Disagreement over the service UID for '%s' (%s)" %
                    (service, service_uid))

        if must_write:
            service_file = "%s/service_%s" % (
                self._wallet_dir,
                _string_to_safestring(service.canonical_url()))
            _write_service(service=service, filename=service_file)

        return service

    def remove_all_services(self):
        """Remove all trusted services from this Wallet"""
        import glob as _glob
        import os as _os
        service_files = _glob.glob("%s/service_*" % self._wallet_dir)

        for service_file in service_files:
            if _os.path.exists(service_file):
                _os.unlink(service_file)

    def remove_service(self, service):
        """Remove the cached service info for the passed service"""
        if isinstance(service, str):
            service_url = service
        else:
            service_url = service.canonical_url()

        from Acquire.ObjectStore import string_to_safestring \
            as _string_to_safestring

        service_file = "%s/service_%s" % (
            self._wallet_dir,
            _string_to_safestring(service_url))

        import os as _os

        if _os.path.exists(service_file):
            _os.unlink(service_file)

    def send_password(self, url, username=None, password=None,
                      otpcode=None, remember_password=True,
                      remember_device=None, dryrun=None):
        """Send a password and one-time code to the supplied login url"""
        if not remember_password:
            remember_device = False

        # the login URL is http[s]://something.com?id=XXXX/YY.YY.YY.YY
        # where XXXX is the service_uid of the service we should
        # connect with, and YY.YY.YY.YY is the short_uid of the login
        try:
            from urllib.parse import urlparse as _urlparse
            from urllib.parse import parse_qs as _parse_qs
            idcode = _parse_qs(_urlparse(url).query)["id"][0]
        except Exception as e:
            from Acquire.Client import LoginError
            raise LoginError(
                "Cannot identify the session or service information from "
                "the login URL '%s'. This should have id=XX-XX/YY.YY.YY.YY "
                "as a query parameter. <%s> %s" %
                (url, e.__class__.__name__, str(e)))

        try:
            (service_uid, short_uid) = idcode.split("/")
        except:
            from Acquire.Client import LoginError
            raise LoginError(
                "Cannot extract the service_uid and short_uid from the "
                "login ID code '%s'. This should be in the format "
                "XX-XX/YY.YY.YY.YY" % idcode)

        # now get the service
        try:
            service = self.get_service(service_uid=service_uid)
        except Exception as e:
            from Acquire.Client import LoginError
            raise LoginError(
                "Cannot find the service with UID %s: <%s> %s" %
                (service_uid, e.__class__.__name__, str(e)))

        if not service.can_identify_users():
            from Acquire.Client import LoginError
            raise LoginError(
                "Service '%s' is unable to identify users! "
                "You cannot log into something that is not "
                "a valid identity service!" % (service))

        userinfo = self._find_userinfo(username=username,
                                       password=password)

        if username is None:
            username = userinfo["username"]

        if "user_uid" in userinfo:
            user_uid = userinfo["user_uid"]
        else:
            user_uid = None

        _output("Logging in using username '%s'" % username)

        try:
            device_uid = userinfo["device_uid"]
        except:
            device_uid = None

        if password is None:
            password = self._get_user_password(userinfo=userinfo)

        if otpcode is None:
            otpcode = self._get_otpcode(userinfo=userinfo)
        else:
            # user is providing the primary OTP, so this is not a device
            device_uid = None

        _output("\nLogging in to '%s', session '%s'..." % (
                service.canonical_url(), short_uid), end="")

        _flush_output()

        if dryrun:
            print("Calling %s with username=%s, password=%s, otpcode=%s, "
                  "remember_device=%s, device_uid=%s, short_uid=%s "
                  "user_uid=%s" %
                  (service.canonical_url(), username, password, otpcode,
                   remember_device, device_uid, short_uid, user_uid))
            return

        try:
            from Acquire.Client import Credentials as _Credentials

            creds = _Credentials(username=username, password=password,
                                 otpcode=otpcode, short_uid=short_uid,
                                 device_uid=device_uid)

            args = {"credentials": creds.to_data(identity_uid=service.uid()),
                    "user_uid": user_uid,
                    "remember_device": remember_device,
                    "short_uid": short_uid}

            response = service.call_function(function="login", args=args)
            _output("SUCCEEDED!")
            _flush_output()
        except Exception as e:
            _output("FAILED!")
            _flush_output()
            from Acquire.Client import LoginError
            raise LoginError("Failed to log in. %s" % e.args)

        if not remember_password:
            return

        try:
            returned_user_uid = response["user_uid"]

            if returned_user_uid != user_uid:
                # change of user?
                userinfo = {}
                user_uid = returned_user_uid
        except:
            # no user_uid, so nothing to save
            return

        if user_uid is None:
            # can't save anything
            return

        userinfo["username"] = username
        userinfo["password"] = password

        try:
            userinfo["device_uid"] = response["device_uid"]
        except:
            pass

        try:
            userinfo["otpsecret"] = response["otpsecret"]
        except:
            pass

        self._set_userinfo(userinfo=userinfo,
                           user_uid=user_uid, identity_uid=service.uid())
