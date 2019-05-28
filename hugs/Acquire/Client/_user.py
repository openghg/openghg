
from enum import Enum as _Enum

__all__ = ["User"]


class _LoginStatus(_Enum):
    EMPTY = 0
    LOGGING_IN = 1
    LOGGED_IN = 2
    LOGGED_OUT = 3
    ERROR = 4


def _output(s, end=None):
    """Simple output function that can be removed during testing"""
    if end is None:
        print(s)
    else:
        print(s, end=end)


def _get_identity_url():
    """Function to discover and return the default identity url"""
    return "fn.acquire-aaai.com"


def _get_identity_service(identity_url=None):
    """Function to return the identity service for the system"""
    if identity_url is None:
        identity_url = _get_identity_url()

    from Acquire.Service import is_running_service as _is_running_service
    if _is_running_service():
        from Acquire.Service import get_trusted_service \
            as _get_trusted_service
        return _get_trusted_service(service_url=identity_url,
                                    service_type='identity')

    from Acquire.Client import LoginError

    try:
        from Acquire.Client import Wallet as _Wallet
        wallet = _Wallet()
        service = wallet.get_service(service_url=identity_url,
                                     service_type="identity")
    except Exception as e:
        from Acquire.Service import exception_to_string
        raise LoginError("Have not received the identity service info from "
                         "the identity service at '%s'\n\nCAUSE: %s" %
                         (identity_url, exception_to_string(e)))

    if not service.can_identify_users():
        raise LoginError(
            "You can only use a valid identity service to log in! "
            "The service at '%s' is a '%s'" %
            (identity_url, service.service_type()))

    return service


def _get_random_sentence():
    """This function generates and returns a random (nonsense)
       sentence
    """
    adjs = ["white", "black", "fluffy", "small", "big"]
    animals = ["cows", "sheep", "mice", "dogs", "cats"]
    verbs = ["walk", "run", "stand", "climb", "sit"]
    advs = ["slowly", "quickly", "quietly", "noisily", "happily"]

    import random as _random

    adj = adjs[_random.randint(0, len(adjs)-1)]
    animal = animals[_random.randint(0, len(animals)-1)]
    verb = verbs[_random.randint(0, len(verbs)-1)]
    adv = advs[_random.randint(0, len(advs)-1)]

    return "%s %s %s %s" % (adj, animal, verb, adv)


class User:
    """This class holds all functionality that would be used
       by a user to authenticate with and access the service.
       This represents a single client login, and is the
       user-facing part of Acquire
    """
    def __init__(self, username=None,
                 identity_url=None, identity_uid=None,
                 scope=None, permissions=None, auto_logout=True):
        """Construct the user with specified 'username', who will
           login to the identity service at specified URL
           'identity_url', or with UID 'identity_uid'. You can
           optionally request a user with a limited scope or
           limited permissions. Service lookup will be using
           you wallet. By default,
           the user will logout when this object is destroyed.
           Prevent this behaviour by setting auto_logout to False
        """
        self._username = username
        self._status = _LoginStatus.EMPTY
        self._identity_service = None
        self._scope = scope
        self._permissions = permissions

        if identity_url:
            self._identity_url = identity_url

        if identity_uid:
            self._identity_uid = identity_uid
        else:
            self._identity_uid = None

        self._user_uid = None

        if auto_logout:
            self._auto_logout = True
        else:
            self._auto_logout = False

    def __str__(self):
        return "User(name='%s', status=%s)" % (self.username(), self.status())

    def __enter__(self):
        """Enter function used by 'with' statements'"""
        pass

    def __exit__(self, exception_type, exception_value, traceback):
        """Ensure that we logout"""
        if self._auto_logout:
            self.logout()

    def __del__(self):
        """Make sure that we log out before deleting this object"""
        if self._auto_logout:
            self.logout()

    def _set_status(self, status):
        """Internal function used to set the status from the
           string obtained from the LoginSession
        """
        if status == "approved":
            self._status = _LoginStatus.LOGGED_IN
        elif status == "denied":
            self._set_error_state("Permission to log in was denied!")
        elif status == "logged_out":
            self._status = _LoginStatus.LOGGED_OUT

    def is_null(self):
        """Return whether or not this user is null"""
        return self._username is None

    def username(self):
        """Return the username of the user"""
        return self._username

    def uid(self):
        """Return the UID of this user. This uniquely identifies the
           user across all systems
        """
        if self.is_null():
            return None

        if self._user_uid is None:
            raise PermissionError(
                "You cannot get the user UID until after you have logged in")

        return self._user_uid

    def guid(self):
        """Return the global UID of the user. While the UID is highly
           likely to be unique, the GUID should be globally guaranteed
           to be unique. This is ensured by combining the UID of the
           user with the UID of the identity service that
           primarily identifies the user
        """
        return "%s@%s" % (self.uid(), self.identity_service().uid())

    def status(self):
        """Return the current status of this user"""
        return self._status

    def _check_for_error(self):
        """Call to ensure that this object is not in an error
           state. If it is in an error state then raise an
           exception"""
        if self._status == _LoginStatus.ERROR:
            from Acquire.Client import LoginError
            raise LoginError(self._error_string)

    def _set_error_state(self, message):
        """Put this object into an error state, displaying the
           passed message if anyone tries to use this object"""
        self._status = _LoginStatus.ERROR
        self._error_string = message

    def session_key(self):
        """Return the session key for the current login session"""
        self._check_for_error()

        try:
            return self._session_key
        except:
            return None

    def signing_key(self):
        """Return the signing key used for the current login session"""
        self._check_for_error()

        try:
            return self._signing_key
        except:
            return None

    def identity_service(self):
        """Return the identity service info object for the identity
           service used to validate the identity of this user
        """
        if self._identity_service:
            return self._identity_service

        identity_service = _get_identity_service(
                                identity_url=self.identity_service_url())

        # if the user supplied the UID then validate this is correct
        # pylint: disable=assignment-from-none
        if self._identity_uid:
            if identity_service.uid() != self._identity_uid:
                from Acquire.Client import LoginError
                raise LoginError(
                    "The UID of the identity service at '%s', which is "
                    "%s, does not match that supplied by the user, '%s'. "
                    "You should double-check that the UID is correct, or "
                    "that you have supplied the correct identity_url" %
                    (self.identity_service_url(), identity_service.uid(),
                     self._identity_uid))
        else:
            self._identity_uid = identity_service.uid()
        # pylint: enable=assignment-from-none

        self._identity_service = identity_service

        return self._identity_service

    def identity_service_uid(self):
        """Return the UID of the identity service. The combination
           of user_uid+service_uid should uniquely identify this user
           account anywhere in the world
        """
        if self._identity_uid is not None:
            return self._identity_uid
        else:
            return self._identity_service.uid()

    def identity_service_url(self):
        """Return the URL to the identity service. This is the full URL
           to the service, minus the actual function to be called, e.g.
           https://function_service.com/t/identity
        """
        self._check_for_error()

        try:
            return self._identity_url
        except:
            # return the default URL - this should be discovered...
            return _get_identity_url()

    def login_url(self):
        """Return the URL that the user must connect to to authenticate
           this login session"""
        self._check_for_error()

        try:
            return self._login_url
        except:
            return None

    def login_qr_code(self):
        """Return a QR code of the login URL that the user must connect to
           to authenticate this login session"""
        from Acquire.Client import create_qrcode as _create_qrcode
        return _create_qrcode(self._login_url)

    def scope(self):
        """The scope under which this login was requested
           (and is thus valid)
        """
        return self._scope

    def permissions(self):
        """The permissions with which this login was requested,
           (and are thus provided)
        """
        return self._permissions

    def session_uid(self):
        """Return the UID of the current login session. Returns None
           if there is no valid login session"""
        self._check_for_error()

        try:
            return self._session_uid
        except:
            return None

    def is_empty(self):
        """Return whether or not this is an empty login (so has not
           been used for anything yet..."""
        return self._status == _LoginStatus.EMPTY

    def is_logged_in(self):
        """Return whether or not the user has successfully logged in"""
        return self._status == _LoginStatus.LOGGED_IN

    def is_logging_in(self):
        """Return whether or not the user is in the process of loggin in"""
        return self._status == _LoginStatus.LOGGING_IN

    def logout(self):
        """Log out from the current session"""
        if self.is_logged_in() or self.is_logging_in():
            service = self.identity_service()

            args = {"session_uid": self._session_uid}

            if self.is_logged_in():
                from Acquire.Client import Authorisation as _Authorisation
                authorisation = _Authorisation(
                                    resource="logout %s" % self._session_uid,
                                    user=self)
                args["authorisation"] = authorisation.to_data()
            else:
                # we are not fully logged in so cannot generate an
                # authorisation for the logout
                from Acquire.ObjectStore import bytes_to_string \
                    as _bytes_to_string
                resource = "logout %s" % self._session_uid
                signature = self.signing_key().sign(resource)
                args["signature"] = _bytes_to_string(signature)

            result = service.call_function(function="logout", args=args)

            self._status = _LoginStatus.LOGGED_OUT

            return result

    @staticmethod
    def register(username, password, identity_url=None):
        """Request to register a new user with the specified
           username one the identity service running
           at 'identity_url', using the supplied 'password'. This will
           return a QR code that you must use immediately to add this
           user on the identity service to a QR code generator"""
        service = _get_identity_service(identity_url=identity_url)

        from Acquire.Client import Credentials as _Credentials

        encoded_password = _Credentials.encode_password(
                                    identity_uid=service.uid(),
                                    password=password)

        args = {"username": username,
                "password": encoded_password}

        result = service.call_function(function="register", args=args)

        try:
            provisioning_uri = result["provisioning_uri"]
        except:
            from Acquire.Client import UserError
            raise UserError(
                "Cannot register the user '%s' on "
                "the identity service at '%s'!" %
                (username, identity_url))

        # return a QR code for the provisioning URI
        result = {}
        result["provisioning_uri"] = provisioning_uri

        try:
            import re
            otpsecret = re.search(r"secret=([\w\d+]+)&issuer",
                                  provisioning_uri).groups()[0]
            result["otpsecret"] = otpsecret
        except:
            pass

        try:
            from Acquire.Client import create_qrcode as _create_qrcode
            result["qrcode"] = _create_qrcode(provisioning_uri)
        except:
            pass

        return result

    def request_login(self, login_message=None):
        """Request to authenticate as this user. This returns a login URL that
           you must connect to to supply your login credentials

           If 'login_message' is supplied, then this is passed to
           the identity service so that it can be displayed
           when the user accesses the login page. This helps
           the user validate that they have accessed the correct
           login page. Note that if the message is None,
           then a random message will be generated.
        """
        self._check_for_error()

        from Acquire.Client import LoginError

        if not self.is_empty():
            raise LoginError("You cannot try to log in twice using the same "
                             "User object. Create another object if you want "
                             "to try to log in again.")

        # first, create a private key that will be used
        # to sign all requests and identify this login
        from Acquire.Client import PrivateKey as _PrivateKey
        session_key = _PrivateKey()
        signing_key = _PrivateKey()

        args = {"username": self._username,
                "public_key": session_key.public_key().to_data(),
                "public_certificate": signing_key.public_key().to_data(),
                "scope": self._scope,
                "permissions": self._permissions
                }

        # get information from the local machine to help
        # the user validate that the login details are correct
        try:
            hostname = _socket.gethostname()
            ipaddr = _socket.gethostbyname(hostname)
            args["hostname"] = hostname
            args["ipaddr"] = ipaddr
        except:
            pass

        if login_message is None:
            try:
                login_message = _get_random_sentence()
            except:
                pass

        if login_message is not None:
            args["login_message"] = login_message

        identity_service = self.identity_service()

        result = identity_service.call_function(
                        function="request_login", args=args)

        try:
            login_url = result["login_url"]
        except:
            login_url = None

        if login_url is None:
            error = "Failed to login. Could not extract the login URL! " \
                    "Result is %s" % (str(result))
            self._set_error_state(error)
            raise LoginError(error)

        try:
            session_uid = result["session_uid"]
        except:
            session_uid = None

        if session_uid is None:
            error = "Failed to login. Could not extract the login " \
                    "session UID! Result is %s" % (str(result))

            self._set_error_state(error)
            raise LoginError(error)

        # now save all of the needed data
        self._login_url = result["login_url"]
        self._session_key = session_key
        self._signing_key = signing_key
        self._session_uid = session_uid
        self._status = _LoginStatus.LOGGING_IN
        self._user_uid = None

        _output("Login by visiting: %s" % self._login_url)

        if login_message is not None:
            _output("(please check that this page displays the message '%s')"
                    % login_message)

        from Acquire.Identity import LoginSession as _LoginSession

        return {"login_url": self._login_url,
                "session_uid": session_uid,
                "short_uid": _LoginSession.to_short_uid(session_uid)}

    def _poll_session_status(self):
        """Function used to query the identity service for this session
           to poll for the session status"""
        from Acquire.ObjectStore import bytes_to_string \
            as _bytes_to_string

        service = self.identity_service()

        args = {"session_uid": self._session_uid}

        result = service.call_function(function="get_session_info", args=args)

        # now update the status...
        status = result["session_status"]
        self._set_status(status)

        if self.is_logged_in():
            if self._user_uid is None:
                user_uid = result["user_uid"]
                assert(user_uid is not None)
                self._user_uid = user_uid

    def wait_for_login(self, timeout=None, polling_delta=5):
        """Block until the user has logged in. If 'timeout' is set
           then we will wait for a maximum of that number of seconds

           This will check whether we have logged in by polling
           the identity service every 'polling_delta' seconds.
        """
        self._check_for_error()

        if not self.is_logging_in():
            return self.is_logged_in()

        polling_delta = int(polling_delta)
        if polling_delta > 60:
            polling_delta = 60
        elif polling_delta < 1:
            polling_delta = 1

        import time as _time

        if timeout is None:
            # block forever....
            while True:
                self._poll_session_status()

                if self.is_logged_in():
                    return True

                elif not self.is_logging_in():
                    return False

                _time.sleep(polling_delta)
        else:
            # only block until the timeout has been reached
            timeout = int(timeout)
            if timeout < 1:
                timeout = 1

            from Acquire.ObjectStore import get_datetime_now \
                as _get_datetime_now

            start_time = _get_datetime_now()

            while (_get_datetime_now() - start_time).seconds < timeout:
                self._poll_session_status()

                if self.is_logged_in():
                    return True

                elif not self.is_logging_in():
                    return False

                _time.sleep(polling_delta)

            return False
