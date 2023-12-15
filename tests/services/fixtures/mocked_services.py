##########
# This file defines all of the monkey-patching and fixtures which
# are needed to run the mocked service tests
##########

import os
import uuid

import Acquire
import Acquire.Stubs
import pytest
from access.route import route as access_functions
from accounting.route import route as accounting_functions
from Acquire.Service import create_handler
from compute.route import route as compute_functions
from identity.route import route as identity_functions
from registry.route import route as registry_functions
# We no longer have the openghg_service folder but just have the service functions
# in their respective files
from route import route as openghg_functions
from storage.route import route as storage_functions

identity_handler = create_handler(identity_functions)
accounting_handler = create_handler(accounting_functions)
access_handler = create_handler(access_functions)
registry_handler = create_handler(registry_functions)
storage_handler = create_handler(storage_functions)
compute_handler = create_handler(compute_functions)

openghg_handler = create_handler(openghg_functions)


def _set_services(s, wallet_dir, wallet_password):
    pytest.my_global_services = s
    pytest.my_global_wallet_dir = wallet_dir
    pytest.my_global_wallet_password = wallet_password


def _get_services():
    return pytest.my_global_services


def _get_wallet_dir(**kwargs):
    return pytest.my_global_wallet_dir


def _get_wallet_password(**kwargs):
    return pytest.my_global_wallet_password


Acquire.Client._wallet._get_wallet_dir = _get_wallet_dir
Acquire.Client._wallet._get_wallet_password = _get_wallet_password


class MockedRequests:
    """Mocked requests object. This provides a requests interface which calls
    the 'handler' functions of the services directly, rather
    than posting the arguments to the online services via a requests
    call. In addition, as services can call services, this also
    handles switching between the different local object stores for
    each of the services
    """

    def __init__(self, status_code, content, encoding="utf-8"):
        self.status_code = status_code
        self.content = content
        self.encoding = encoding

    @staticmethod
    def get(url, data, timeout=None):
        return MockedRequests._perform(url, data, is_post=False)

    @staticmethod
    def post(url, data, timeout=None):
        return MockedRequests._perform(url, data, is_post=True)

    @staticmethod
    def _perform(url, data, is_post=False):
        _services = _get_services()

        if "identity" not in _services:
            raise ValueError("NO SERVICES? %s" % str)

        from Acquire.Service import pop_testing_objstore, push_testing_objstore

        if url.startswith("http://"):
            url = url[7:]
        elif url.startswith("https://"):
            url = url[8:]

        # print("We're performing here ", url)

        if url.startswith("identity"):
            push_testing_objstore(_services["identity"])
            func = identity_handler
        elif url.startswith("access"):
            push_testing_objstore(_services["access"])
            func = access_handler
        elif url.startswith("accounting"):
            push_testing_objstore(_services["accounting"])
            func = accounting_handler
        elif url.startswith("storage"):
            push_testing_objstore(_services["storage"])
            func = storage_handler
        elif url.startswith("compute"):
            push_testing_objstore(_services["compute"])
            func = compute_handler
        elif url.startswith("registry"):
            push_testing_objstore(_services["registry"])
            func = registry_handler
        elif url.startswith("openghg"):
            push_testing_objstore(_services["openghg"])
            func = openghg_handler
        else:
            raise ValueError("Cannot recognise service from '%s'" % url)

        result = func(data)

        pop_testing_objstore()

        if type(result) is str:
            result = result.encode("utf-8")

        return MockedRequests(status_code=200, content=result)


def mocked_input(s):
    return "y"


def mocked_output(s, end=None):
    pass


def mocked_flush_output():
    pass


# monkey-patch _pycurl.Curl so that we can mock calls
Acquire.Stubs.requests = MockedRequests

# monkey-patch input so that we can say "y", and so there is no output
Acquire.Client._wallet._input = mocked_input
Acquire.Client._wallet._output = mocked_output
Acquire.Client._wallet._flush_output = mocked_flush_output
Acquire.Client._user._output = mocked_output

_wallet_password = Acquire.Crypto.PrivateKey.random_passphrase()


def _login_admin(service_url, username, password, otp):
    """Internal function used to get a valid login to the specified
    service for the passed username, password and otp
    """
    from Acquire.Client import User, Wallet

    wallet = Wallet()

    user = User(username=username, identity_url=service_url, auto_logout=False)

    result = user.request_login()
    login_url = result["login_url"]

    wallet.send_password(
        url=login_url,
        username=username,
        password=password,
        otpcode=otp.generate(),
        remember_password=False,
        remember_device=False,
    )

    user.wait_for_login()

    return user


@pytest.fixture(scope="session")
def aaai_services(tmpdir_factory):
    """This function creates mocked versions of all of the main services
    of the system, returning the json describing each service as
    a dictionary (which is passed to the test functions as the
    fixture)
    """
    from Acquire.Crypto import OTP, PrivateKey
    from Acquire.Identity import Authorisation
    from Acquire.Service import Service, call_function

    _services = {}
    _services["registry"] = tmpdir_factory.mktemp("registry")
    _services["identity"] = tmpdir_factory.mktemp("identity")
    _services["accounting"] = tmpdir_factory.mktemp("accounting")
    _services["access"] = tmpdir_factory.mktemp("access")
    _services["storage"] = tmpdir_factory.mktemp("storage")
    _services["userdata"] = tmpdir_factory.mktemp("userdata")
    _services["compute"] = tmpdir_factory.mktemp("compute")
    _services["openghg"] = tmpdir_factory.mktemp("openghg")

    wallet_dir = tmpdir_factory.mktemp("wallet")
    wallet_password = PrivateKey.random_passphrase()

    _set_services(_services, wallet_dir, wallet_password)

    password = PrivateKey.random_passphrase()
    args = {"password": password}

    responses = {}

    os.environ["SERVICE_PASSWORD"] = "Service_pa33word"
    os.environ["STORAGE_COMPARTMENT"] = str(_services["userdata"])

    args["canonical_url"] = "registry"
    args["service_type"] = "registry"
    args["registry_uid"] = "Z9-Z9"  # UID of testing registry
    response = call_function("registry", function="admin.setup", args=args)

    registry_service = Service.from_data(response["service"])
    registry_otp = OTP(OTP.extract_secret(response["provisioning_uri"]))
    registry_user = _login_admin("registry", "admin", password, registry_otp)
    responses["registry"] = {
        "service": registry_service,
        "user": registry_user,
        "response": response,
    }

    assert registry_service.registry_uid() == registry_service.uid()
    service_uids = [registry_service.uid()]

    args["canonical_url"] = "identity"
    args["service_type"] = "identity"
    response = call_function("identity", function="admin.setup", args=args)

    identity_service = Service.from_data(response["service"])
    identity_otp = OTP(OTP.extract_secret(response["provisioning_uri"]))
    identity_user = _login_admin("identity", "admin", password, identity_otp)
    responses["identity"] = {
        "service": identity_service,
        "user": identity_user,
        "response": response,
    }

    assert identity_service.registry_uid() == registry_service.uid()
    assert identity_service.uid() not in service_uids
    service_uids.append(identity_service.uid())

    args["canonical_url"] = "accounting"
    args["service_type"] = "accounting"
    response = call_function("accounting", function="admin.setup", args=args)
    accounting_service = Service.from_data(response["service"])
    accounting_otp = OTP(OTP.extract_secret(response["provisioning_uri"]))
    accounting_user = _login_admin("accounting", "admin", password, accounting_otp)
    responses["accounting"] = {
        "service": accounting_service,
        "user": accounting_user,
        "response": response,
    }

    assert accounting_service.registry_uid() == registry_service.uid()
    assert accounting_service.uid() not in service_uids
    service_uids.append(accounting_service.uid())

    args["canonical_url"] = "access"
    args["service_type"] = "access"
    response = call_function("access", function="admin.setup", args=args)
    responses["access"] = response
    access_service = Service.from_data(response["service"])
    access_otp = OTP(OTP.extract_secret(response["provisioning_uri"]))
    access_user = _login_admin("access", "admin", password, access_otp)
    responses["access"] = {
        "service": access_service,
        "user": access_user,
        "response": response,
    }

    assert access_service.registry_uid() == registry_service.uid()
    assert access_service.uid() not in service_uids
    service_uids.append(access_service.uid())

    args["canonical_url"] = "compute"
    args["service_type"] = "compute"
    response = call_function("compute", function="admin.setup", args=args)
    responses["compute"] = response
    compute_service = Service.from_data(response["service"])
    compute_otp = OTP(OTP.extract_secret(response["provisioning_uri"]))
    compute_user = _login_admin("compute", "admin", password, compute_otp)
    responses["compute"] = {
        "service": compute_service,
        "user": compute_user,
        "response": response,
    }

    assert compute_service.registry_uid() == registry_service.uid()
    assert compute_service.uid() not in service_uids
    service_uids.append(compute_service.uid())

    args["canonical_url"] = "storage"
    args["service_type"] = "storage"
    response = call_function("storage", function="admin.setup", args=args)
    storage_service = Service.from_data(response["service"])
    storage_otp = OTP(OTP.extract_secret(response["provisioning_uri"]))
    storage_user = _login_admin("storage", "admin", password, storage_otp)
    responses["storage"] = {
        "service": storage_service,
        "user": storage_user,
        "response": response,
    }

    assert storage_service.registry_uid() == registry_service.uid()
    assert storage_service.uid() not in service_uids
    service_uids.append(storage_service.uid())

    args["canonical_url"] = "openghg"
    args["service_type"] = "openghg"
    response = call_function("openghg", function="admin.setup", args=args)
    responses["openghg"] = response
    openghg_service = Service.from_data(response["service"])
    openghg_otp = OTP(OTP.extract_secret(response["provisioning_uri"]))
    openghg_user = _login_admin("openghg", "admin", password, openghg_otp)
    responses["openghg"] = {
        "service": openghg_service,
        "user": openghg_user,
        "response": response,
    }

    resource = "trust_accounting_service %s" % accounting_service.uid()
    args = {
        "service_url": accounting_service.canonical_url(),
        "authorisation": Authorisation(user=access_user, resource=resource).to_data(),
    }
    access_service.call_function(function="admin.trust_accounting_service", args=args)

    responses["_services"] = _services

    return responses


@pytest.fixture(scope="session")
def authenticated_user(aaai_services):
    from Acquire.Client import User, Wallet
    from Acquire.Crypto import OTP, PrivateKey

    username = str(uuid.uuid4())
    password = PrivateKey.random_passphrase()

    result = User.register(username=username, password=password, identity_url="identity")

    otpsecret = result["otpsecret"]
    otp = OTP(otpsecret)

    # now log the user in
    user = User(username=username, identity_url="identity", auto_logout=False)

    result = user.request_login()

    assert isinstance(result, dict)

    wallet = Wallet()

    wallet.send_password(
        url=result["login_url"],
        username=username,
        password=password,
        otpcode=otp.generate(),
        remember_password=False,
        remember_device=False,
    )

    user.wait_for_login()

    assert user.is_logged_in()

    return user
