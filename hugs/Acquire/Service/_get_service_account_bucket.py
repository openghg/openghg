
import os as _os
import json as _json

from cachetools import cached as _cached
from cachetools import LRUCache as _LRUCache

# The cache can hold a maximum of 50 objects, and will remove the
# least recently used items from the cache
_login_cache = _LRUCache(maxsize=50)

__all__ = ["get_service_account_bucket",
           "push_testing_objstore", "pop_testing_objstore",
           "clear_login_cache"]

_current_testing_objstore = None
_testing_objstore_stack = []


def clear_login_cache():
    """Call to clear the login cache"""
    _login_cache.clear()


def push_testing_objstore(testing_dir):
    """Function used in testing to push a new object store onto the stack"""
    from Acquire.Service import clear_service_cache as _clear_service_cache

    global _current_testing_objstore
    global _testing_objstore_stack

    _testing_objstore_stack.append(_current_testing_objstore)
    _current_testing_objstore = testing_dir
    _clear_service_cache()


def pop_testing_objstore():
    """Function used in testing to pop an object store from the stack"""
    from Acquire.Service import clear_service_cache as _clear_service_cache

    global _current_testing_objstore
    global _testing_objstore_stack

    try:
        d = _testing_objstore_stack.pop()
    except:
        d = None

    _current_testing_objstore = d
    _clear_service_cache()


# Cache this function as the result changes very infrequently, as involves
# lots of round trips to the object store, and it will give the same
# result regardless of which Fn function on the service makes the call
@_cached(_login_cache)
def get_service_account_bucket(testing_dir=None):
    """This function logs into the object store account of the service account.
       Accessing the object store means being able to access
       all resources and which can authorise the creation
       of access all resources on the object store. Obviously this is
       a powerful account, so only log into it if you need it!!!

       The login information should not be put into a public
       repository or stored in plain text. In this case,
       the login information is held in an environment variable
       (which should be encrypted or hidden in some way...)
    """
    from Acquire.Service import assert_running_service as \
        _assert_running_service

    _assert_running_service()

    # read the password for the secret key from the filesystem
    try:
        with open("secret_key", "r") as FILE:
            password = FILE.readline()[0:-1]
    except:
        password = None

        # we must be in testing mode...
        from Acquire.ObjectStore import use_testing_object_store_backend as \
            _use_testing_object_store_backend

        # see if this is running in testing mode...
        global _current_testing_objstore
        if testing_dir:
            _current_testing_objstore = testing_dir
            return _use_testing_object_store_backend(testing_dir)
        elif _current_testing_objstore:
            return _use_testing_object_store_backend(_current_testing_objstore)

    if password is None:
        from Acquire.Service import ServiceAccountError
        raise ServiceAccountError(
            "You need to supply login credentials via the 'secret_key' "
            "file, and 'SECRET_KEY' and 'SECRET_CONFIG' environment "
            "variables! %s" % testing_dir)

    secret_key = _os.getenv("SECRET_KEY")

    if secret_key is None:
        from Acquire.Service import ServiceAccountError
        raise ServiceAccountError(
            "You must supply the password used to unlock the configuration "
            "key in the 'SECRET_KEY' environment variable")

    try:
        secret_key = _json.loads(secret_key)
    except Exception as e:
        from Acquire.Service import ServiceAccountError
        raise ServiceAccountError(
            "Unable to decode valid JSON from the secret key: %s" % str(e))

    # use the password to decrypt the SECRET_KEY in the config
    try:
        from Acquire.Crypto import PrivateKey as _PrivateKey
        secret_key = _PrivateKey.from_data(secret_key, password)
    except Exception as e:
        from Acquire.Service import ServiceAccountError
        raise ServiceAccountError(
            "Unable to open the private SECRET_KEY using the password "
            "supplied in the 'secret_key' file: %s" % str(e))

    config = _os.getenv("SECRET_CONFIG")

    if config is None:
        from Acquire.Service import ServiceAccountError
        raise ServiceAccountError(
            "You must supply the encrypted config in teh 'SECRET_CONFIG' "
            "environment variable!")

    try:
        from Acquire.ObjectStore import string_to_bytes as _string_to_bytes
        config = secret_key.decrypt(_string_to_bytes(config))
    except Exception as e:
        from Acquire.Service import ServiceAccountError
        raise ServiceAccountError(
            "Cannot decrypt the 'SECRET_CONFIG' with the 'SECRET_KEY'. Are "
            "you sure that the configuration has been set up correctly? %s "
            % str(e))

    # use the secret_key to decrypt the config in SECRET_CONFIG
    try:
        config = _json.loads(config)
    except Exception as e:
        from Acquire.Service import ServiceAccountError
        raise ServiceAccountError(
            "Unable to decode valid JSON from the config: %s" % str(e))

    # get info from this config
    access_data = config["LOGIN"]
    bucket_data = config["BUCKET"]

    # save the service password to the environment
    _os.environ["SERVICE_PASSWORD"] = config["PASSWORD"]

    # save any other decrypted config data to environment variables
    for key in config.keys():
        if key not in ["LOGIN", "BUCKET", "PASSWORD"]:
            _os.environ[key] = config[key]

    # we have OCI login details, so make sure that we are using
    # the OCI object store backend
    from Acquire.ObjectStore import use_oci_object_store_backend as \
        _use_oci_object_store_backend

    _use_oci_object_store_backend()

    # now login and create/load the bucket for this account
    try:
        from ._oci_account import OCIAccount as _OCIAccount

        account_bucket = _OCIAccount.create_and_connect_to_bucket(
                                    access_data,
                                    bucket_data["compartment"],
                                    bucket_data["bucket"])
    except Exception as e:
        from Acquire.Service import ServiceAccountError
        raise ServiceAccountError(
             "Error connecting to the service account: %s" % str(e))

    return account_bucket
