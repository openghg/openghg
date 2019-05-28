
__all__ = ["get_registry_details", "update_registry_keys_and_certs"]

_testing_registry = {
    "canonical_url": "registry",
    "uid": "Z9-Z9",
    "public_key": None,
    "public_certificate": None}

_acquire_registry = {
    "canonical_url": "http://fn.acquire-aaai.com:8080/t/registry",
    "uid": "a0-a0",
    "public_key": None,
    "public_certificate": None}

_registries = {_testing_registry["uid"]: _testing_registry,
               _acquire_registry["uid"]: _acquire_registry}


def update_registry_keys_and_certs(registry_uid, public_key,
                                   public_certificate):
    """This function is called to update the registry details stored
       globally with those in the newly-created registry-service. This
       function should only be called by registry services after
       construction
    """
    if registry_uid not in _registries:
        raise PermissionError(
            "Cannot update registry details as this is not one of the "
            "centrally approved registries!")

    from Acquire.Crypto import PublicKey as _PublicKey

    if type(public_key) is not _PublicKey:
        raise TypeError("The public key must be type PublicKey")

    if type(public_certificate) is not _PublicKey:
        raise TypeError("The public certificate must be type PublicKey")

    r = _registries[registry_uid]

    r["public_key"] = public_key.to_data()
    r["public_certificate"] = public_certificate.to_data()

    _registries[registry_uid] = r


def get_registry_details(registry_uid):
    """Return the details for the registry with specified UID.
       Note that this will only return details for the approved
       and centrally-registered registries. This returns
       a dictionary with key registry details.
    """
    try:
        registry = _registries[registry_uid]
    except:
        registry = _registries["a0-a0"]

    if registry["public_key"] is None:
        try:
            from importlib import import_module as _import_module
            _keys = _import_module("._keys_%s" % registry_uid,
                                   package="Acquire.Registry")
            registry["public_key"] = _keys.public_key
            registry["public_certificate"] = _keys.public_certificate
        except:
            pass

    import copy as _copy

    return _copy.copy(registry)
