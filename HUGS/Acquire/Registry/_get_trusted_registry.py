
__all__ = ["get_trusted_registry_service", "get_primary_registry_uid"]


def get_primary_registry_uid(service_uid):
    """Return the primary registry uid of the service with
       passed service_uid.
    """
    try:
        root = service_uid.split("-")[0]
        return "%s-%s" % (root, root)
    except:
        return "a0-a0"


def get_trusted_registry_service(registry_uid=None,
                                 service_uid=None, service_url=None):
    """Return the trusted service info for the registry with specified
       registry_uid, or get any trusted registry service using either
       'service_uid' or 'service_url' as a starting hint to
       locate a suitable registry
    """
    if service_uid is not None:
        # for the moment, just try to get one registry. Eventually we should
        # try to get several in case this one is down
        registry_uid = get_primary_registry_uid(service_uid)
        return get_trusted_registry_service(registry_uid=registry_uid)

    if service_url is not None:
        if service_url.find(".") != -1:
            # try the main acquire registry first
            return get_trusted_registry_service(registry_uid="a0-a0")
        else:
            # this must be testing...
            return get_trusted_registry_service(registry_uid="Z9-Z9")

    if registry_uid is None:
        raise PermissionError(
            "You must specify one of registry_uid, service_uid "
            "or service_url")

    from Acquire.Service import get_trusted_service as _get_trusted_service

    try:
        registry_service = _get_trusted_service(service_uid=registry_uid,
                                                autofetch=False)
    except:
        registry_service = None

    if registry_service is not None:
        if not registry_service.is_registry_service():
            from Acquire.Service import ServiceError
            raise ServiceError(
                "The requested service (%s) for %s is NOT a "
                "registry service!" % (registry_service, registry_uid))

        if registry_service.uid() != registry_uid:
            from Acquire.Service import ServiceError
            raise ServiceError(
                "Disagreement of UID (%s) is NOT the right registry service!" %
                registry_service)

        # everything is ok - we have seen this registry before
        return registry_service

    # boostrapping
    from Acquire.Registry import get_registry_details \
        as _get_registry_details

    details = _get_registry_details(registry_uid)

    from Acquire.Service import call_function as _call_function
    from Acquire.Service import Service as _Service
    from Acquire.Crypto import get_private_key as _get_private_key
    from Acquire.Crypto import PrivateKey as _PrivateKey
    from Acquire.Crypto import PublicKey as _PublicKey
    from Acquire.ObjectStore import bytes_to_string as _bytes_to_string
    from Acquire.ObjectStore import string_to_bytes as _string_to_bytes

    privkey = _get_private_key(key="registry")

    pubkey = _PublicKey.from_data(details["public_key"])
    pubcert = _PublicKey.from_data(details["public_certificate"])

    # ask the registry to return to us their latest details - use
    # a challenge-response to make sure that the response is
    # properly returned
    challenge = _PrivateKey.random_passphrase()
    encrypted_challenge = _bytes_to_string(pubkey.encrypt(challenge))
    args = {"challenge": encrypted_challenge,
            "fingerprint": pubkey.fingerprint()}

    result = _call_function(service_url=details["canonical_url"],
                            function=None,
                            args=args,
                            args_key=pubkey,
                            response_key=privkey,
                            public_cert=pubcert)

    if result["response"] != challenge:
        from Acquire.Service import ServiceError
        raise ServiceError(
            "The requested service (%s) failed to respond to the challenge!" %
            registry_service)

    registry_service = _Service.from_data(result["service_info"])

    if not registry_service.is_registry_service():
        from Acquire.Service import ServiceError
        raise ServiceError(
            "The requested service (%s) is NOT a registry service!" %
            registry_service)

    if registry_service.uid() != details["uid"]:
        from Acquire.Service import ServiceError
        raise ServiceError(
            "Disagreement of UID (%s) is NOT the right registry service!" %
            registry_service)

    # ok - we've got the registry - add this to the set of
    # trusted services so that we don't need to bootstrap from
    # the registry details again
    from Acquire.Service import trust_service as _trust_service
    _trust_service(registry_service)

    return registry_service
