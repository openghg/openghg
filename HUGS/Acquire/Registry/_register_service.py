
__all__ = ["register_service"]


def register_service(service, registry_uid):
    """Call this function to register the passed new service with
       the specified registry_uid. This function will complete
       registration and construction of the service
    """
    from Acquire.Service import Service as _Service
    if not isinstance(service, _Service):
        raise TypeError("You can only register Service objects")

    if not service.uid().startswith("STAGE1"):
        raise PermissionError(
            "You can only register services that are at STAGE1 of "
            "construction")

    if service.service_type() == "registry":
        from Acquire.Registry import get_registry_details \
            as _get_registry_details

        details = _get_registry_details(registry_uid=registry_uid)

        from Acquire.Service import Service as _Service
        canonical_url = _Service.get_canonical_url(details["canonical_url"])

        # make sure that everything matches what was specified
        if canonical_url != service.canonical_url():
            raise PermissionError(
                "Cannot change the canonical URL. I expect %s, but "
                "you are trying to set to %s" %
                (service.canonical_url(), details["canonical_url"]))

        from Acquire.Registry import update_registry_keys_and_certs \
            as _update_registry_keys_and_certs

        _update_registry_keys_and_certs(
                            registry_uid=registry_uid,
                            public_key=service.public_key(),
                            public_certificate=service.public_certificate())

        service.create_stage2(service_uid=registry_uid,
                              response=service._uid)
        return service

    # get the trusted registry
    from Acquire.Registry import get_trusted_registry_service \
        as _get_trusted_registry_service
    registry_service = _get_trusted_registry_service(
                                        service_uid=registry_uid)

    if not registry_service.is_registry_service():
        raise PermissionError(
            "You can only register new services on an existing and valid "
            "registry service. Not %s" % registry_service)

    from Acquire.ObjectStore import bytes_to_string as _bytes_to_string
    pubkey = registry_service.public_key()
    challenge = pubkey.encrypt(service.uid())

    args = {"service": service.to_data(),
            "challenge": _bytes_to_string(challenge),
            "fingerprint": pubkey.fingerprint()}

    result = registry_service.call_function(function="register_service",
                                            args=args)

    service_uid = result["service_uid"]
    response = result["response"]

    service.create_stage2(service_uid=service_uid, response=response)

    return service
