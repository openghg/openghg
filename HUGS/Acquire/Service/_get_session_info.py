
__all__ = ["get_session_info"]


def get_session_info(identity_url, session_uid,
                     scope=None, permissions=None):
    """Call the identity_url to obtain information about the
       specified login session_uid. Optionally limit
       the scope and permissions for which these certs would
       be valid
    """
    from Acquire.Service import get_trusted_service as _get_trusted_service

    service = _get_trusted_service(identity_url)

    args = {"session_uid": session_uid}

    if scope is not None:
        args["scope"] = scope

    if permissions is not None:
        args["permissions"] = permissions

    response = service.call_function(function="get_session_info", args=args)

    try:
        del response["status"]
    except:
        pass

    try:
        del response["message"]
    except:
        pass

    from Acquire.Crypto import PublicKey as _PublicKey

    for key in response.keys():
        if key in ["public_key", "public_certificate"]:
            response[key] = _PublicKey.from_data(response[key])

    return response
