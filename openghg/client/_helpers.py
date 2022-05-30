import json
import os
from openghg.types import FunctionError


def get_function_url(fn_name: str) -> str:
    """Get the URl for the required service

    Args:
        service_name: Service name
    Returns:
        str: Service / function URL
    """
    try:
        urls = json.loads(os.environ["FN_URLS"])
    except KeyError:
        raise FunctionError("No FN_URLS environment variable set for function URLs")

    try:
        return urls[fn_name.upper()]
    except KeyError:
        raise FunctionError(f"Unable to find URL for {fn_name}")


def get_auth_key() -> str:
    """Get the authentication key from the local environmen

    This offers very limited control over calling of the functions for now.
    It will be replaced by whatever user authentication system we end up using.

    Returns:
        str: Authentication key for serverless Fn
    """
    try:
        return os.environ["AUTH_KEY"]
    except KeyError:
        raise FunctionError("A valid AUTH_KEY secret must be set.")
