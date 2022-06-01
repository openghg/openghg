"""
Call OpenGHG serverless functions
"""
import requests
from typing import Dict, Optional, Union
import json
import os
import msgpack
from openghg.types import FunctionError


def call_function(fn_name: str, data: Union[Dict, bytes]) -> Dict:
    """Calls an OpenGHG serverless function and returns its response

    Args:
        fn_name: Function name, for list of function names see the documentation
        NOTE: * ADD DOCS *
    Returns:
        dict: Dictionary containing response result
    """
    # First lookup the function URL
    fn_url = _get_function_url(fn_name=fn_name)
    auth_key = _get_auth_key()

    headers = {}
    headers["Content-Type"] = "application/octet-stream"
    headers["authentication"] = auth_key

    packed_data = msgpack.packb(data)

    response = requests.post(url=fn_url, data=packed_data, headers=headers)

    response_content = msgpack.unpackb(response.content)

    d: Dict[str, Union[int, str, Dict, bytes]] = {}
    d["status"] = response.status_code
    d["headers"] = dict(response.headers)
    d["content"] = response_content

    return d


def _get_function_url(fn_name: str) -> str:
    """Get the URl for the required service

    Args:
        service_name: Service name
    Returns:
        str: Service / function URL
    """
    try:
        urls: Dict[str, str] = json.loads(os.environ["FN_URLS"])
    except KeyError:
        raise FunctionError("No FN_URLS environment variable set for function URLs")

    try:
        return urls[fn_name.lower()]
    except KeyError:
        raise FunctionError(f"Unable to find URL for {fn_name}")


def _get_auth_key() -> str:
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


def _put(
    url: str, data: bytes, headers: Optional[Dict] = None, auth_key: Optional[str] = None
) -> requests.Response:
    """PUT some data to the URL

    Args:
        url: URL
        data: Data as bytes
        headers: Optional headers dictionary
        auth_key: Authorisation key if required
    Returns:
        requests.Response
    """
    if headers is None:
        headers = {}

    headers["Content-Type"] = "application/octet-stream"

    if auth_key is not None:
        headers["authentication"] = auth_key

    return requests.put(url=url, data=data, headers=headers)


def _post(url: str, data: Optional[Dict] = None, auth_key: Optional[str] = None) -> requests.Response:
    """POST to a URL

    Args:
        url: URL
        data: Dictionary of data to POST
    Returns:
        requests.Response
    """
    if data is None:
        data = {}

    if auth_key is not None:
        data["authorisation"] = auth_key

    return requests.post(url=url, data=data)
