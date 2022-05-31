"""
Call OpenGHG serverless functions
"""
import requests
from typing import Dict, Optional, Union

def call_function(fn_name: str) -> Dict:
    """ Calls an OpenGHG serverless function and returns its response

    Args:
        fn_name: Function name, for list of function names see the documentation
        NOTE: * ADD DOCS *
    Returns:
        dict: Dictionary containing response result
    """

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
