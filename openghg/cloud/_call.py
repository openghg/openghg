"""
Call OpenGHG serverless functions
"""
import requests
from typing import Dict, Optional, Union, Tuple
import os
import msgpack
from openghg.types import FunctionError
from pathlib import Path
from openghg.util import hash_bytes, compress


def create_file_package(filepath: Path, obs_type: str) -> Tuple[bytes, Dict]:
    """Reads file metadata and compresses data to be sent to the serverless function

    Args:
        filepath: Path of data
        obs_type: Observation type
    Returns:
        tuple: Compressed data as bytes and dictionary of file metadata

    """
    in_mem_limit = 300  # MB
    # To convert bytes to megabytes
    MB = 1e6

    # Get the file size in megabytes
    file_size = filepath.stat().st_size / MB

    if file_size > in_mem_limit:
        raise NotImplementedError("We can't handle this size of file yet.")

    file_data = filepath.read_bytes()
    sha1_hash = hash_bytes(data=file_data)
    compressed_data = compress(data=file_data)
    filename = filepath.name

    file_metadata = {
        "sha1_hash": sha1_hash,
        "filename": filename,
        "compressed": True,
        "obs_type": obs_type.lower(),
    }

    return compressed_data, file_metadata


def create_post_dict(
    function_name: str,
    data: bytes,
    metadata: Dict,
    file_metadata: Dict,
    precision_data: Optional[Dict] = None,
    precision_file_metadata: Optional[Dict] = None,
) -> Dict:
    """Create the dictionary to POST to the remote function

    Args:
        function: Function name
        data: Compressed data
        metadata: Metadata dictionary
        file_metadata: File metadata
        precision_data: GCWERKS precision data
        precision_file_metadata: GCWERKS precision file metadata
    Returns:
        dict: Dictionary ready for function call
    """
    to_post = {
        "function": function_name,
        "data": data,
        "metadata": metadata,
        "file_metadata": file_metadata,
    }

    if precision_data is not None:
        to_post["precision_data"] = precision_data

        if precision_file_metadata is None:
            raise ValueError("We need both precision data and file metadata.")

        to_post["precision_file_metadata"] = precision_file_metadata

    return to_post


def call_function(data: Union[Dict, bytes]) -> Dict:
    """Calls an OpenGHG serverless function and returns its response

    Args:
        data: Data to POST. Must be a dictionary created using the create_post_dict function.
    Returns:
        dict: Dictionary containing response status, headers and content.
    """
    # First lookup the function URL
    fn_url = _get_function_url()
    auth_key = _get_auth_key()

    headers = {}
    headers["Content-Type"] = "application/octet-stream"
    headers["authorization"] = auth_key

    packed_data = msgpack.packb(data)

    response = requests.post(url=fn_url, data=packed_data, headers=headers)

    if response.status_code != 200:
        raise FunctionError(f"Function call error: {str(response.content)}")

    response_content = msgpack.unpackb(response.content)

    d: Dict[str, Union[int, str, Dict, bytes]] = {}
    d["status"] = response.status_code
    d["headers"] = dict(response.headers)
    d["content"] = response_content

    return d


def _get_function_url() -> str:
    """Get the URl for the required service

    Args:
        service_name: Service name
    Returns:
        str: Service / function URL
    """
    try:
        return os.environ["OPENGHG_FN_URL"]
    except KeyError:
        raise FunctionError("No OPENGHG_FN_URL environment variable set for function URLs")


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
