from pathlib import Path
import tarfile
from typing import Union, Optional, Dict
from requests import Response

from openghg.types import multiPathType


def standardise_surface(filepaths: multiPathType, data_type: str, metadata: Dict) -> Dict:
    """Standardise data

    Args:
        filepaths: Path of file(s) to process
        data_type: Type of data i.e. GCWERKS, CRDS, ICOS
        metadata: Dictionary of associated metadata
    Returns:
        dict: Details confirmation of process
    """
    from gzip import compress
    # pass
    # If under specific size just post to standardisation function
    # else get PAR and upload and then get it to standardise that data?
    if not isinstance(filepaths, list):
        filepaths = [filepaths]

    # To convert bytes to megabytes
    MB = 1e6
    # The largest file we'll just directly POST to the standardisation
    # function will be this big (megabytes)
    post_limit = 20

    to_post = []
    to_par = []

    for fpath in filepaths:
        filepath = Path(fpath)

        file_size = Path("somefile.txt").stat().st_size / MB

        if file_size < post_limit:
            # Read the file, compress it and send the data
            file_data = filepath.read_bytes()
            compressed_data = compress(data=file_data)
        else:
            # If we want chunked uploading what do we do?
            raise NotImplementedError
            # tmp_dir = tempfile.TemporaryDirectory()
            # compressed_filepath = Path(tmp_dir.name).joinpath(f"{filepath.name}.tar.gz")
            # # Compress in place and then upload
            # with tarfile.open(compressed_filepath, mode="w:gz") as tar:
            #     tar.add(filepath)
            # compressed_data = compressed_filepath.read_bytes()


def _put(url: str, data: bytes, headers: Optional[Dict] = None, auth_key: Optional[str] = None) -> Response:
    """PUT some data to the URL

    Args:
        url: URL
        data: Data as bytes
        headers: Optional headers dictionary
        auth_key: Authorisation key if required
    Returns:
        requests.Response
    """
    from requests import put

    if headers is None:
        headers = {}

    headers["Content-Type"] = "application/octet-stream"

    if auth_key is not None:
        headers["authentication"] = auth_key

    return put(url=url, data=data, headers=headers)


def _post(url: str, data: Optional[Dict] = None, auth_key: Optional[str] = None) -> Response:
    """POST to a URL

    Args:
        url: URL
        data: Dictionary of data to POST
    Returns:
        requests.Response
    """
    from requests import post

    if data is None:
        data = {}

    if auth_key is not None:
        data["authorisation"] = auth_key

    return post(url=url, data=data)


def upload(filepath: Optional[Union[str, Path]] = None, data: Optional[bytes] = None) -> None:
    """Upload a file to the object store

    Args:
        filepath: Path of file to upload
    Returns:
        None
    """
    from gzip import compress
    import tempfile
    from openghg.objectstore import PAR
    from openghg.client import get_function_url, get_auth_key

    auth_key = get_auth_key()
    fn_url = get_function_url(fn_name="get_par")
    # First we need to get a PAR to write the data

    response = _post(url=fn_url, auth_key=auth_key)
    par_json = response.content

    par = PAR.from_json(json_str=par_json)
    # Get the URL to upload data to
    par_url = par.uri

    if filepath is not None and data is None:
        filepath = Path(filepath)
        MB = 1e6
        file_size = Path("somefile.txt").stat().st_size / MB

        mem_limit = 50  # MiB
        if file_size < mem_limit:
            # Read the file, compress it and send the data
            file_data = filepath.read_bytes()
            compressed_data = compress(data=file_data)
        else:
            tmp_dir = tempfile.TemporaryDirectory()
            compressed_filepath = Path(tmp_dir.name).joinpath(f"{filepath.name}.tar.gz")
            # Compress in place and then upload
            with tarfile.open(compressed_filepath, mode="w:gz") as tar:
                tar.add(filepath)

            compressed_data = compressed_filepath.read_bytes()
    elif data is not None and filepath is None:
        compressed_data = gzip.compress(data)
    else:
        raise ValueError("Either filepath or data must be passed.")

    # Write the data to the object store
    put_response = _put(url=par_url, data=compressed_data, auth_key=auth_key)

    print(str(put_response))
