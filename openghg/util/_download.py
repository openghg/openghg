from pathlib import Path
from typing import Optional, Union


def parse_url_filename(url: str) -> str:
    """Get the filename from a (messy) URL.

    Args:
        url: URL of file
    Returns:
        str: Filename
    """
    from urllib.parse import urlparse

    return Path(urlparse(url).path).name


def download_data(
    url: str, filepath: Optional[Union[str, Path]] = None, timeout: int = 10
) -> Optional[bytes]:
    """Download data file, with progress bar.

    Based on https://stackoverflow.com/a/63831344/1303032

    Args:
        url: URL of content to download
        filepath: Filepath to write out data
        timeount: Timeout for HTTP request (seconds)
    Returns:
        bytes / None: Bytes if no filepath given
    """
    import functools
    import shutil
    import requests
    import io
    from tqdm.auto import tqdm  # type: ignore
    from urllib.parse import urlparse
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry  # type: ignore

    retriable_status_codes = [
        requests.codes.internal_server_error,
        requests.codes.bad_gateway,
        requests.codes.service_unavailable,
        requests.codes.gateway_timeout,
        requests.codes.too_many_requests,
        requests.codes.request_timeout,
    ]

    retry_strategy = Retry(
        total=3,
        status_forcelist=retriable_status_codes,
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1,
    )  # type: ignore

    adapter = HTTPAdapter(max_retries=retry_strategy)

    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    try:
        r = http.get(url=url, stream=True, allow_redirects=True, timeout=timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        print(f"Unable to retrieve data from {url}, error: {str(e)}")
        return None

    filename = Path(urlparse(url).path).name

    if r.status_code != 200:
        print(f"Unable to download {url}, please check URL.")
        return None

    file_size = int(r.headers.get("Content-Length", 0))

    desc = f"Downloading {filename}"
    r.raw.read = functools.partial(r.raw.read, decode_content=True)

    with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
        with io.BytesIO() as buf:
            shutil.copyfileobj(r_raw, buf)

            if filepath is None:
                return buf.getvalue()
            else:
                Path(filepath).write_bytes(buf.getvalue())
                return None
