from pathlib import Path
import requests
from typing import Optional, Union


def download_data(url: str, filepath: Optional[Union[str, Path]] = None, timeout: int = 5) -> Optional[bytes]:
    """Download data from a remote URL and returns it as bytes if retrieved correctly.

    Args:
        url: URL of content to download
        filepath: Filepath to write out data
        timeount: Timeout for HTTP request (seconds)
    Returns:
        bytes / None: Bytes if no filepath given
    """
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

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
        content = http.get(url, timeout=timeout).content
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        print(f"Unable to retrieve data from {url}, error: {str(e)}")
        return None

    if filepath is None:
        return content
    else:
        filepath = Path(filepath)
        filepath.write_bytes(content)
        return None
