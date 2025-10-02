from pathlib import Path
from rich.progress import wrap_file
import logging

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_url_filename(url: str) -> str:
    """Get the filename from a (messy) URL.

    Args:
        url: URL of file
    Returns:
        str: Filename
    """
    from urllib.parse import urlparse

    return Path(urlparse(url).path).name


def download_data(url: str, filepath: str | Path | None = None, timeout: int = 10) -> bytes | None:
    """Download data file, with progress bar.

    Based on https://stackoverflow.com/a/63831344/1303032

    Args:
        url: URL of content to download
        filepath: Filepath to write out data
        timeount: Timeout for HTTP request (seconds)
    Returns:
        bytes / None: Bytes if no filepath given
    """
    import io
    import shutil
    from urllib.parse import urlparse

    import requests
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
    except (
        requests.exceptions.RequestException,
        requests.exceptions.ConnectionError,
    ) as e:
        logger.info(f"Unable to retrieve data from {url}, error: {str(e)}")
        return None

    filename = Path(urlparse(url).path).name

    if r.status_code != 200:
        logger.info(f"Unable to download {url}, please check URL.")
        return None

    file_size = int(r.headers.get("Content-Length", 0))

    desc = f"Downloading {filename}"
    r.raw.decode_content = True

    # mypy error ignored
    # rich and requests libraries not quite aligning but urllib3.response.HTTPResponse should be very similiar to BinaryIO object expected.
    with wrap_file(r.raw, total=file_size, description=desc) as r_raw:  # type:ignore
        with io.BytesIO() as buf:
            shutil.copyfileobj(r_raw, buf)

            if filepath is None:
                return buf.getvalue()
            else:
                Path(filepath).write_bytes(buf.getvalue())
                return None
