from pathlib import Path
from typing import Union, Optional, Dict


def upload(filepath: Optional[Union[str, Path]] = None, data: Optional[bytes] = None) -> None:
    """Upload a file to the object store

    Args:
        filepath: Path of file to upload
    Returns:
        None
    """
    # import requests
    # response = _requests.put(url, data=data)
