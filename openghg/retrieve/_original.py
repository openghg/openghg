from pathlib import Path
from typing import Dict, Union

from openghg.store import get_data_class
from openghg.objectstore import get_bucket


def check_file_processed(store: str, data_type: str, filepath: Union[str, Path]) -> bool:
    """Check if a file has already been added to the object store

    Args:
        store: Object store to retrieve from
        data_type: Data type, e.g. footprints, surface etc
        filepath: Path of file to hash
    Returns:
        bool: True if file already processed
    """
    bucket = get_bucket(name=store)
    data_class = get_data_class(data_type=data_type)
    dc = data_class(bucket=bucket)

    seen, _ = dc.check_hashes(filepaths=filepath, force=False)

    return bool(seen)


def retrieve_original_files(
    store: str, data_type: str, hash_data: Dict, output_folderpath: Union[str, Path]
) -> None:
    """Retrieve the original files used when standardising data. The hash_data argument
    should be the {file_hash: filename, ...} format as stored for each version of data
    in the object store.

    Args:
        store: Object store to retrieve from
        data_type: Data type, e.g. footprints, surface etc
        hash_data: Hash data dictionary from metadata
        output_folderpath: The folder to save the retrieved files to
    Returns:
        None
    """
    bucket = get_bucket(name=store)
    data_class = get_data_class(data_type=data_type)
    dc = data_class(bucket=bucket)
    dc.get_original_files(hash_data=hash_data, output_folder=output_folderpath)
