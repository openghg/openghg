from pathlib import Path
from typing import Dict, Union

from openghg.store.base import get_data_class
from openghg.objectstore import get_bucket


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
