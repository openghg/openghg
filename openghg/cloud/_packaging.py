from collections import defaultdict
from typing import DefaultDict, Dict, Optional

from openghg.util import (
    compress,
    compress_json,
    compress_str,
    decompress,
    decompress_json,
    hash_bytes,
)


def unpackage(data: Dict) -> Dict:
    """Unpackages and checks a dictionary created by the package_from_function function.
    This checks the SHA1 sums and decompresses the data.

    Args:
        data: Dictionary
    Returns:
        dict: Dictionary containing data and metadata if given
    """
    unpacked = {}

    file_metadata = data["file_metadata"]
    remote_sum = file_metadata["data"]["sha1_hash"]

    decompressed_data = decompress(data=data["data"])
    local_sum = hash_bytes(data=decompressed_data)

    unpacked["data"] = decompressed_data

    if not remote_sum == local_sum:
        raise ValueError(f"Hash mismatch, remote {remote_sum} - local {local_sum}.")

    try:
        compressed_metadata = data["metadata"]
    except KeyError:
        pass
    else:
        unpacked["metadata"] = decompress_json(data=compressed_metadata)

    return unpacked


def package_from_function(data: bytes, metadata: Optional[str] = None) -> Dict:
    """Creates a package of data ready to be sent back to the caller.
    This calculates the SHA1 sum of the passed data and compresses it.
    If metadata is passed this is added to the returned dictionary. No SHA1
    is calculated for the metadata.

    NOTE: This function should only be used internally by a serverless function.

    Args:
        data: Binary data
        metadata: Result of json.dumps
    Returns:
        dict: Dictionary of compressed data and file metadata.
    """
    sha1_hash = hash_bytes(data=data)
    compressed_data = compress(data=data)
    compression_type = "bz2"

    packaged: DefaultDict = defaultdict(dict)
    packaged["found"] = True
    packaged["data"] = compressed_data
    packaged["file_metadata"]["data"] = {"sha1_hash": sha1_hash, "compression_type": compression_type}

    if metadata is not None:
        try:
            compressed_metadata = compress_str(s=metadata)
        except AttributeError:
            try:
                compressed_metadata = compress_json(data=metadata)
            except Exception as e:
                raise TypeError(f"Unable to process this object: {e}")

        packaged["metadata"] = compressed_metadata
        packaged["file_metadata"]["metadata"] = {"sha1_hash": False, "compression_type": compression_type}

    return dict(packaged)
