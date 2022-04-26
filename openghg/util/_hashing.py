"""
Some functions for hashing data or strings for idendification of sources
"""
from hashlib import sha1
from pathlib import Path
from typing import Dict


def hash_string(to_hash: str) -> str:
    """Return the SHA-1 hash of a string

    Args:
        to_hash: String to hash
    Returns:
        str: SHA1 hash of string
    """
    return sha1(str(to_hash).encode("utf-8")).hexdigest()


def hash_file(filepath: Path) -> str:
    """Opens the file at filepath and calculates its SHA1 hash

    Taken from https://stackoverflow.com/a/22058673

    Args:
        filepath (pathlib.Path): Path to file
    Returns:
        str: SHA1 hash
    """
    import hashlib

    # Let's read stuff in 64kB chunks
    BUF_SIZE = 65536
    sha1 = hashlib.sha1()

    with open(filepath, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)

    return sha1.hexdigest()


def hash_retrieved_data(to_hash: Dict[str, Dict]) -> Dict:
    """Hash data retrieved from a data platform. This calculates the SHA1 of the metadata
    and the start date, end date and the number of timestamps in the Dataset.

    Args:
        to_hash: Dictionary to hash
        We expected this to be a dictionary such as
        {species_key: {"data": xr.Dataset, "metadata": {...}}}
    Returns:
        dict: Dictionary of hash: species_key
    """
    from hashlib import sha1
    from json import dumps

    hashes: Dict[str, str] = {}
    for key, data in to_hash.items():
        metadata = data["metadata"]
        metadata_hash = sha1(dumps(metadata, sort_keys=True).encode("utf8")).hexdigest()

        ds = data["data"]

        start_date = str(ds.time.min())
        end_date = str(ds.time.max())
        n_timestamps = str(ds.time.size)

        basic_info = f"{start_date}_{end_date}_{n_timestamps}".encode("utf8")
        time_hash = sha1(basic_info).hexdigest()

        combo = (metadata_hash + time_hash).encode("utf8")
        combo_hash = sha1(combo).hexdigest()

        site = metadata["site"]
        species = metadata["species"]
        inlet = metadata["inlet"]

        hashes[combo_hash] = f"{site}_{species}_{inlet}"

    return hashes
