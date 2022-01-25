"""
Some functions for hashing data or strings for idendification of sources
"""
from hashlib import sha1
from pathlib import Path

__all__ = ["hash_file", "hash_string"]


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
