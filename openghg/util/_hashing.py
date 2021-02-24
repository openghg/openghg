"""
Some functions for hashing data or strings for idendification of sources
"""
from hashlib import sha1

__all__ = ["hash_string"]


def hash_string(to_hash: str) -> str:
    """ Return the SHA-1 hash of a string

        Args:
            to_hash: String to hash
        Returns:
            str: SHA1 hash of string
    """
    return sha1(str(to_hash).encode("utf-8")).hexdigest()
