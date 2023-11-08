from abc import ABC, abstractmethod


class ZarrStore(ABC):
    """Interface for our zarr stores."""

    @abstractmethod
    def add(self):
        """Add an xr.Dataset to the zarr store."""
        pass

    @abstractmethod
    def delete(self):
        """Remove data from the zarr store"""
        pass

    @abstractmethod
    def delete_all(self):
        """Remove data from the zarr store"""
        pass

    @abstractmethod
    def keys(self):
        """Keys of data stored in the zarr store"""
        pass

    @abstractmethod
    def close(self):
        """Close the zarr store."""
        pass

    @abstractmethod
    def store_key(self) -> str:
        """Return the key of this zarr store"""
        pass

    def pop(self):
        """Pop some data from the store."""
        pass

    @abstractmethod
    def copy_to_memorystore(self):
        """Copies the compressed data from the filesystem store to a list of in-memory stores.
        This preserves the compression and chunking of the data and creates a list
        that can be opened as a single dataset.
        """

    @abstractmethod
    def update(self):
        """Update the data at the given key"""
        pass

    @abstractmethod
    def hash(self):
        """Hash the data at the given key"""
        pass

    @abstractmethod
    def get_hash(self):
        """Get the hash of the data at the given key"""
        pass

    @abstractmethod
    def hash_equal(self):
        """Compare the hashes of the data at the given key and the passed xr.Dataset"""
        pass

    @abstractmethod
    def bytes_stored(self):
        """Return the number of bytes stored in the zarr store"""
        pass
