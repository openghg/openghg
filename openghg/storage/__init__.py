"""Storage for Xarray Datasets."""

from ._chunking import ChunkingSchema, chunk_size_in_megabytes
from ._compression import compare_compression
from ._convert import convert_store
from ._store import Store, MemoryStore, VersionedMemoryStore
from ._zarr_store import (
    get_zarr_directory_store,
    get_zarr_memory_store,
    get_versioned_zarr_directory_store,
    get_versioned_zarr_memory_store,
)

__all__ = (
    "ChunkingSchema",
    "chunk_size_in_megabytes",
    "compare_compression",
    "convert_store",
    "MemoryStore",
    "Store",
    "VersionedMemoryStore",
    "get_versioned_zarr_directory_store",
    "get_versioned_zarr_memory_store",
    "get_zarr_directory_store",
    "get_zarr_memory_store",
)
