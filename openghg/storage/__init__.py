"""Storage for Xarray Datasets."""

from typing import TYPE_CHECKING, Any

from ._chunking import ChunkingSchema, chunk_size_in_megabytes
from ._store import Store, MemoryStore, VersionedMemoryStore
from ._zarr_store import (
    get_zarr_directory_store,
    get_zarr_memory_store,
    get_versioned_zarr_directory_store,
    get_versioned_zarr_memory_store,
)

if TYPE_CHECKING:
    from ._compression import compare_compression
    from ._convert import convert_store

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


def __getattr__(name: str) -> Any:
    """Lazily expose rare storage utilities without slowing core imports."""
    if name == "compare_compression":
        from ._compression import compare_compression

        globals()[name] = compare_compression
        return compare_compression

    if name == "convert_store":
        from ._convert import convert_store

        globals()[name] = convert_store
        return convert_store

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
