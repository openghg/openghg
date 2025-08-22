from dataclasses import dataclass
import math


def chunk_size_in_megabytes(dtype_bytes: int, chunks: dict[str, int]) -> int:
    """Compute size of chunks (in MB) given number of bytes in scalar value and chunks dict."""
    MB_to_bytes = 1024**2
    bytes_to_MB = 1 / MB_to_bytes
    chunk_bytes = dtype_bytes * math.prod(chunks.values())
    return int(chunk_bytes * bytes_to_MB)


@dataclass(frozen=True)
class ChunkingSchema:
    """Chunking scheme for zarr storage.

    Args:
        variable: Name of the variable
        chunks: Dictionary of chunk sizes for each dimension
        secondary_dimensions: List of secondary dimensions to chunk over
    """

    variable: str
    chunks: dict[str, int]
    secondary_dims: list[str]
    # max chunk size in megabytes
    max_chunk_size: int = 300
