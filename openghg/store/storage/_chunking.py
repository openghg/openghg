from dataclasses import dataclass


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
