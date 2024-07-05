from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class ChunkingSchema:
    """Chunking scheme for zarr storage.

    Args:
        variable: Name of the variable
        chunks: Dictionary of chunk sizes for each dimension
        secondary_dimensions: List of secondary dimensions to chunk over
    """

    variable: str
    chunks: Dict[str, int]
    secondary_dims: List[str]
    # max chunk size in megabytes
    max_chunk_size: int = 300
