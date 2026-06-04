from dataclasses import dataclass

from xarray import Dataset

__all__ = ["METData"]


@dataclass(frozen=True)
class METData:
    data: Dataset
    metadata: dict
