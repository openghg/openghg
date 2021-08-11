"""
This is used as a base for the other dataclasses and shouldn't be used directly.
"""
from dataclasses import dataclass
from xarray import Dataset
from typing import Dict


@dataclass(frozen=True)
class _BaseData:
    data: Dataset
    metadata: Dict

    def __bool__(self) -> bool:
        return bool(self.data)

    def __str__(self) -> str:
        return f"Data: {self.data}\nMetadata : {self.metadata}"
