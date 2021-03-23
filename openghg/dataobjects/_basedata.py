"""
This is used as a base for the other dataclasses and shouldn't be used directly.
"""
from dataclasses import dataclass
from xarray import Dataset
from typing import Dict


@dataclass
class _BaseData:
    data: Dataset
    metadata: Dict

    def __str__(self):
        return f"Data: {self.data}\nMetadata : {self.metadata}"
