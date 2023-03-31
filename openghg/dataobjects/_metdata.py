from dataclasses import dataclass
from typing import Dict

from xarray import Dataset


@dataclass(frozen=True)
class METData:
    data: Dataset
    metadata: Dict
    
    def __bool__(self) -> bool:
        return bool(self.data)

    def __str__(self) -> str:
        return f"Data: {self.data}\nMetadata : {self.metadata}"
