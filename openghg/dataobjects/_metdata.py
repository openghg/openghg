from dataclasses import dataclass
from xarray import Dataset
from typing import Dict


@dataclass(frozen=True)
class METData:
    data: Dataset
    metadata: Dict
