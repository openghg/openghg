from dataclasses import dataclass
from typing import Dict

from xarray import Dataset


@dataclass(frozen=True)
class METData:
    data: Dataset
    metadata: Dict
