from dataclasses import dataclass

from xarray import Dataset


@dataclass(frozen=True)
class METData:
    data: Dataset
    metadata: dict
