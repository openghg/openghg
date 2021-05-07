from dataclasses import dataclass
from ._basedata import _BaseData

__all__ = ["SearchData"]


@dataclass(frozen=True)
class SearchData(_BaseData):
    """ This class is used to return data from the search function

        Args:
            keys: Data keys
            metadata: Dictionary of metadata
    """
    pass
