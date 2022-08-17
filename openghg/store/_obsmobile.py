from pathlib import Path
from typing import Union

from openghg.store.base import BaseStore

pathType = Union[str, Path]


class ObsMobile(BaseStore):
    @staticmethod
    def read_file(filepath: pathType, network: str) -> None:
        raise NotImplementedError
