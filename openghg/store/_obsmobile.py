from openghg.store.base import BaseStore
from pathlib import Path
from typing import Union

pathType = Union[str, Path]


class ObsMobile(BaseStore):
    def __init__(self):
        self.something = []
        raise NotImplementedError

    @staticmethod
    def read_file(filepath: pathType, network: str):
        raise NotImplementedError
