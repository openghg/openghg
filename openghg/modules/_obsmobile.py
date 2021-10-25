from openghg.modules import BaseModule
from pathlib import Path
from typing import Union, List, Dict

pathType = Union[str, Path]

class ObsMobile(BaseModule):
    def __init__(self):
        self.something = []

    @staticmethod
    def read_file(filepath: pathType, network: str):
        raise NotImplementedError
