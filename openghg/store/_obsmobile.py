from pathlib import Path
from typing import Union

from openghg.store.base import BaseStore

pathType = Union[str, Path]


class ObsMobile(BaseStore):
    _data_type = "mobile"
    pass
