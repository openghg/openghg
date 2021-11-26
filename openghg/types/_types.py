from typing import DefaultDict, Dict, Union
from pathlib import Path

pathType = Union[str, Path]
multiPathType = Union[str, Path, list]
resultsType = DefaultDict[str, Dict]
