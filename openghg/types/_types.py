from typing import DefaultDict, Dict, List, Union
from pathlib import Path

pathType = Union[str, Path]
multiPathType = Union[str, Path, List[Union[str, Path]]]
resultsType = DefaultDict[str, Dict]
