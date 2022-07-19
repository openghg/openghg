from typing import DefaultDict, Dict, List, Tuple, Union
from pathlib import Path

pathType = Union[str, Path]
multiPathType = Union[str, Path, Tuple, List]
resultsType = DefaultDict[str, Dict]
