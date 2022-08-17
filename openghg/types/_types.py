from pathlib import Path
from typing import DefaultDict, Dict, List, Tuple, Union

pathType = Union[str, Path]
multiPathType = Union[str, Path, Tuple, List]
resultsType = DefaultDict[str, Dict]
