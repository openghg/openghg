from pathlib import Path
from typing import DefaultDict, Dict, List, Tuple, Union, Optional

pathType = Union[str, Path]
optionalPathType = Optional[pathType]
multiPathType = Union[str, Path, Tuple, List]
resultsType = DefaultDict[str, Dict]
