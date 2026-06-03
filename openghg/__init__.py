import logging as _logging
import sys as _sys
from importlib import import_module as _import_module
from importlib.metadata import PackageNotFoundError, version as _version
from typing import Any

if _sys.version_info < (3, 10):
    raise ImportError("openghg requires Python >= 3.10")

__all__ = [
    "analyse",
    "dataobjects",
    "objectstore",
    "datapack",
    "retrieve",
    "plotting",
    "standardise",
    "store",
    "types",
    "tutorial",
    "util",
]

_SUBMODULES = frozenset(__all__)


def __getattr__(name: str) -> Any:
    """Lazily import top-level OpenGHG subpackages."""
    if name in _SUBMODULES:
        module = _import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Return the lazy public API for interactive introspection."""
    return sorted([*__all__, "__version__", "__branch__", "__repository__", "__revisionid__", "logger"])


try:
    __version__ = _version("openghg")
except PackageNotFoundError:
    # Fallback version if package metadata is not available
    __version__ = "unknown"

# These attributes are no longer available with the new versioning approach
# Set to None for backward compatibility
__branch__ = None
__repository__ = None
__revisionid__ = None

logger = _logging.getLogger("openghg")
logger.addHandler(_logging.NullHandler())
