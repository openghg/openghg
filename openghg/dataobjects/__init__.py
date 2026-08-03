from importlib import import_module as _import_module
from typing import Any

__all__ = [
    "BoundaryConditionsData",
    "FluxData",
    "FootprintData",
    "METData",
    "ObsData",
    "ObsColumnData",
    "SearchResults",
    "DataManager",
    "data_manager",
]

_EXPORTS = {
    "BoundaryConditionsData": "._bc_data",
    "FluxData": "._flux_data",
    "FootprintData": "._footprint_data",
    "METData": "._metdata",
    "ObsData": "._obsdata",
    "ObsColumnData": "._obscolumn_data",
    "SearchResults": "._searchresults",
    "DataManager": "._datamanager",
    "data_manager": "._datamanager",
}


def __getattr__(name: str) -> Any:
    """Lazily import data object classes on first access."""
    try:
        module_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

    value = getattr(_import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return the lazy public API for interactive introspection."""
    return sorted(__all__)
