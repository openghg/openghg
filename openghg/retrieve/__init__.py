from importlib import import_module as _import_module
from typing import Any

__all__ = [
    "get_bc",
    "get_flux",
    "get_footprint",
    "get_obs_column",
    "get_obs_surface",
    "get_ceda_file",
    "search",
    "search_bc",
    "search_column",
    "search_flux",
    "search_eulerian",
    "search_footprints",
    "search_site_met",
    "search_surface",
]

_EXPORTS = {
    "get_bc": "._access",
    "get_flux": "._access",
    "get_footprint": "._access",
    "get_obs_column": "._access",
    "get_obs_surface": "._access",
    "get_ceda_file": "._export",
    "search": "._search",
    "search_bc": "._search",
    "search_column": "._search",
    "search_flux": "._search",
    "search_eulerian": "._search",
    "search_footprints": "._search",
    "search_site_met": "._search",
    "search_surface": "._search",
}


def __getattr__(name: str) -> Any:
    """Lazily import retrieve helpers on first access."""
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
