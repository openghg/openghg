from importlib import import_module as _import_module
from typing import Any

__all__ = [
    "BoundaryConditions",
    "DataSchema",
    "Flux",
    "EulerianModel",
    "Footprints",
    "infer_date_range",
    "update_zero_dim",
    "ObsMobile",
    "ObsColumn",
    "ObsSurface",
    "add_noaa_obspack",
    "SiteMet",
    "data_class_info",
    "get_data_class",
    "FluxTimeseries",
    "check_metakeys",
    "get_metakeys",
    "write_metakeys",
    "create_custom_config",
    "get_metakey_defaults",
    "define_general_informational_keys",
    "find_info_list_metakeys",
    "find_list_metakeys",
]

_EXPORTS = {
    "BoundaryConditions": "._boundary_conditions",
    "DataSchema": "._data_schema",
    "Flux": "._flux",
    "EulerianModel": "._eulerian_model",
    "Footprints": "._footprints",
    "infer_date_range": "._infer_time",
    "update_zero_dim": "._infer_time",
    "ObsMobile": "._obsmobile",
    "ObsColumn": "._obscolumn",
    "ObsSurface": "._obssurface",
    "add_noaa_obspack": "._populate",
    "SiteMet": "._met",
    "data_class_info": "._meta",
    "get_data_class": "._meta",
    "FluxTimeseries": "._flux_timeseries",
    "check_metakeys": "._metakeys_config",
    "get_metakeys": "._metakeys_config",
    "write_metakeys": "._metakeys_config",
    "create_custom_config": "._metakeys_config",
    "get_metakey_defaults": "._metakeys_config",
    "define_general_informational_keys": "._metakeys_config",
    "find_info_list_metakeys": "._metakeys_config",
    "find_list_metakeys": "._metakeys_config",
}


def __getattr__(name: str) -> Any:
    """Lazily import store classes and helpers on first access."""
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
