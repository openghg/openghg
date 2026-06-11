from importlib import import_module as _import_module
from typing import Any

__all__ = [
    "SurfaceTypes",
    "ColumnTypes",
    "ColumnSources",
    "ObsTypes",
    "FluxTimeseriesTypes",
    "FluxTypes",
    "FluxDatabases",
    "FootprintTypes",
    "BoundaryConditions",
    "EulerianModelTypes",
    "MetTypes",
    "DataTypes",
    "OpenGHGError",
    "InvalidSiteError",
    "UnknownDataError",
    "FunctionError",
    "ObjectStoreError",
    "DatasourceLookupError",
    "DatasourceCombineError",
    "EncodingError",
    "MutexTimeoutError",
    "RequestBucketError",
    "SearchError",
    "ParseError",
    "AttrMismatchError",
    "MetadataFormatError",
    "DataOverlapError",
    "ConfigFileError",
    "MetastoreError",
    "ZarrStoreError",
    "KeyExistsError",
    "MetadataMissingError",
    "StandardiseError",
    "TransformError",
    "ValidationError",
    "construct_xesmf_import_error",
    "StorageError",
    "UpdateError",
    "multiPathType",
    "pathType",
    "resultsType",
    "ArrayLike",
    "ArrayLikeMatch",
    "XrDataLike",
    "XrDataLikeMatch",
    "ReindexMethod",
    "TimePeriod",
    "HasMetadataAndData",
    "MetadataAndData",
    "Comparable",
    "convert_to_list_of_metadata_and_data",
]

_ENUM_EXPORTS = {
    "SurfaceTypes",
    "ColumnTypes",
    "ColumnSources",
    "ObsTypes",
    "FluxTimeseriesTypes",
    "FluxTypes",
    "FluxDatabases",
    "FootprintTypes",
    "BoundaryConditions",
    "EulerianModelTypes",
    "MetTypes",
    "DataTypes",
}

_ERROR_EXPORTS = {
    "OpenGHGError",
    "InvalidSiteError",
    "UnknownDataError",
    "FunctionError",
    "ObjectStoreError",
    "DatasourceLookupError",
    "DatasourceCombineError",
    "EncodingError",
    "MutexTimeoutError",
    "RequestBucketError",
    "SearchError",
    "ParseError",
    "AttrMismatchError",
    "MetadataFormatError",
    "DataOverlapError",
    "ConfigFileError",
    "MetastoreError",
    "ZarrStoreError",
    "KeyExistsError",
    "MetadataMissingError",
    "StandardiseError",
    "TransformError",
    "ValidationError",
    "construct_xesmf_import_error",
    "StorageError",
    "UpdateError",
}

_TYPE_EXPORTS = {
    "multiPathType",
    "pathType",
    "resultsType",
    "ArrayLike",
    "ArrayLikeMatch",
    "XrDataLike",
    "XrDataLikeMatch",
    "ReindexMethod",
    "TimePeriod",
    "HasMetadataAndData",
    "MetadataAndData",
    "Comparable",
    "convert_to_list_of_metadata_and_data",
}

_EXPORTS = {
    **{name: "._enum" for name in _ENUM_EXPORTS},
    **{name: "._errors" for name in _ERROR_EXPORTS},
    **{name: "._types" for name in _TYPE_EXPORTS},
}


def __getattr__(name: str) -> Any:
    """Lazily import type, enum, and error definitions on first access."""
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
