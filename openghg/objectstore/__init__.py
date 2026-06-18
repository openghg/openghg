from importlib import import_module as _import_module
from typing import Any

__all__ = [
    "delete_object",
    "delete_objects",
    "exists",
    "get_all_object_names",
    "get_bucket",
    "get_object",
    "get_object_data_path",
    "get_object_lock_path",
    "get_object_from_json",
    "get_object_names",
    "get_objectstore_info",
    "get_user_objectstore_path",
    "get_tutorial_store_path",
    "move_object",
    "move_objects",
    "query_store",
    "set_object",
    "set_object_from_file",
    "set_object_from_json",
    "get_writable_bucket",
    "get_writable_buckets",
    "get_readable_buckets",
    "get_folder_size",
    "bytes_to_string",
    "datetime_to_datetime",
    "get_datetime_now",
    "string_to_bytes",
    "integrity_check",
    "Datasource",
    "get_datasource",
    "locking_object_store",
    "LockingObjectStoreType",
    "open_object_store",
]

_EXPORTS = {
    "delete_object": "._local_store",
    "delete_objects": "._local_store",
    "exists": "._local_store",
    "get_all_object_names": "._local_store",
    "get_bucket": "._local_store",
    "get_object": "._local_store",
    "get_object_data_path": "._local_store",
    "get_object_lock_path": "._local_store",
    "get_object_from_json": "._local_store",
    "get_object_names": "._local_store",
    "get_objectstore_info": "._local_store",
    "get_user_objectstore_path": "._local_store",
    "get_tutorial_store_path": "._local_store",
    "move_object": "._local_store",
    "move_objects": "._local_store",
    "query_store": "._local_store",
    "set_object": "._local_store",
    "set_object_from_file": "._local_store",
    "set_object_from_json": "._local_store",
    "get_writable_bucket": "._local_store",
    "get_writable_buckets": "._local_store",
    "get_readable_buckets": "._local_store",
    "get_folder_size": "._local_store",
    "bytes_to_string": "._encoding",
    "datetime_to_datetime": "._encoding",
    "get_datetime_now": "._encoding",
    "string_to_bytes": "._encoding",
    "integrity_check": "._integrity",
    "Datasource": "._legacy_datasource",
    "get_datasource": "._objectstore",
    "locking_object_store": "._objectstore",
    "LockingObjectStoreType": "._objectstore",
    "open_object_store": "._objectstore",
}


def __getattr__(name: str) -> Any:
    """Lazily import object store helpers on first access."""
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
