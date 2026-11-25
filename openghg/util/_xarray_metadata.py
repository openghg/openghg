"""Versioned JSON metadata and provenance helpers for xarray objects."""

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import json
import math
from typing import Any, TypeVar, cast

import xarray as xr

__all__ = [
    "encode_xarray_metadata",
    "decode_xarray_metadata",
    "with_xarray_metadata",
    "append_xarray_history",
]

_SCHEMA_VERSION = 1
_Xarray = TypeVar("_Xarray", xr.DataArray, xr.Dataset)


def _primitive(value: Any) -> Any:
    """Return JSON primitives, rejecting values that would need implicit coercion."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("metadata numbers must be finite")
        return value
    if isinstance(value, list):
        return [_primitive(item) for item in value]
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("metadata keys must be strings")
        return {key: _primitive(item) for key, item in value.items()}
    raise TypeError(f"unsupported metadata value: {type(value).__name__}")


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate metadata JSON key: {key!r}")
        result[key] = value
    return result


def encode_xarray_metadata(metadata: Any) -> str:
    """Encode a dataclass instance or mapping as versioned JSON for an xarray attribute.

    Values must be JSON primitives, lists, or mappings with string keys. Tuples,
    NumPy scalars, dates, and non-finite numbers are rejected rather than silently
    converted. The result has ``schema_version`` (currently 1) and ``metadata`` keys.

    Raises:
        TypeError: Input or a nested value is unsupported.
        ValueError: A number is non-finite or the input contains a cycle.
    """
    try:
        if is_dataclass(metadata) and not isinstance(metadata, type):
            metadata = asdict(metadata)
        if not isinstance(metadata, Mapping):
            raise TypeError("metadata must be a dataclass instance or mapping")
        payload = _primitive(metadata)
    except RecursionError as exc:
        raise ValueError("metadata must not contain a cycle") from exc
    return json.dumps(
        {"schema_version": _SCHEMA_VERSION, "metadata": payload}, allow_nan=False, sort_keys=True
    )


def decode_xarray_metadata(value: str) -> dict[str, Any]:
    """Decode version 1 metadata JSON, returning its validated mapping.

    Raises:
        TypeError: The attribute is not a string.
        ValueError: JSON is malformed, its envelope or values are invalid, or its
            schema version is missing or unsupported.
    """
    if not isinstance(value, str):
        raise TypeError("metadata attribute must be a JSON string")
    try:
        envelope = json.loads(value, object_pairs_hook=_unique_pairs)
    except json.JSONDecodeError as exc:
        raise ValueError("malformed metadata JSON") from exc
    if not isinstance(envelope, dict) or set(envelope) != {"schema_version", "metadata"}:
        raise ValueError("metadata JSON must contain schema_version and metadata")
    if type(envelope["schema_version"]) is not int or envelope["schema_version"] != _SCHEMA_VERSION:
        raise ValueError(f"unsupported metadata schema version: {envelope['schema_version']!r}")
    if not isinstance(envelope["metadata"], dict):
        raise ValueError("metadata must be a mapping")
    try:
        return cast(dict[str, Any], _primitive(envelope["metadata"]))
    except (TypeError, ValueError) as exc:
        raise ValueError("metadata JSON contains invalid values") from exc


def with_xarray_metadata(data: _Xarray, metadata: Any, *, key: str) -> _Xarray:
    """Return a shallow xarray copy with versioned JSON in one global attribute.

    Other attributes and the input object are unchanged. ``key`` must be a nonempty
    attribute name; an existing attribute with that name is replaced.
    """
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a nonempty string")
    encoded = encode_xarray_metadata(metadata)
    result = data.copy(deep=False)
    result.attrs = {**data.attrs, key: encoded}
    return cast(_Xarray, result)


def append_xarray_history(data: _Xarray, entry: str) -> _Xarray:
    """Append a UTC timestamp and one-line entry to the CF ``history`` attribute.

    Existing history and other attributes are preserved on a shallow copy. Raises
    ``ValueError`` for an empty or multiline entry and ``TypeError`` if existing
    history is not a string.
    """
    if not isinstance(entry, str) or not entry.strip() or "\n" in entry or "\r" in entry:
        raise ValueError("history entry must be a nonempty single line")
    history = data.attrs.get("history", "")
    if not isinstance(history, str):
        raise TypeError("history attribute must be a string")
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    line = f"{timestamp}: {entry}"
    result = data.copy(deep=False)
    result.attrs = {
        **data.attrs,
        "history": history + ("\n" if history and not history.endswith("\n") else "") + line,
    }
    return cast(_Xarray, result)
