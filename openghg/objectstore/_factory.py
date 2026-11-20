"""Select configured ObjectStore factories without interpreting backend locations."""

from __future__ import annotations

import importlib
import os
from typing import TYPE_CHECKING, Any, Literal, cast

from openghg.types import ConfigFileError, ObjectStoreError

if TYPE_CHECKING:
    from ._objectstore import ObjectStore


def get_object_store_config(bucket: str) -> dict[str, Any] | None:
    """Return configuration for a bucket location, or None for a direct local path.

    Args:
        bucket: Local path or opaque location declared in the user configuration.

    Returns:
        Matching store configuration, when present.

    Raises:
        ConfigFileError: If multiple stores declare the same location with a custom factory.
    """
    from ._local_store import get_objectstore_info
    from openghg.util._user import get_user_config_path

    try:
        stores = get_objectstore_info()
    except ConfigFileError:
        # Direct local paths have always worked without running quickstart first.
        if get_user_config_path().exists():
            raise
        return None

    matches = [config for config in stores.values() if config["path"] == str(bucket)]
    if len(matches) > 1 and any("factory" in config for config in matches):
        raise ConfigFileError("Custom object stores must have distinct configured locations.")
    return matches[0] if matches else None


def configured_object_store(
    bucket: str,
    data_type: str,
    mode: Literal["r", "rw"] = "rw",
    skip_keys: list[str] | None = None,
    extend_keys: list[str] | None = None,
) -> ObjectStore[Any, Any] | None:
    """Construct the configured backend, or return None to select local storage.

    Factories use ``module:callable`` import paths and receive ``bucket``, ``data_type``,
    ``mode``, ``skip_keys``, ``extend_keys``, and configured ``options`` as keyword arguments.
    ``credentials_env`` maps additional argument names to environment-variable names;
    their values are read only when constructing the backend. Factory return values must
    support the ObjectStore interface and a reusable context manager. Backends own their
    connection and locking lifetimes, including datasources returned outside a context.
    An empty ``data_type`` selects store-level document operations only.

    Args:
        bucket: Configured backend location or direct local path.
        data_type: Data type, or an empty string for store-level documents.
        mode: Requested read or read/write access.
        skip_keys: Metadata keys to preserve during normalization.
        extend_keys: Metadata keys to merge as lists.

    Returns:
        Configured store, or None when the local implementation should be used.

    Raises:
        ConfigFileError: If the factory, options, or credential references are invalid.
        ObjectStoreError: If a URI has no configured factory or write access is disallowed.
        ValueError: If mode is not ``r`` or ``rw``.
    """
    if mode not in ("r", "rw"):
        raise ValueError("Object store mode must be 'r' or 'rw'.")

    config = get_object_store_config(bucket)
    if config is None or "factory" not in config:
        if "://" in str(bucket):
            raise ObjectStoreError("A non-local object store location requires a configured factory.")
        return None

    if mode == "rw" and "w" not in config.get("permissions", ""):
        raise ObjectStoreError("The configured object store does not allow writes.")
    if "r" not in config.get("permissions", ""):
        raise ObjectStoreError("The configured object store does not allow reads.")

    factory_path = config["factory"]
    if not isinstance(factory_path, str) or factory_path.count(":") != 1:
        raise ConfigFileError("Object store factory must use 'module:callable' syntax.")
    module_name, attribute_name = factory_path.split(":")
    if not module_name or not attribute_name:
        raise ConfigFileError("Object store factory must use 'module:callable' syntax.")

    options = config.get("options", {})
    credentials_env = config.get("credentials_env", {})
    if not isinstance(options, dict) or not isinstance(credentials_env, dict):
        raise ConfigFileError("Object store options and credentials_env must be TOML tables.")
    reserved = {"bucket", "data_type", "mode", "skip_keys", "extend_keys"}
    if reserved.intersection(options) or reserved.intersection(credentials_env):
        raise ConfigFileError("Object store options cannot override factory arguments.")
    if options.keys() & credentials_env.keys():
        raise ConfigFileError("Object store options and credentials_env must use distinct argument names.")

    kwargs = dict(options)
    for key, environment_name in credentials_env.items():
        if not isinstance(environment_name, str) or not environment_name:
            raise ConfigFileError("Each credentials_env value must name an environment variable.")
        try:
            kwargs[key] = os.environ[environment_name]
        except KeyError:
            raise ConfigFileError(
                f"Required credential environment variable {environment_name!r} is unset."
            ) from None

    try:
        factory = getattr(importlib.import_module(module_name), attribute_name)
    except (ImportError, AttributeError) as exc:
        raise ConfigFileError(f"Unable to import ObjectStore factory {factory_path!r}.") from exc
    if not callable(factory):
        raise ConfigFileError(f"ObjectStore factory {factory_path!r} is not callable.")

    store = factory(
        bucket=str(bucket),
        data_type=data_type,
        mode=mode,
        skip_keys=skip_keys,
        extend_keys=extend_keys,
        **kwargs,
    )
    if store is None:
        raise ConfigFileError("The configured ObjectStore factory returned None.")
    return cast("ObjectStore[Any, Any]", store)
