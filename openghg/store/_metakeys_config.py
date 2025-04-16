import json
import logging
import importlib
from pprint import pformat
from pathlib import Path
from typing import cast

from openghg.types import ConfigFileError
from openghg.util import timestamp_now

logger = logging.getLogger("openghg.objectstore")
logger.setLevel(logging.INFO)  # Have to set level for logger as well as handlerF


def _get_custom_config_folderpath(bucket: str) -> Path:
    """Get the filepath of the config file for the object store

    Args:
        bucket: Object store bucket path
    Returns:
        Path: Path to config folder
    """
    return Path(bucket, "config")


def _get_custom_metakeys_filepath(bucket: str, previous: bool = False) -> Path:
    """Get the expected path from the metakeys JSON file
    within an object store.

    Args:
        bucket: Object store bucket path
        previous: Get previous name for the file (now deprecated)
    Returns:
        Path: Path to metakeys JSON
    """
    if previous:
        filename = "metadata_keys.json"
    else:
        filename = "metadata_keys_v2.json"
    return _get_custom_config_folderpath(bucket=bucket) / filename


def get_metakeys_defaults_filepath() -> Path:
    """Get path for the defaults metakeys file within openghg.

    Returns:
        Path: Path to default metakeys JSON
    """
    config_file_ref = importlib.resources.files("openghg") / "data/config/objectstore/defaults.json"

    with importlib.resources.as_file(config_file_ref) as f:
        config_file_path = f

    return config_file_path


def get_metakey_defaults() -> dict:
    """Return the dictionary of default values for the metadata keys.

    Returns:
        dict: Dictionary of defaults
    """
    config_filepath = get_metakeys_defaults_filepath()
    default_config = json.loads(config_filepath.read_text())
    return cast(dict, default_config)  # cast because we know this JSON file will product a dict


def check_metakeys(metakeys: dict) -> bool:
    """Checks the metakeys dictionary to ensure it contains all the required
    information

    Args:
        metakeys: Dictionary of metakeys
    Returns:
        bool: True if valid, else False
    Raises:
        ValueError if data missing
    """
    from openghg.store import data_class_info

    data_types = data_class_info()

    total_errors = {}
    missing_keys = set(data_types) - set(metakeys)
    if missing_keys:
        total_errors["FATAL"] = f"We require metakeys for each data type, we're missing: {missing_keys}"

    def _check_keys(_key_data: dict) -> list:
        errors = []
        for metakey, type_data in _key_data.items():
            try:
                types = type_data["type"]
            except KeyError:
                errors.append(f"Missing type data for {metakey}")
                continue

            if not types:
                errors.append(f"Missing types for {metakey}")

            if not isinstance(types, list):
                errors.append(f"The type(s) for {metakey} must be contained in a list")

            # TODO - in the future add in a check to ensure they're a type we
            # for key_type in types:
            # pass

        return errors

    for dtype, key_data in metakeys.items():
        dtype_errors = []
        try:
            required = key_data["required"]
        except KeyError:
            dtype_errors.append(
                "A number of required metakeys are required, please see example, skipping further checks."
            )
            continue

        errors_required = _check_keys(required)

        if errors_required:
            dtype_errors.append("Required key errors:")
            dtype_errors.extend(errors_required)

        if "optional" in key_data:
            errors_optional = _check_keys(key_data["optional"])
            if errors_optional:
                dtype_errors.append("Optional key errors:")
                dtype_errors.extend(errors_optional)

        if dtype_errors:
            total_errors[dtype] = dtype_errors  # type: ignore[assignment]

    if total_errors:
        logger.error("Errors found with metakeys:")
        logger.error(pformat(total_errors))
        return False

    return True


def create_custom_config(bucket: str) -> None:
    """Creates a copy of the default configuration file and puts this in
    an object store so this can be modified.

    Args:
        bucket: Object store bucket path
    Returns:
        None
    """
    config_folderpath = _get_custom_config_folderpath(bucket=bucket)

    config_folderpath.mkdir(parents=True, exist_ok=True)

    # Make the expected folder structure
    db_config_folderpath = config_folderpath.joinpath("config")
    db_config_folderpath.mkdir(exist_ok=True)

    default_keys = get_metakey_defaults()

    # Now we create the default metadata keys file and write out the defaults
    _write_metakey_config(bucket=bucket, metakeys=default_keys)


def _write_metakey_config(bucket: str, metakeys: dict) -> None:
    """Write the metakeys data to file, adding the version of OpenGHG
    it was written by and a timestamp.

    Args:
        bucket: Path to object store
        metakeys: Dictionary of metakeys data
    Returns:
        None
    """
    try:
        version = str(importlib.metadata.version("openghg"))
    except importlib.metadata.PackageNotFoundError:
        version = "UNKNOWN"

    config_data = {
        "openghg_version": version,
        "date_written": str(timestamp_now()),
        "metakeys": metakeys,
    }

    metakey_path = _get_custom_metakeys_filepath(bucket=bucket)
    config_folder = metakey_path.parent

    if not config_folder.exists():
        logger.debug(f"Creating folder at {config_folder}")
        config_folder.mkdir(parents=True, exist_ok=True)

    metakey_path.write_text(json.dumps(config_data))


def _get_metakeys_from_file(metakey_path: Path) -> dict:
    """Get the metakeys from a JSON file. Expect this will
    either contain 'metakeys' key containing the config data
    or the top level will contain this data directly.

    Args:
        metakey_path: Path to metakey JSON file
    Returns:
        dict: metakey details from the JSON file
    """
    config_data = json.loads(metakey_path.read_text())
    if "metakeys" in config_data:
        metakeys = config_data["metakeys"]
    else:
        metakeys = config_data

    if not isinstance(metakeys, dict):
        raise ValueError(f"Format of metakeys file {metakey_path} is invalid.")

    return metakeys


def get_metakeys(bucket: str | None = None) -> dict[str, dict]:
    """Read the metakeys. This will look for a custom
    config file within the object store and if one is not
    found this will use the defaults from within openghg.

    Args:
        bucket: Path to object store
    Returns:
        dict: Configuration data
    """
    if bucket is not None:
        metakey_path: Path | None = _get_custom_metakeys_filepath(bucket=bucket)
    else:
        metakey_path = None

    if metakey_path is None or not metakey_path.exists():
        metakey_path = get_metakeys_defaults_filepath()

    if bucket is not None:
        prev_metakey_path = _get_custom_metakeys_filepath(bucket=bucket, previous=False)
        if prev_metakey_path.exists():
            logger.warning(
                f"Previous config file: '{prev_metakey_path}' exists in the object store. This is now deprecated and so is no longer used."
            )

    metakeys = _get_metakeys_from_file(metakey_path)

    return metakeys


def write_metakeys(bucket: str, metakeys: dict) -> None:
    """Write metadata keys to file. The dictionary will be checked
    before writing and information on errros presented to the user.

    Args:
        bucket: Path to object store
        metakeys: Dictionary of keys
    Returns:
        None
    """
    if not check_metakeys(metakeys=metakeys):
        raise ConfigFileError("Invalid metakeys, see errors above.")

    _write_metakey_config(bucket=bucket, metakeys=metakeys)
