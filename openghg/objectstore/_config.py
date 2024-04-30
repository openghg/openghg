from dataclasses import dataclass
import importlib
import importlib.resources
from pathlib import Path
import toml
from typing import Dict, Union

import openghg.objectstore


# from openghg.util import timestamp_now
from openghg.types import ConfigFileError, ObjectStoreError


@dataclass(frozen=True)
class ObjectStoreConfig:
    store_path: Union[str, Path]
    metadata_keys: Dict[str, Dict]
    config_version: int


def _get_config_folderpath(bucket: str) -> Path:
    """Get the filepath of the config file for the object store

    Args:
        bucket: Object store bucket path
    Returns:
        Path: Path to config folder
    """
    return Path(bucket, "config")


def _get_metakeys_filepath(bucket: str) -> Path:
    """Get the path to the metakeys TOML file

    Args:
        bucket: Object store bucket path
    Returns:
        Path: Path to metakeys TOML
    """
    return _get_config_folderpath(bucket=bucket) / "db" / "metadata_keys.toml"


def get_metakey_defaults() -> Dict:
    """Return the dictionary of default values for the metadata keys

    Returns:
        dict: Dictionary of defaults
    """
    # We use importlib here to allow us to get the path of the file independent of how OpenGHG
    # is installed
    defaults_file = importlib.resources.files(openghg.objectstore) / "config" / "defaults.toml"
    defaults = toml.loads(defaults_file.read_text())
    return defaults


def create_default_config(bucket: str) -> None:
    """Creates the default configuration file for an object store

    Args:
        bucket: Object store bucket path
    Returns:
        None
    """
    # bucket = get_bucket(name=store)
    config_folderpath = _get_config_folderpath(bucket=bucket)
    if config_folderpath.exists():
        raise ObjectStoreError(f"config folder already exists at {config_folderpath}")

    config_folderpath.mkdir(parents=True)

    # Make the expected folder structure
    db_config_folderpath = config_folderpath.joinpath("db")
    db_config_folderpath.mkdir()

    default_keys = get_metakey_defaults()
    # Now we create the default metadata keys file and write out the defaults
    metadata_keys_filepath = _get_metakeys_filepath(bucket=bucket)
    metadata_keys_filepath.write_text(toml.dumps(default_keys))

    # # Write the version this config folder was created by
    # version_str = "1"
    # version_filepath = config_folderpath.joinpath("version.toml")
    # version_data = {"config_version": version_str, "file_created": str(timestamp_now())}
    # version_filepath.write_text(toml.dumps(version_data))


def get_metakeys(bucket: str) -> Dict:
    """Read the object store config

    Args:
        store: Store name
    Returns:
        dict: Configuration data
    """
    metakey_path = _get_metakeys_filepath(bucket=bucket)

    if not metakey_path.exists():
        raise ConfigFileError(f"Unable to read metadata keys config file in {bucket} at {metakey_path}")

    config = toml.loads(metakey_path.read_text())

    return config


def get_datatype_metakeys(bucket: str, data_type: str) -> Dict:
    """Return a list of the metakeys

    Args:
        data_type: Name of storage class
    Returns:
        list: List of keys
    """
    from openghg.store import data_class_info

    valid_classes = set(data_class_info())

    if data_type not in valid_classes:
        raise ValueError(f"{data_type} is not a valid storage class. Valid classes are: {valid_classes}")

    metakeys = get_metakeys(bucket=bucket)

    return metakeys[data_type]


def write_metakeys(bucket: str, metakeys: Dict) -> None:
    """Write metadata keys to file. The keys must contain data for each

    Args:
        store: Store name
        metakeys: Dictionary of keys
    Returns:
        None
    """
    from openghg.store import data_class_info

    # Quickly check we have keys for each of the storage classes
    storage_class_data = data_class_info()
    missing_keys = set(storage_class_data) - set(metakeys)

    if missing_keys:
        raise ValueError(
            "The metakeys dictionary must contain keys for each of the storage classes.\n"
            + f"We're missing: {missing_keys}"
        )

    metakey_path = _get_metakeys_filepath(bucket=bucket)
    metakey_path.write_text(toml.dumps(metakeys))
