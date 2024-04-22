from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Union

import toml
from openghg.objectstore import get_bucket
from openghg.util import timestamp_now
from openghg.types import ConfigFileError, ObjectStoreError


@dataclass(frozen=True)
class ObjectStoreConfig:
    store_path: Union[str, Path]
    metadata_keys: Dict[str, Dict]
    config_version: int


def _get_config_folderpath(store: str) -> Path:
    """Get the filepath of the config file for the object store

    Args:
        store: Store name

    """
    store_path = get_bucket(name=store)
    return Path(store_path, "config")


def create_default_config(store: str) -> None:
    """Creates the default configuration file for an object store

    Args:
        store: Store name
    Returns:
        None
    """
    config_folderpath = _get_config_folderpath(store=store)
    if config_folderpath.exists():
        raise ObjectStoreError(f"config folder already exists at {config_folderpath}")

    config_folderpath.mkdir(parents=True)

    # Make the expected folder structure
    db_config_folderpath = config_folderpath.joinpath("db")
    db_config_folderpath.mkdir()

    metadata_keys_filepath = db_config_folderpath.joinpath("metadata_keys.toml")

    # TODO - where to store these keys?

    defaults = {
        "default_metakeys": {
            "boundary_conditions": ["species", "bc_input", "domain"],
            "eulerian_model": ["model", "species", "date"],
            # QUESTION - do we want all these keys here? The last three were from optional_keywords
            "flux": ["species", "source", "domain", "database", "database_version", "model"],
            "footprints": [
                "site",
                "model",
                "inlet",
                "domain",
                "high_time_resolution",
                "high_spatial_resolution",
                "short_lifetime",
                "species",
                "met_model",
            ],
            "obscolumn": ["satellite", "selection", "domain", "site", "species", "network"],
            "obssurface": [
                "species",
                "site",
                "sampling_period",
                "station_long_name",
                "inlet",
                "instrument",
                "network",
                "source_format",
                "data_source",
                "icos_data_level",
                "data_type",
            ],
        }
    }

    # Write the version this config folder was created by
    version_str = "1"
    version_filepath = config_folderpath.joinpath("version.toml")
    version_data = {"config_version": version_str, "file_created": str(timestamp_now())}
    version_filepath.write_text(toml.dumps(version_data))


def read_config(store: str) -> Dict:
    """Read the object store config

    Args:
        store: Store name
    Returns:
        dict: Configuration data
    """
    bucket = get_bucket(name=store)

    config_folder = Path(bucket, "config")
    config_filepath = Path(config_folder, "db", "metadata_keys.toml")

    if not config_filepath.exists():
        raise ConfigFileError(
            f"Unable to read metadata keys config file for store {store} at {config_filepath}"
        )

    config = toml.loads(config_filepath.read_text())

    return config
