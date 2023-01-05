import argparse
import logging
import os
import platform

# from functools import lru_cache
from pathlib import Path
from typing import Dict, Union
import uuid
import toml
from openghg.util import versions

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


# @lru_cache
def get_user_id() -> str:
    """Return the user's ID

    Returns:
        str: User ID
    """
    config = read_local_config()
    uid = str(config.get("user_id", "NA"))
    return uid


def default_objectstore_path() -> Path:
    """Returns the default object store path in the user's home directory

    Returns:
        Path: Object store path in ~/openghg_store
    """
    return Path.home().joinpath("openghg_store").absolute()


# @lru_cache
def get_user_config_path() -> Path:
    """Checks if a config file has already been create for
    OpenGHG to use. This file is created in the user's home directory
    in  ~/.config/openghg/user.conf on Linux / macOS or
    in LOCALAPPDATA/openghg/openghg.conf on Windows.

    Returns:
        pathlib.Path: Path to user config file
    """
    user_platform = platform.system()

    openghg_config_filename = "openghg.conf"

    if user_platform == "Windows":
        appdata_path = os.getenv("LOCALAPPDATA")
        if appdata_path is None:
            raise ValueError("Unable to read LOCALAPPDATA environment variable.")

        config_path = Path(appdata_path).joinpath("openghg", openghg_config_filename)
    elif user_platform in ("Linux", "Darwin"):
        config_path = Path.home().joinpath(".config", "openghg", openghg_config_filename)
    else:
        raise ValueError(f"Unknown platform: {user_platform}")

    return config_path


def create_config(silent: bool = False) -> None:
    """Creates a user config.

    Returns:
        None
    """

    default_objstore_path = default_objectstore_path()

    object_store_path: Union[str, Path] = input(
        f"\nPlease enter a path for the object store (default: {default_objstore_path}): "
    )

    if object_store_path:
        object_store_path = Path(object_store_path)
    else:
        object_store_path = default_objstore_path

    user_config_path = get_user_config_path()

    # If the config file exists we might need to update it due to the introduction
    # of the user ID
    if user_config_path.exists():
        print(f"User config exists at {str(user_config_path)}, checking...")

        config = toml.loads(user_config_path.read_text())

        objstore_path_config = Path(config["object_store"]["local_store"])

        if objstore_path_config != object_store_path:
            config_input = input(
                "Would you like to update the object store path from:"
                + f"\n{objstore_path_config}\n"
                + "to"
                + f"\n{object_store_path}?"
            )

            if config_input.lower() in ("y", "yes"):
                config["object_store"]["local_store"] = object_store_path
        else:
            print("Matching object store path, nothing to do.\n")

        # Some users may not have a user ID if they've used previous versions of OpenGHG
        try:
            user_id = config["user_id"]
        except KeyError:
            config["user_id"] = str(uuid.uuid4())
    else:
        print(f"Creating config at {str(user_config_path)}")

        user_config_path.parent.mkdir(parents=True, exist_ok=True)

        if object_store_path is None:
            object_store_path = default_objectstore_path()
        else:
            object_store_path = Path(object_store_path)

        user_id = str(uuid.uuid4())
        config = {"object_store": {"local_store": str(object_store_path)}, "user_id": user_id}

    object_store_path.mkdir(exist_ok=True)
    user_config_path.write_text(toml.dumps(config))


# @lru_cache
def read_local_config() -> Dict:
    """Reads the local config file.

    Returns:
        dict: OpenGHG configurations
    """
    config_path = get_user_config_path()

    if not config_path.exists():
        raise FileNotFoundError(
            "Unable to read configuration file, please see the installation instructions or run openghg --quickstart"
        )

    config: Dict = toml.loads(config_path.read_text())
    return config
