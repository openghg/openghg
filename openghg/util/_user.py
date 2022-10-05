import json
import logging
import os
import platform

# from functools import lru_cache
from pathlib import Path
from typing import Dict

import toml

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


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


def create_default_config() -> None:
    """Creates a default user config in the user's home directory.

    Returns:
        None
    """
    user_config_path = get_user_config_path()

    if user_config_path.exists():
        logger.info(f"User config already exists at {str(user_config_path)}")
        return None
    else:
        logger.info(f"Creating config file at {str(user_config_path)}")

        try:
            user_config_path.parent.mkdir(parents=True)
        except FileExistsError:
            pass

        object_store_path = Path.home().joinpath("openghg_store").absolute()

        try:
            object_store_path.mkdir()
        except FileExistsError:
            pass

        config = {"object_store": {"local_store": str(object_store_path)}}

        toml_str = toml.dumps(config)

        user_config_path.write_text(toml_str)


# @lru_cache
def read_local_config() -> Dict:
    """Reads the local config file.

    Returns:
        dict: OpenGHG configurations
    """
    config_path = get_user_config_path()
    config: Dict = toml.loads(config_path.read_text())
    return config


def create_user_config() -> None:
    """Guides the user through setting up a config file for local users

    Returns:
        None
    """
    raise NotImplementedError
    main_store = input("Please enter the path to the main object store: ")
    main_store = Path(main_store).resolve()

    if not main_store.exists():
        print("Cannot find object store, please ensure path is correct.")

    user_store = input("Please enter a path to your own object store: ")
    user_store = Path(user_store).resolve()

    if not user_store.exists():
        create_obj_store = input(f"Create {str(user_store)} (y/n)?: ")
        if create_obj_store.lower() in ("y", "yes"):
            user_store.mkdir()
        else:
            print(f"Unable to proceed, please create the {str(user_store)} folder.")

    user_config = {}
    user_config["main_store"] = str(main_store)
    user_config["user_store"] = str(user_store)

    openghg_config_filename = "openghg.conf"

    _platform = platform.system()

    if _platform == "Windows":
        config_path = Path(os.getenv("LOCALAPPDATA")).joinpath(openghg_config_filename)
    elif _platform in ("Linux", "Darwin"):
        config_folder = Path.home().joinpath(".config")
        if not config_folder.exists():
            config_folder.mkdir()

        config_path = config_folder.joinpath(openghg_config_filename)

    write_config = input(f"Write config file to {str(config_path)}? (y/n): ")
    if write_config.lower() in ("y", "yes"):
        config_path.write_text(json.dumps(user_config))
    else:
        print("No config file written.")

    return None
