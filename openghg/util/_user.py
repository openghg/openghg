import json
import logging
import os
import platform

# from functools import lru_cache
from pathlib import Path
from typing import Dict, Union
import uuid
import toml

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


def quickstart() -> None:
    """Get the user setup with a configuration file

    Returns:
        None
    """
    print("\nOpenGHG quickstart")
    config_path: Union[str, Path] = input(
        "Path for configuration file (default: ~/config/openghg/openghg.conf): "
    )

    if not config_path:
        config_path = get_user_config_path()
    else:
        config_path = Path(config_path)

    object_store_path: Union[str, None] = input("Path for object store (default: ~/openghg_store): ")

    if not object_store_path:
        object_store_path = None

    create_config(user_config_path=config_path, object_store_path=object_store_path)


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


def create_config(
    user_config_path: Union[Path, str, None] = None, object_store_path: Union[Path, str, None] = None
) -> None:
    """Creates a user config.

    Returns:
        None
    """
    if user_config_path is None:
        user_config_path = get_user_config_path()
    else:
        user_config_path = Path(user_config_path)

    # If the config file exists we might need to update it due to the introduction
    # of the user ID
    if user_config_path.exists():
        logger.info(f"User config exists at {str(user_config_path)}, checking..")

        config = toml.loads(user_config_path.read_text())

        object_store_path = Path(config["object_store"]["local_store"])

        # Some users may not have a user ID if they've used previous versions of OpenGHG
        try:
            user_id = config["user_id"]
        except KeyError:
            config["user_id"] = uuid.uuid4()
    else:
        logger.info(f"Creating config at {str(user_config_path)}")

        try:
            user_config_path.parent.mkdir(parents=True)
        except FileExistsError:
            pass

        if object_store_path is None:
            object_store_path = Path.home().joinpath("openghg_store").absolute()
        else:
            object_store_path = Path(object_store_path)

        user_id = uuid.uuid4()
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
