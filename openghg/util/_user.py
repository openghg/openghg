import logging
import os
import platform
from pathlib import Path
import pprint
from typing import Dict
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


def get_default_objectstore_path() -> Path:
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
    print("\nOpenGHG configuration")
    print("---------------------\n")

    user_config_path = get_user_config_path()

    updated = False
    # If the config file exists we might need to update it due to the introduction
    # of the user ID
    if user_config_path.exists():
        if silent:
            print("Error: cannot overwrite an existing configuration. Please run quickstart.")
            return

        print(f"User config exists at {str(user_config_path)}, checking...")

        config = toml.loads(user_config_path.read_text())

        objstore_path_config = Path(config["object_store"]["local_store"])

        print(f"Current object store path: {objstore_path_config}")
        update_input = input("Would you like to update the path? (y/n): ")
        if update_input.lower() in ("y", "yes"):
            new_path_input = input("Enter new path for object store: ")
            new_path = Path(new_path_input).expanduser().resolve()

            config["object_store"]["local_store"] = str(new_path)
            updated = True
        else:
            print("Matching object store path, nothing to do.\n")

        # Some users may not have a user ID if they've used previous versions of OpenGHG
        # Or if they UUID isn't valid a new one is created, the value of the UUID
        # doesn't matter at the moment, it's not used for anything very important
        try:
            user_id = config["user_id"]
            uuid.UUID(user_id, version=4)
        except (KeyError, ValueError):
            config["user_id"] = str(uuid.uuid4())
            updated = True

        if updated:
            print("Updated configuration saved.\n")
    else:
        default_objstore_path = get_default_objectstore_path()

        if silent:
            obj_store_path = default_objstore_path
        else:
            obj_store_input = input(f"Enter path for object store (default {default_objstore_path}): ")
            obj_store_path = Path(obj_store_input).expanduser().resolve()

        user_config_path.parent.mkdir(parents=True, exist_ok=True)

        if not obj_store_path:
            obj_store_path = default_objstore_path

        user_id = str(uuid.uuid4())
        config = {"object_store": {"local_store": str(obj_store_path)}, "user_id": user_id}

        obj_store_path.mkdir(exist_ok=True)

        print(f"Creating config at {str(user_config_path)}\n")

    if updated:
        pp = pprint.PrettyPrinter(width=50, compact=True)
        print("Writing configuration:\n")
        pp.pprint(config)
        print("\n")

        user_config_path.write_text(toml.dumps(config))
    else:
        print("Configuration unchanged.")


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


def check_config() -> None:
    """Check that the user config file is valid and the paths
    given in it exist.

    Returns:
        bool
    """
    config_path = get_user_config_path()

    if not config_path.exists():
        logger.warning("Configuration file does not exist.")
        create_config()

    config = read_local_config()
    uid = config["user_id"]
    object_stores = config["object_store"]

    try:
        uuid.UUID(uid, version=4)
    except ValueError:
        logger.error("Invalid user ID. Please re-run quickstart to setup a valid config file.")

    for path in object_stores.values():
        if not Path(path).exists():
            logger.info(f"{path} does not exist but will be created.")
