import logging
import os
import platform
from pathlib import Path
from typing import Dict, Optional
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

    Args:
        silent: Creates the basic configuration file with only
        the user's object store in a default location.
    Returns:
        None
    """
    if not silent:
        print("\nOpenGHG configuration")
        print("---------------------\n")

    user_config_path = get_user_config_path()

    # Current config version as of version 0.5.1
    config_version = "2"
    positive_responses = ("y", "yes")

    # If the config file exists we might need to update it due to the introduction
    # of the user ID and new object store path handling for multiple stores
    if user_config_path.exists():
        if silent:
            logger.error("Error: cannot overwrite an existing configuration. Please run interactively.")
            return

        logger.info(f"User config exists at {str(user_config_path)}, checking...")

        config = toml.loads(user_config_path.read_text())

        recent = False
        try:
            _ = config["config_version"]
            recent = True
        except KeyError:
            pass

        if recent:
            user_store_path = Path(config["object_store"]["user"]["path"])
        else:
            user_store_path = Path(config["object_store"]["local_store"])

        logger.info(f"Current user object store path: {user_store_path}")

        # Store the object store info
        stores = {}

        update_input = input("Would you like to update the path? (y/n): ")
        if update_input.lower() in positive_responses:
            new_path_input = input("Enter new path for object store: ")

            if not new_path_input:
                print("You must enter a path. Unable to complete config setup.")
                return

            new_path = Path(new_path_input).expanduser().resolve()
            stores["user"] = {"path": str(new_path), "permissions": "rw"}
        else:
            stores["user"] = {"path": str(user_store_path), "permissions": "rw"}

        # Copy in exisiting shared stores
        if recent:
            stores.update({k: v for k, v in config["object_store"].items() if k != "user"})

        # Now ask the user if they want to add new stores
        new_shared_stores = _user_multstore_input()

        if new_shared_stores:
            existing = [k for k in new_shared_stores if k in stores]
            if existing:
                print(f"Some names match those of existing stores: {existing}, please update manually.")

            stores.update(new_shared_stores)

        # Some users may not have a user ID if they've used previous versions of OpenGHG
        user_id = config.get("user_id")
        config = _combine_config(config_version=config_version, object_stores=stores, user_id=user_id)
    else:
        stores = {}

        # 1. Create the user's object store first
        if not silent:
            logger.info("We'll first create your user object store.\n")

        obj_store_path = get_default_objectstore_path()

        if silent:
            obj_store_path = get_default_objectstore_path()
        else:
            obj_store_input = input(f"Enter path for object store (default {obj_store_path}): ")
            if obj_store_input:
                obj_store_path = Path(obj_store_input).expanduser().resolve()

        # Let's create the store to make sure it's a valid path
        obj_store_path.mkdir(parents=True, exist_ok=True)

        stores["user"] = {"path": str(obj_store_path), "permissions": "rw"}

        if not silent:
            shared_stores = _user_multstore_input()
            stores.update(shared_stores)

        config = _combine_config(config_version=config_version, object_stores=stores)

    # Make the .config/openghg folder
    user_config_path.parent.mkdir(parents=True, exist_ok=True)

    if not silent:
        logger.info(f"Configuration written to {user_config_path}")

    user_config_path.write_text(toml.dumps(config))


def _user_multstore_input() -> Dict:
    """Ask the user to input data about shared object stores

    Returns:
        dict: Dictionary of object store paths and permissions
    """
    positive_responses = ("y", "yes")
    stores = {}
    # 2. Ask the user to enter other object store paths
    while True:
        response = input("Would you like to add another object store? (y/n): ")
        if response.lower() in positive_responses:
            store_name = input("Enter the name of the store: ")
            store_path = input("Enter the object store path: ")
            print("\nYou will now be asked for read/write permissions for the store.")
            print("For read only enter r, for read and write enter rw.")

            store_permissions = ""
            while store_permissions not in ("r", "rw"):
                store_permissions = input("\nEnter object store permissions: ")

            stores[store_name] = {"path": store_path, "permissions": store_permissions}
        else:
            break

    return stores


def _combine_config(config_version: str, object_stores: Dict, user_id: Optional[str] = None) -> Dict:
    """Combine parts required into the proper dictionary format

    Args:
        config_version: Configuration version number
        object_stores: Object store configuration dictionary
        user_id: User ID
    """
    # Create the object store dictionary
    object_store_info = {}
    for name, data in object_stores.items():
        path = str(Path(data["path"]).expanduser().resolve())
        permissions = data["permissions"].strip()

        object_store_info[name] = {"path": path, "permissions": permissions}

    if user_id is None:
        user_id = str(uuid.uuid4())

    return {"user_id": user_id, "config_version": config_version, "object_store": object_store_info}


def update_config_file(path: Path):
    """Update the user's configuration file

    Returns:
        None
    """
    raise NotImplementedError


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


def check_config() -> bool:
    """Check that the user config file is valid and the paths
    given in it exist.

    Returns:
        bool: True if config OK, else False
    """
    valid = True

    config_path = get_user_config_path()

    if not config_path.exists():
        logger.warning("Configuration file does not exist. Please create it by running openghg --quickstart.")
        valid = False

    config = read_local_config()
    uid = config["user_id"]
    config_version = config.get("config_version")
    if config_version is None:
        logger.warn(
            "Your configuration file is not in the latest format, please run openghg --quickstart to update it."
        )
        valid = False

    object_stores = config["object_store"]

    try:
        uuid.UUID(uid, version=4)
    except ValueError:
        valid = False
        logger.warn("Invalid user ID, please run openghg --quickstart to update it.")

    for name, data in object_stores.items():
        p = Path(data["path"])
        if not p.exists():
            logger.info(f"The path for object store {name} at {p} does not exist but will be created.")

    return valid
