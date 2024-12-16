import logging
import os
import platform
from pathlib import Path
import uuid
import toml
import shutil
from openghg.types import ConfigFileError

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

openghg_config_filename = "openghg.conf"


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
    """Returns path to user config file.

    This file is created in the user's home directory
    in  ~/.ghgconfig/openghg/user.conf on Linux / macOS or
    in LOCALAPPDATA/openghg/openghg.conf on Windows.

    Returns:
        pathlib.Path: Path to user config file
    """
    user_platform = platform.system()

    if user_platform == "Windows":
        appdata_path = os.getenv("LOCALAPPDATA")
        if appdata_path is None:
            raise ValueError("Unable to read LOCALAPPDATA environment variable.")

        config_path = Path(appdata_path).joinpath("openghg", openghg_config_filename)
    elif user_platform in ("Linux", "Darwin"):
        config_path = Path.home().joinpath(".openghg", openghg_config_filename)
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

    # Current config version as of version 0.6.0
    config_version = "2"
    positive_responses = ("y", "yes")

    # If the config file exists we might need to update it due to the introduction
    # of the user ID and new object store path handling for multiple stores
    if user_config_path.exists():
        if silent:
            logger.error("Cannot update an existing configuration silently. Please run interactively.")
            return

        logger.info(f"User config exists at {str(user_config_path)}, checking...")

        config = toml.loads(user_config_path.read_text())

        recent = "config_version" in config

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

            existing_paths = [path_data["path"] for path_data in stores.values()]

            # Here it checks if newly entered paths are already present in config file.
            duplicate_paths_with_store_name = {
                store_name: path_data
                for store_name, path_data in new_shared_stores.items()
                if path_data["path"] in existing_paths
            }

            if existing:
                print(f"Some names match those of existing stores: {existing}, please update manually.")

            if duplicate_paths_with_store_name:
                raise ValueError(
                    f"Paths of the following new stores match those of existing store: {duplicate_paths_with_store_name}"
                )

            stores.update(new_shared_stores)

        # Some users may not have a user ID if they've used previous versions of OpenGHG
        user_id = config.get("user_id")
        config = _combine_config(config_version=config_version, object_stores=stores, user_id=user_id)
    else:
        # Let's try migrating the old config
        # If it works we call this function again
        # otherwise continue on to create a new config
        try:
            _migrate_config()
        except FileNotFoundError:
            pass
        else:
            create_config(silent=silent)
            return

        stores = {}

        # 1. Create the user's object store first
        if not silent:
            logger.info("We'll first create your user object store.\n")

        obj_store_path = get_default_objectstore_path()

        if silent:
            obj_store_path = get_default_objectstore_path()
        else:
            obj_store_input = input(f"Enter path for your local object store (default {obj_store_path}): ")
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


def _user_multstore_input() -> dict:
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


def _combine_config(config_version: str, object_stores: dict, user_id: str | None = None) -> dict:
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


# @lru_cache
def read_local_config() -> dict:
    """Reads the local config file.

    Returns:
        dict: OpenGHG configurations
    """
    config_path = get_user_config_path()

    if not config_path.exists():
        try:
            _migrate_config()
        except FileNotFoundError as e:
            raise ConfigFileError(
                "Unable to read configuration file, please see the installation instructions \
                or run openghg --quickstart"
            ) from e

    config: dict = toml.loads(config_path.read_text())

    try:
        _ = config["object_store"]["user"]
    except KeyError:
        raise ConfigFileError(
            "Invalid config file detected, please please see the installation instructions \
                or run openghg --quickstart"
        )

    # Check see is the store uses the new zarr storage format
    # for OpenGHG >= 0.8.0
    valid_stores = {}
    for name, store_data in config["object_store"].items():
        store_path = Path(store_data["path"])
        # If it doesn't exist or its empty then we expect it to be created / populated
        if not store_path.exists() or not any(store_path.iterdir()):
            valid_stores[name] = store_data
        # Otherwise we check an existing store to see if it's the correct format
        else:
            if _check_valid_store(store_path):
                valid_stores[name] = store_data
            else:
                logger.warning(
                    f"Object store {name} does not use the new Zarr storage format and will be ignored."
                )

    if not valid_stores:
        raise ConfigFileError(
            "We've only detected old (non-Zarr) object stores. Please update your configuration to add a new path."
        )

    config["object_store"] = valid_stores

    return config


def check_config() -> None:
    """Check that the user config file is valid and the paths
    given in it exist. Raises ConfigFileError if problems found.

    Returns:
        None
    """
    config_path = get_user_config_path()
    please_update = "please run openghg --quickstart to update it."

    if not config_path.exists():
        raise ConfigFileError(
            "Configuration file does not exist. Please create it by running openghg --quickstart."
        )

    config = read_local_config()
    try:
        uid = config["user_id"]
    except KeyError:
        raise ConfigFileError("Unable to read user ID, ")

    try:
        uuid.UUID(uid, version=4)
    except ValueError:
        raise ConfigFileError(f"Invalid user ID, {please_update}")

    try:
        _ = config["config_version"]
    except KeyError:
        raise ConfigFileError(f"Invalid config file, {please_update}")

    try:
        object_stores = config["object_store"]
    except KeyError:
        raise ConfigFileError(f"Unable to read object store data, {please_update}")

    for name, data in object_stores.items():
        p = Path(data["path"])
        if not p.exists():
            logger.info(f"The path for object store {name} at {p} does not exist but will be created.")


def _migrate_config() -> None:
    """If user config file is in ~/.config, move it to ~/.openghg.

    If no config is found in ~/.config or system is Windows, raise FileNotFoundError.

    Returns:
        None
    """
    old_config_path = Path.home().joinpath(".config", "openghg", openghg_config_filename)

    if old_config_path.exists():
        new_config_path = get_user_config_path()
        new_config_path.parent.mkdir(parents=True)
        shutil.move(str(old_config_path), str(new_config_path))
        logger.info(f"Moved user config file from {str(old_config_path)} to {str(new_config_path)}.")
        shutil.rmtree(old_config_path.parent)  # remove "openghg" dir from ~/.config
    else:
        raise FileNotFoundError("Configuration file not found.")


def _check_valid_store(store_path: Path) -> bool:
    """Checks if the store is a valid object store using the new Zarr storage
    format. If it is return True, otherwise False.

    TODO - remove this when users have all moved to the new object store format.

    Args:
        store_path: Object store path
    Returns:
        bool: True if valid, False if not
    """
    data_dir = Path(store_path).joinpath("data")
    # Now check if there's a zarr folder in the data directory
    store_dirs = list(data_dir.glob("*"))
    # Let's take the first data directory and see if there's a zarr folder in it
    if not store_dirs:
        logger.info(
            f"No data found in the object store {store_path}, "
            "so we are treating this empty store as a zarr store."
        )
        return True

    store_data_dir = store_dirs[0]

    return store_data_dir.joinpath("zarr").exists()
