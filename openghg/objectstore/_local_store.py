import glob
import json
import os
import threading
from pathlib import Path
import shutil
from typing import Dict, List, Optional, Union
from uuid import uuid4
import logging
import pyvis
from openghg.types import ObjectStoreError
from openghg.util import read_local_config

rlock = threading.RLock()

__all__ = [
    "delete_object",
    "get_user_objectstore_path",
    "get_tutorial_store_path",
    "get_all_object_names",
    "get_object_names",
    "get_object",
    "set_object",
    "set_object_from_json",
    "set_object_from_file",
    "get_object_from_json",
    "exists",
    "visualise_store",
]

logger = logging.getLogger("openghg.objectstore")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def get_readable_buckets() -> Dict[str, str]:
    """Get a dictionary of readable buckets - {store_name: store_path, ...}

    Returns:
        dict: List of readable buckets
    """
    config = read_local_config()
    object_stores = config["object_store"]

    return {
        store_name: data["path"] for store_name, data in object_stores.items() if "r" in data["permissions"]
    }


def get_writable_buckets() -> Dict[str, str]:
    """Get a dictionary of writable buckets - {store_name: store_path, ...}

    Returns:
        dict: Dictionary of buckets this user can write to
    """
    config = read_local_config()
    object_stores = config["object_store"]

    return {
        store_name: data["path"] for store_name, data in object_stores.items() if "w" in data["permissions"]
    }


def get_writable_bucket(name: Optional[str] = None) -> str:
    """Get the path to a writable bucket, passing in the name of a bucket if
    more than one writable bucket available.

    Args:
        name: Name of writable bucket
    Returns:
        str: Path to writable bucket
    """
    if os.getenv("OPENGHG_TUT_STORE") is not None:
        return str(get_tutorial_store_path())

    writable_buckets = get_writable_buckets()

    if not writable_buckets:
        raise ObjectStoreError("No writable object stores found. Check configuration file.")

    if len(writable_buckets) == 1:
        return next(iter(writable_buckets.values()))
    elif name is not None:
        try:
            bucket_path = writable_buckets[name]
        except KeyError:
            raise ObjectStoreError(
                f"Invalid object store name, stores you can write to are: {', '.join(writable_buckets)}"
            )
        return bucket_path
    else:
        raise ObjectStoreError(
            f"More than one writable store, stores you can write to are: {', '.join(writable_buckets)}."
        )


def get_tutorial_store_path() -> Path:
    """Get the path to the local tutorial store

    Returns:
        pathlib.Path: Path of tutorial store
    """
    return get_user_objectstore_path() / "tutorial_store"


# @lru_cache
def get_user_objectstore_path() -> Path:
    """Returns the path of the user's local object store

    Returns:
        pathlib.Path: Path of object store
    """
    config = read_local_config()
    return Path(config["object_store"]["user"]["path"])


def get_objectstore_info() -> Dict:
    """Read the local config file and return the data of each of the object stores the
    user has access to.

    Returns:
        dict: Dictionary of object store data
    """
    config = read_local_config()
    return config["object_store"]


def get_all_object_names(bucket: str, prefix: Optional[str] = None, without_prefix: bool = False) -> List:
    """Returns the names of all objects in the passed bucket

    Args:
        bucket: Bucket path
        prefix: Prefix for keys
        without_prefix: If True don't use prefix
    Returns:
        list: List of object names
    """
    root = bucket

    if prefix is not None:
        root = f"{bucket}/{prefix}"

    root_len = len(bucket) + 1

    if without_prefix is not None:
        prefix_len = len(str(prefix))

    subdir_names = glob.glob(f"{root}*")

    object_names = []

    while True:
        names = subdir_names
        subdir_names = []

        for name in names:
            if name.endswith("._data"):
                # remove the  ._data at the end
                name = name[root_len:-6]
                while name.endswith("/"):
                    name = name[0:-1]

                if without_prefix:
                    name = name[prefix_len:]
                    while name.startswith("/"):
                        name = name[1:]

                if len(name) > 0:
                    object_names.append(name)
            elif os.path.isdir(name):
                subdir_names += glob.glob(f"{name}/*")

        if len(subdir_names) == 0:
            break

    return object_names


def delete_object(bucket: str, key: str) -> None:
    """Remove object at key in bucket

    Args:
        bucket: Bucket path
        key: Key to data in bucket
    Returns:
        None
    """
    key = f"{bucket}/{key}._data"
    try:
        os.remove(key)
    except FileNotFoundError:
        pass


def get_object_names(bucket: str, prefix: Optional[str] = None) -> List[str]:
    """List all the keys in the object store

    Args:
        bucket: Bucket containing data
    Returns:
        list: List of keys in object store
    """
    return get_all_object_names(bucket=bucket, prefix=prefix)


def get_object(bucket: str, key: str) -> bytes:
    """Gets the object at key in the passed bucket

    Args:
        bucket: Bucket containing data
        key: Key for data in bucket
    Returns:
        bytes: Binary data from the store
    """
    with rlock:
        filepath = Path(f"{bucket}/{key}._data")

        if filepath.exists():
            return filepath.read_bytes()
        else:
            raise ObjectStoreError(f"No object at key '{key}'")


def set_object(bucket: str, key: str, data: bytes) -> None:
    """Store data in bucket at key

    Args:
        bucket: Bucket path
        key: Key to store data in bucket
        data: Data in string form
    Returns:
        None
    """
    filename = f"{bucket}/{key}._data"

    with rlock:
        try:
            with open(filename, "wb") as f:
                f.write(data)
        except FileNotFoundError:
            dir = "/".join(filename.split("/")[0:-1])
            os.makedirs(dir, exist_ok=True)

            with open(filename, "wb") as f:
                f.write(data)


def set_object_from_json(bucket: str, key: str, data: Union[str, Dict]) -> None:
    """Set JSON data in the object store

    Args:
        bucket: Bucket for data storage
        key: Key for data in bucket
        data: JSON serialised data string
    Returns:
        None
    """
    data_bytes = json.dumps(data).encode("utf-8")

    set_object(bucket=bucket, key=key, data=data_bytes)


def set_object_from_file(bucket: str, key: str, filename: Union[str, Path]) -> None:
    """Set the contents of file at filename to key in bucket

    Args:
        bucket: Bucket path
        key: Key to for data
        filename (str, pathlib.Path): Filename/path
    Returns:
        None
    """
    set_object(bucket=bucket, key=key, data=open(filename, "rb").read())


def get_object_from_json(bucket: str, key: str) -> Dict[str, Union[str, Dict]]:
    """Return an object constructed from JSON stored at key.

    Args:
        bucket: Bucket containing data
        key: Key for data in bucket
    Returns:
        dict: Dictionary
    """
    data: Union[str, bytes] = get_object(bucket, key).decode("utf-8")
    data_dict: Dict = json.loads(data)

    return data_dict


def exists(bucket: str, key: str) -> bool:
    """Checks if there is an object in the object store with the given key

    Args:
        bucket: Bucket containing data
        key: Prefix for key in object store
    Returns:
        bool: True if key exists in store
    """
    names = get_all_object_names(bucket=bucket, prefix=key)

    return len(names) > 0


def get_bucket(name: Optional[str] = None) -> str:
    """Find and return the local object store path. This will return
    the path to the user's local object store if no name is given.

    Args:
        name: Object store name in config file
    Returns:
        str: Path to object store
    """
    config = read_local_config()

    if name is not None:
        try:
            return config["object_store"][name]["path"]
        except KeyError:
            raise ObjectStoreError("Invalid object store name.")

    tutorial_store = os.getenv("OPENGHG_TUT_STORE")

    if tutorial_store is not None:
        return str(get_tutorial_store_path())

    local_store = get_user_objectstore_path()

    return str(local_store)


def clear_object_store() -> None:
    """Delete the object store. This will only delete a local object store and not
    a group level or other store. You will be asked for input to confirm the path.

    Returns:
        None
    """
    local_store = str(get_user_objectstore_path())
    logger.warning(f"You have requested to delete {local_store}.")

    confirmed_path = input("Please enter the full path of the store: ")
    if confirmed_path == local_store:
        shutil.rmtree(local_store, ignore_errors=True)
    else:
        logger.warning("Cannot delete object store.")


def query_store() -> Dict:
    """Create a dictionary that can be used to visualise the object store

    Returns:
        dict: Dictionary for data to be shown in force graph
    """
    raise NotImplementedError
    from openghg.store import ObsSurface
    from openghg.store.base import Datasource

    obs = ObsSurface.load()

    datasource_uuids = obs.datasources()
    datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

    data = {}

    for d in datasources:
        metadata = d.metadata()
        result = {
            "site": metadata["site"],
            "species": metadata["species"],
            "instrument": metadata.get("instrument", "Unknown"),
            "network": metadata.get("network", "Unknown"),
            "inlet": metadata.get("inlet", "Unknown"),
        }
        data[d.uuid()] = result

    return data


def visualise_store() -> pyvis.network.Network:
    """View the object store using a pyvis force graph.

    This function should only be called from within a notebook

    Returns:
        pyvis.network.Network
    """
    raise NotImplementedError
    from addict import Dict as aDict

    data = query_store()

    net = pyvis.network.Network("800px", "100%", notebook=True)
    net.force_atlas_2based()

    # Create the ObsSurface node
    net.add_node(0, label="Surface Observations", color="#4e79a7", value=5000)

    network_split = aDict()

    for key, value in data.items():
        # Iterate over Datasources to select the networks
        network = value["network"]
        site = value["site"]
        inlet = value["inlet"]
        network_split[network][site][inlet][key] = value

    for network, sites in network_split.items():
        network_name = network.upper()
        net.add_node(network, label=network_name, color="#59a14f", value=2500)
        net.add_edge(source=0, to=network)

        # Then we want a subnode for each site
        for site, site_data in sites.items():
            # Don't want to use a site here as a site might be in multiple networks
            site_name = site.upper()
            site_id = str(uuid4())
            net.add_node(site_id, label=site_name, color="#e15759", value=1000)
            net.add_edge(source=network, to=site_id)

            for inlet, inlet_data in site_data.items():
                inlet_name = str(inlet).lower()
                inlet_id = str(uuid4())
                net.add_node(n_id=inlet_id, label=inlet_name, color="#808080", value=500)
                net.add_edge(source=site_id, to=inlet_id)

                # Now for each site create the datasource nodes
                for uid, datasource in inlet_data.items():
                    species = datasource["species"]
                    instrument = datasource["instrument"].upper()

                    label = f"{species.upper()} {instrument}"
                    title = "\n".join(
                        [
                            f"Site: {site.upper()}",
                            f"Species : {species.upper()}",
                            f"Instrument: {instrument}",
                        ]
                    )
                    net.add_node(n_id=uid, label=label, title=title, color="#f28e2b", value=100)
                    net.add_edge(source=inlet_id, to=uid)

    return net.show("openghg_objstore.html")
