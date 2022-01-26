import glob
import json
import os
from pathlib import Path
import threading
from openghg.types import ObjectStoreError
import pyvis
from uuid import uuid4
from typing import Dict, List, Optional, Union

rlock = threading.RLock()

__all__ = [
    "delete_object",
    "get_openghg_local_path",
    "get_all_object_names",
    "get_object_names",
    "get_bucket",
    "get_local_bucket",
    "get_object",
    "set_object",
    "set_object_from_json",
    "set_object_from_file",
    "get_object_from_json",
    "exists",
    "visualise_store",
]


def get_openghg_local_path() -> Path:
    """Returns the path to the local OpenGHG object store bucket

    Returns:
        pathlib.Path: Path of object store
    """
    env_path = os.getenv("OPENGHG_PATH")

    if env_path is not None:
        return Path(env_path)
    else:
        raise ValueError(
            "No environment variable OPENGHG_PATH found, please set to use the local object store"
        )


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


def get_object_names(bucket: str, prefix: str = None) -> List[str]:
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
        Object: Object from store
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
    """Removes the daterange from the passed key and uses the reduced
    key to get an object from the object store.

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


def get_bucket() -> str:
    """Find and return a new bucket in the object store called
    'bucket_name'. If 'create_if_needed' is True
    then the bucket will be created if it doesn't exist. Otherwise,
    if the bucket does not exist then an exception will be raised.
    """
    bucket_path = get_openghg_local_path()

    if not bucket_path.exists():
        bucket_path.mkdir(parents=True)

    return str(bucket_path)


def get_local_bucket(empty: bool = False) -> str:
    """Creates and returns a local bucket

    Args:
        empty: If True return an empty bucket
    Returns:
        str: Path to local bucket
    """
    import shutil

    local_buckets_dir = get_openghg_local_path()

    if local_buckets_dir.exists():
        if empty is True:
            shutil.rmtree(local_buckets_dir)
            local_buckets_dir.mkdir(parents=True)
    else:
        local_buckets_dir.mkdir(parents=True)

    return str(local_buckets_dir)


def query_store() -> Dict:
    """Create a dictionary that can be used to visualise the object store

    Returns:
        dict: Dictionary for data to be shown in force graph
    """
    from openghg.store.base import Datasource
    from openghg.store import ObsSurface

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
