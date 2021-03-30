import glob
import json
import os
from pathlib import Path
import threading
from Acquire.ObjectStore import ObjectStoreError
import pyvis
from collections import defaultdict
from uuid import uuid4

rlock = threading.RLock()

__all__ = ["delete_object", 
            "get_hugs_local_path", 
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
            "visualise_store"]


def get_hugs_local_path():
    """ Returns the path to the local OpenGHG object store bucket

        Returns:
            pathlib.Path
    """
    env_path = os.getenv("OPENGHG_PATH")

    if env_path:
        return Path(env_path)
    else:
        raise ValueError("No environment variable OPENGHG_PATH found, please set to use the local object store")
    # return Path("/tmp/hugs_local")


def get_all_object_names(bucket, prefix=None, without_prefix=False):
    """ Returns the names of all objects in the passed bucket

        Args:
            bucket (str): Bucket path
            prefix (str, default=None): Prefix for keys
            withot_prefix (bool, default=False)
        Returns:
            list: List of object names
    """
    root = bucket

    if prefix is not None:
        root = f"{bucket}/{prefix}"

    root_len = len(bucket) + 1

    if without_prefix:
        prefix_len = len(prefix)

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


def delete_object(bucket, key):
    """ Remove object at key in bucket

        Args:
            bucket (str): Bucket path
            key (str): Key to data in bucket
        Returns:
            None
    """
    key = f"{bucket}/{key}._data"
    try:
        os.remove(key)
    except FileNotFoundError:
        pass


def get_object_names(bucket, prefix=None):
    """ List all the keys in the object store

        Args:
            bucket (str): Bucket containing data
        Returns:
            list: List of keys in object store
    """
    return get_all_object_names(bucket=bucket, prefix=prefix)


def get_object(bucket, key):
    """ Gets the object at key in the passed bucket

        Args:
            bucket (str): Bucket containing data
            key (str): Key for data in bucket
        Returns:
            Object: Object from store
    """
    with rlock:
        filepath = Path(f"{bucket}/{key}._data")

        if filepath.exists():
            return filepath.read_bytes()
        else:
            raise ObjectStoreError(f"No object at key '{key}'")


def set_object(bucket, key, data):
    """ Store data in bucket at key

        Args:
            bucket (str): Bucket path
            key (str): Key to store data in bucket
            data (str): Data in string form
        Returns:
            None
    """
    filename = f"{bucket}/{key}._data"

    with rlock:
        try:
            with open(filename, 'wb') as f:
                f.write(data)
        except FileNotFoundError:
            dir = "/".join(filename.split("/")[0:-1])
            os.makedirs(dir, exist_ok=True)

            with open(filename, 'wb') as f:
                f.write(data)


def set_object_from_json(bucket, key, data):
    """ Set JSON data in the object store 

        Args:
            bucket (str): Bucket for data storage
            key (str): Key for data in bucket
            data (str): JSON serialised data string
        Returns:
            None
    """
    data = json.dumps(data).encode("utf-8")

    set_object(bucket=bucket, key=key, data=data)


def set_object_from_file(bucket, key, filename):
    """ Set the contents of file at filename to key in bucket

        Args:
            bucket (str): Bucket path
            key (str): Key to for data
            filename (str, pathlib.Path): Filename/path
        Returns:
            None
    """
    set_object(bucket=bucket, key=key, data=open(filename, "rb").read())


def get_object_from_json(bucket, key):
    """ Removes the daterange from the passed key and uses the reduced
        key to get an object from the object store.

        Args:
            bucket (str): Bucket containing data
            key (str): Key for data in bucket
        Returns:
            Object: Object created from data
    """
    data = get_object(bucket, key).decode("utf-8")

    return json.loads(data)


def exists(bucket, key):
    """ Checks if there is an object in the object store with the given key

        Args:
            bucket (dict): Bucket containing data
            key (str): Prefix for key in object store
        Returns:
            bool: True if key exists in store
    """
    names = get_all_object_names(bucket, prefix=key)

    return len(names) > 0


def get_bucket():
    """Find and return a new bucket in the object store called
        'bucket_name'. If 'create_if_needed' is True
        then the bucket will be created if it doesn't exist. Otherwise,
        if the bucket does not exist then an exception will be raised.
    """
    bucket_path = get_hugs_local_path()

    if not bucket_path.exists():
        bucket_path.mkdir(parents=True)

    return str(bucket_path)


def get_local_bucket(empty=False):
    """ Creates and returns a local bucket that's created in the
        /tmp/hugs_test directory

        Args:
            empty (bool, default=False): If True return an empty bucket
        Returns:
            str: Path to local bucket
    """
    import shutil

    local_buckets_dir = get_hugs_local_path()

    if local_buckets_dir.exists():
        if empty is True:
            shutil.rmtree(local_buckets_dir)
            local_buckets_dir.mkdir(parents=True)
    else:
        local_buckets_dir.mkdir(parents=True)

    return str(local_buckets_dir)


def query_store():
    """ Create a dictionary that can be used to visualise the object store 

        Returns:
            dict: Dictionary for data to be shown in force graph 
    """
    from openghg.modules import Datasource, ObsSurface

    obs = ObsSurface.load()

    datasource_uuids = obs.datasources()
    datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

    data = {d.uuid(): d.metadata() for d in datasources}

    return data


def visualise_store() -> pyvis.network.Network:
    """ View the object store using a pyvis force graph.

        This function should only be called from within a notebook

        Returns:
            pyvis.network.Network
    """
    data = query_store()

    net = pyvis.network.Network("800px", "100%", notebook=True, heading="OpenGHG Object Store")
    net.force_atlas_2based()

    # Create the ObsSurface node
    net.add_node(0, label="ObsSurface", color="#4e79a7", value=5000)

    # We want to created a nested dictionary
    def nested_dict():
        return defaultdict(nested_dict)

    network_split = nested_dict()

    for key, value in data.items():
        # Iterate over Datasources to select the networks etc
        network = value["network"]
        site = value["site"]
        instrument = value["instrument"]

        # These should be standardised so we always have inlet but check
        # This type of visualisation will only really work with obs data 
        # as other data types won't have a fixed inlet height.
        try:
            inlet = value["inlet"]
        except KeyError:
            try:
                inlet = value["inlet_height_magl"]
            except KeyError:
                inlet = "Unknown Inlet"

        network_split[network][site][instrument][inlet][key] = value

    for network, sites in network_split.items():
        network_name = network.upper()
        net.add_node(network, label=network_name, color="#59a14f", value=2500)
        net.add_edge(source=0, to=network)

        # Then we want a subnode for each site  
        for site, instrument in sites.items():
            # Don't want to use a site here as a site might be in multiple networks
            site_name = site.upper()
            site_id = str(uuid4())
            net.add_node(site_id, label=site_name, color="#e15759", value=1000)
            net.add_edge(source=network, to=site_id)

            for inst, inlets in instrument.items():
                inst_id = f"{inst}_{site}"
                net.add_node(inst_id, label=inst, value=333)
                net.add_edge(source=site_id, to=inst_id)

                # Now we want to check for multiple heights
                for inlet, data in inlets.items():
                    inlet_id = f"{inlet}_{inst}_{site}"
                    label = inlet.upper()
                    net.add_node(inlet_id, label=inlet, value=200, color="#B07AA2")
                    net.add_edge(source=inst_id, to=inlet_id)

                    # Now for each site create the datasource nodes
                    for uid, datasource in data.items():
                        species = datasource["species"]
                        instrument = datasource["instrument"].upper()

                        label = species.upper()
                        title = "\n".join([f"Site: {site.upper()}", f"Species : {species.upper()}", f"Instrument: {instrument}"])
                        net.add_node(uid, label=label, title=title, color="#f28e2b", value=100)
                        net.add_edge(source=inlet_id, to=uid)

    return net.show("openghg_objstore.html")
