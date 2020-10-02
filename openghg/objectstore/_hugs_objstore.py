""" Query the object store for data uploaded by a certain user etc

"""
from Acquire.ObjectStore import ObjectStore


__all__ = [
    "delete_object",
    "get_object_names",
    "get_object",
    "get_object_from_json",
    "get_local_bucket",
    "set_object_from_json",
    "set_object_from_file",
    "exists",
    "get_abs_filepaths",
    "get_md5",
    "get_md5_bytes",
    "hash_files",
    "get_bucket",
]


def delete_object(bucket, key):
    """ Delete the object at key in bucket

        Args:
            bucket (str): Bucket containing data
            key (str): Key for data in bucket
        Returns:
            None
    """
    return ObjectStore.delete_object(bucket=bucket, key=key)


def get_object_names(bucket, prefix=None):
    """ List all the keys in the object store

        TODO - temp function, remove

        Args:
            bucket (dict): Bucket containing data
        Returns:
            list: List of keys in object store
    """
    return ObjectStore.get_all_object_names(bukcet=bucket, prefix=prefix)


def get_object(bucket, key):
    """ Gets the object at key in the passed bucket

        Wraps the Acquire get_object function

        Args:
            bucket (dict): Bucket containing data
            key (str): Key for data in bucket
        Returns:
            Object: Object from store
    """
    return ObjectStore.get_object(bucket, key)


def get_object_from_json(bucket, key):
    """ Removes the daterange from the passed key and uses the reduced
        key to get an object from the object store.

        Wraps the Acquire get_object_from_json function

        Args:
            bucket (dict): Bucket containing data
            key (str): Key for data in bucket
        Returns:
            Object: Object from store
    """
    # Get the object and use the key as a prefix
    name = ObjectStore.get_all_object_names(bucket, prefix=key)

    if len(name) > 1:
        raise ValueError("There should only be one object with this key")

    return ObjectStore.get_object_from_json(bucket, name[0])


def exists(bucket, key):
    """ Checks if there is an object in the object store with the given key

        Args:
            bucket (dict): Bucket containing data
            key (str): Prefix for key in object store
        Returns:
            bool: True if exists in store
    """
    # Get the object and use the key as a prefix
    name = ObjectStore.get_all_object_names(bucket, prefix=key)

    return len(name) > 0


def set_object_from_json(bucket, key, data):
    """ Wraps the Acquire set_object_from_json function

        Args:
            bucket (str): Bucket for data storage
            key (str): Key for data in bucket
            data: Data
        Returns:
            None
    """
    return ObjectStore.set_object_from_json(bucket=bucket, key=key, data=data)


def set_object_from_file(bucket, key, filename):
    """ Set an object in the object store from the file
        at filename

        Args:
            bucket (str): Bucket to contain data
            key (str): Key for data in bucket
            filename (str, pathlib.Path): Filename/path
        Returns:
            None
    """
    return ObjectStore.set_object_from_file(bucket=bucket, key=key, filename=filename)


def get_abs_filepaths(directory):
    """ Returns the absolute paths of all the files in
        the directory

        Args:
            directory (str): Directory to walk
        Returns:
            list: List of absolute filepaths

    """
    import os as _os

    full_filepaths = []
    for dirpath, _, filenames in _os.walk(directory):
        for f in filenames:
            full_filepaths.append(_os.path.abspath(_os.path.join(dirpath, f)))

    return full_filepaths


def get_md5(filename):
    """ Calculates the MD5 sum of the passed file

        Args:
            filename (str): File to hash
        Returns:
            str: MD5 hash of file

    """
    import hashlib

    # Size of buffer in bytes
    BUF_SIZE = 65536
    md5 = hashlib.md5()

    # Read the file in 64 kB blocks
    with open(filename, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)

    return md5.hexdigest()


def get_md5_bytes(data):
    """ Gets the MD5 hash of a bytes object holding data

        Args:
            data (bytes): Data as a bytes object
        Returns:
            str: MD5 hash of data

    """
    import hashlib

    return hashlib.md5(data).hexdigest()


def hash_files(file_list):
    """ Helper function to hash all the files in
        file_list using MD5

        Args:
            file_list (str): List of files to hash
        Returns:
            list: Returns a list of tuples in the form filename, md5_hash
    """
    # Create a list of tuples for the original files
    hashes = []

    for filepath in file_list:
        md5_hash = get_md5(filepath)
        filename = filepath.split("/")[-1]
        hashes.append((filename, md5_hash))

    return hashes


def get_bucket(empty=False):
    """ Returns the the object store bucket

        Args:
            empty (bool, default=False): Get an empty bucket
        Returns:
            str: Bucket path as string
    """
    from Acquire.Service import get_service_account_bucket, ServiceError

    try:
        bucket = get_service_account_bucket()
    except ServiceError:
        bucket = get_local_bucket(empty=empty)

    return bucket


def get_local_bucket(empty=False):
    """ Creates and returns a local bucket that's created in the
        /tmp/hugs_test directory

        Args:
            empty (bool, default=False): If True return an empty bucket
        Returns:
            str: Path to local bucket
    """
    from pathlib import Path
    import shutil
    from Acquire.ObjectStore import use_testing_object_store_backend

    local_buckets_dir = Path("/tmp/hugs_test")

    if local_buckets_dir.exists() and empty is True:
        shutil.rmtree(local_buckets_dir)
        local_buckets_dir.mkdir(parents=True)
    else:
        local_buckets_dir.mkdir(parents=True)

    root_bucket = use_testing_object_store_backend(local_buckets_dir)

    bucket = ObjectStore.create_bucket(bucket=root_bucket, bucket_name="hugs_test")

    return bucket
