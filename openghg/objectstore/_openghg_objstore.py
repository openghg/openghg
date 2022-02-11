# """ Query the object store for data uploaded by a certain user etc

# """
# from typing import Dict, List, Union
# from pathlib import Path
# from Acquire.ObjectStore import ObjectStore


# __all__ = [
#     "delete_object",
#     "get_object",
#     "get_object_from_json",
#     "get_local_bucket",
#     "set_object_from_json",
#     "set_object_from_file",
#     "exists",
#     "get_abs_filepaths",
#     "get_md5",
#     "get_md5_bytes",
#     "hash_files",
#     "get_bucket",
# ]


# def delete_object(bucket: str, key: str) -> None:
#     """Delete the object at key in bucket

#     Args:
#         bucket (str): Bucket containing data
#         key (str): Key for data in bucket
#     Returns:
#         None
#     """
#     return ObjectStore.delete_object(bucket=bucket, key=key)


# def get_object(bucket: str, key: str) -> bytes:
#     """Gets the object at key in the passed bucket

#     Wraps the Acquire get_object function

#     Args:
#         bucket: Bucket containing data
#         key: Key for data in bucket
#     Returns:
#         bytes: Object from store
#     """
#     return ObjectStore.get_object(bucket, key)


# def get_object_from_json(bucket: str, key: str) -> Dict[str, Union[str, Dict]]:
#     """Removes the daterange from the passed key and uses the reduced
#     key to get an object from the object store.

#     Args:
#         bucket: Bucket containing data
#         key: Key for data in bucket
#     Returns:
#         dict: Dictionary
#     """
#     from json import loads

#     data: Union[str, bytes] = get_object(bucket, key).decode("utf-8")
#     data_dict: Dict = loads(data)

#     return data_dict


# def exists(bucket: str, key: str) -> bool:
#     """Checks if there is an object in the object store with the given key

#     Args:
#         bucket (dict): Bucket containing data
#         key (str): Prefix for key in object store
#     Returns:
#         bool: True if exists in store
#     """
#     # Get the object and use the key as a prefix
#     name: List = ObjectStore.get_all_object_names(bucket, prefix=key)

#     return len(name) > 0


# def set_object_from_json(bucket: str, key: str, data: Union[str, Dict]) -> None:
#     """Wraps the Acquire set_object_from_json function

#     Args:
#         bucket: Bucket for data storage
#         key: Key for data in bucket
#         data: Data
#     Returns:
#         None
#     """
#     return ObjectStore.set_object_from_json(bucket=bucket, key=key, data=data)


# def set_object_from_file(bucket: str, key: str, filename: str) -> None:
#     """Set an object in the object store from the file
#     at filename

#     Args:
#         bucket: Bucket to contain data
#         key: Key for data in bucket
#         filename: Filename
#     Returns:
#         None
#     """
#     return ObjectStore.set_object_from_file(bucket=bucket, key=key, filename=filename)


# def get_abs_filepaths(directory: str) -> List:
#     """Returns the absolute paths of all the files in
#     the directory

#     Args:
#         directory: Directory to walk
#     Returns:
#         list: List of absolute filepaths
#     """
#     import os as _os

#     full_filepaths = []
#     for dirpath, _, filenames in _os.walk(directory):
#         for f in filenames:
#             full_filepaths.append(_os.path.abspath(_os.path.join(dirpath, f)))

#     return full_filepaths


# def get_md5(filename: str) -> str:
#     """Calculates the MD5 sum of the passed file

#     Args:
#         filename: File to hash
#     Returns:
#         str: MD5 hash of file
#     """
#     import hashlib

#     # Size of buffer in bytes
#     BUF_SIZE = 65536
#     md5 = hashlib.md5()

#     # Read the file in 64 kB blocks
#     with open(filename, "rb") as f:
#         while True:
#             data = f.read(BUF_SIZE)
#             if not data:
#                 break
#             md5.update(data)

#     return md5.hexdigest()


# def get_md5_bytes(data: bytes) -> str:
#     """Gets the MD5 hash of a bytes object holding data

#     Args:
#         data: Data as a bytes object
#     Returns:
#         str: MD5 hash of data
#     """
#     import hashlib

#     return hashlib.md5(data).hexdigest()


# def hash_files(file_list: List) -> List[str]:
#     """Helper function to hash all the files in
#     file_list using MD5

#     Args:
#         file_list: List of files to hash
#     Returns:
#         list: Returns a list of tuples in the form filename, md5_hash
#     """
#     # Create a list of tuples for the original files
#     hashes = []

#     for filepath in file_list:
#         md5_hash = get_md5(filepath)
#         filename = filepath.split("/")[-1]
#         hashes.append((filename, md5_hash))

#     return hashes


# def get_bucket() -> str:
#     """Returns the the object store bucket

#     Args:
#         empty (bool, default=False): Get an empty bucket
#     Returns:
#         str: Bucket path as string
#     """
#     from Acquire.Service import get_service_account_bucket

#     bucket = get_service_account_bucket()

#     return bucket


# def get_local_bucket(empty: bool = False) -> str:
#     """Creates and returns a local bucket

#     Args:
#         empty: If True return an empty bucket
#     Returns:
#         str: Path to local bucket
#     """
#     import shutil
#     import os

#     local_buckets_dir = os.getenv("OPENGHG_PATH")

#     if local_buckets_dir is not None:
#         return Path(local_buckets_dir)
#     else:
#         raise ValueError(
#             "No environment variable OPENGHG_PATH found, please set to use the local object store"
#         )

#     if local_buckets_dir.exists():
#         if empty is True:
#             shutil.rmtree(local_buckets_dir)
#             local_buckets_dir.mkdir(parents=True)
#     else:
#         local_buckets_dir.mkdir(parents=True)

#     return str(local_buckets_dir)


# # TODO - should these functions raise errors if called in the cloud version?
# # get_all_object_names'
# # get_object_names'
# # get_openghg_local_path'
# # query_store'
# # set_object'
# # visualise_store
