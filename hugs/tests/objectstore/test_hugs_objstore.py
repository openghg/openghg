""" Test the HUGS object store

"""
import os
import pytest

from objectstore import _hugs_objstore

from Acquire.ObjectStore import ObjectStore, ObjectStoreError
from Acquire.Service import get_service_account_bucket, \
    push_is_running_service, pop_is_running_service

@pytest.fixture(scope="session")
def bucket(tmpdir_factory):
    d = tmpdir_factory.mktemp("simple_objstore")
    push_is_running_service()
    bucket = get_service_account_bucket(str(d))
    pop_is_running_service()
    return bucket

# These are not needed due to the functionality being in Acquire

# def test_hash_files():
#     """ Ensure 3 files are hashed and returned correctly """

#     file_list = ["test.dat", "test1.dat", "test2.dat"]
#     # dirname = os.path.dirname(__file__)
#     path = os.path.join("..", "data")

#     file_list = [os.path.join(path, f) for f in file_list]

#     hashes = hugs_objstore.hash_files(file_list)

#     _, hash1 = hashes[0]
#     _, hash2 = hashes[1]
#     _, hash3 = hashes[2]

#     assert hash1 == "aca19f43ba0f49102546425308443538"
#     assert hash2 == "39e7b40232726e88905e5c382f7540c9"
#     assert hash3 == "73f6ec825d38769bec51cd260f0259df"


# def get_filename(filepath):
#     """ Strip the filename from a filepath

#         Example:
#             Takes a filepath /home/user/file.dat
#             Returns file.dat
#         Args:
#             filepath (str): Filepath for accessing a file
#         Returns:
#             str: Filename
#     """
#     return filepath.split("/")[-1]
