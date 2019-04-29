"""

Query the object store for data uploaded by a certain user etc

"""
import hashlib
import os

from Acquire.ObjectStore import ObjectStore, ObjectStoreError
from Acquire.Service import get_service_account_bucket, \
    push_is_running_service, pop_is_running_service


def get_abs_filepaths(directory):
    full_filepaths = []
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            full_filepaths.append(os.path.abspath(os.path.join(dirpath, f)))

    return full_filepaths


def get_md5(filename):
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
    md5 = hashlib.md5()

    with open(filename, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)

    return md5.hexdigest()


def get_md5_bytes(data):
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
    md5 = hashlib.md5()

    # if len(data) < BUF_SIZE:

    print(len(data))

    md5.update(data)

    # while True:
    #     buf = data.read(BUF_SIZE)
    #     if not buf:
    #         break
    #     md5.update(buf)
    
    return md5.hexdigest()


def hash_files(file_list):
    ''' Helper function to hash all the files in
        file_list using MD5
    '''
    # Create a list of tuples for the original files
    hashes = []

    for filepath in file_list:
        md5_hash = get_md5(filepath)
        filename = filepath.split("/")[-1]
        hashes.append((filename, md5_hash))

    return hashes

def bucket_handler():
    """ How to handle buckets for raw data that's being uploaded?
        Store a certain number or size of data in each bucket?

    """

def store_raw_data(bucket, filepath):
    """ Store the raw uploaded data with related metadata
        such as the uploader, upload date, file format provided
        by user etc

        Args:
            raw_bucket (bytes): The bucket to

        Returns:
            tuple (str, int, str): Filename stored, 
            size in bytes and the MD5 hash of the file

            Some kind of UUID?

    """
    # Get the size and MD5 of the file
    md5_hash = get_md5(filepath)
    size = os.path.getsize(filepath)
    filename = filepath.split("/")[-1]

    # Add to object store
    ObjectStore.set_object_from_file(bucket=bucket, key=filename, filename=filepath)

    return filename, size, md5_hash

    
def get_raw_data(bucket, filename):
    """ Get the raw data described by the passed filename,
        from the object store

        Args:
            bucket (dict): Bucket containing raw data
            filename (str): Filename of requested file
        Returns:
            bytes: Binary data contained in object

    """

    return ObjectStore.get_object(bucket=bucket, key=filename)    


def get_by_user():
    """ Get files uploaded by a specific user
    
    """
