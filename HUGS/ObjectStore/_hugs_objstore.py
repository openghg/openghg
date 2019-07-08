""" Query the object store for data uploaded by a certain user etc

"""
import sys as _sys

if _sys.version_info.major < 3:
    raise ImportError("HUGS requires Python 3.6 minimum")

# from Acquire.ObjectStore import ObjectStore, ObjectStoreError
# from Acquire.Service import get_service_account_bucket, \
#     push_is_running_service, pop_is_running_service

# dateless_key = "/".join(key.split("/")[:-1])
def get_object_names(bucket, prefix=None):
    """ List all the keys in the object store

        TODO - temp function, remove

        Args:
            bucket (dict): Bucket containing data
        Returns:
            list: List of keys in object store
    """
    from Acquire.ObjectStore import ObjectStore as _ObjectStore

    return _ObjectStore.get_all_object_names(bucket, prefix)


def get_dated_object(bucket, key):
    """ Removes the daterange from the passed key and uses the reduced
        key to get an object from the object store.
    
        Wraps the Acquire get_object function
            
        Args:
            bucket (dict): Bucket containing data
            key (str): Key for data in bucket
        Returns:
            Object: Object from store
    """
    from Acquire.ObjectStore import ObjectStore as _ObjectStore

    # Get the object and use the key as a prefix
    name = _ObjectStore.get_all_object_names(bucket, prefix=key)

    if len(name) > 1:
        raise ValueError("There should only be one object")

    return _ObjectStore.get_object(bucket, name[0])


def get_object(bucket, key):
    """ Gets the object at key in the passed bucket

        Wraps the Acquire get_object function

        Args:  
            bucket (dict): Bucket containing data
            key (str): Key for data in bucket
        Returns:
            Object: Object from store
    """
    from Acquire.ObjectStore import ObjectStore as _ObjectStore

    return _ObjectStore.get_object(bucket, key)


def get_dated_object_json(bucket, key):
    """ Removes the daterange from the passed key and uses the reduced
        key to get an object from the object store.
        
        Wraps the Acquire get_object_from_json function

        Args:  
            bucket (dict): Bucket containing data
            key (str): Key for data in bucket
        Returns:
            Object: Object from store
    """
    from Acquire.ObjectStore import ObjectStore as _ObjectStore

    # Get the object and use the key as a prefix
    name = _ObjectStore.get_all_object_names(bucket, prefix=key)

    if len(name) > 1:
        raise ValueError("There should only be one object")

    return _ObjectStore.get_object_from_json(bucket, name[0])


def exists(bucket, key):
    """ Checks if there is an object in the object store with the given key

        Args:
            bucket (dict): Bucket containing data
            key (str): Prefix for key in object store
        Returns:
            bool: True if exists in store
    """
    from Acquire.ObjectStore import ObjectStore as _ObjectStore

    # Get the object and use the key as a prefix
    name = _ObjectStore.get_all_object_names(bucket, prefix=key)

    return len(name) > 0


def get_object_json(bucket, key):
    """ Gets the object at key in the passed bucket

        Wraps the Acquire get_object_from_json function

        Args:
            bucket(dict): Bucket containing data
            key(str): Key for data in bucket
        Returns:
            Object: Object from store
    """
    from Acquire.ObjectStore import ObjectStore as _ObjectStore

    return _ObjectStore.get_object_from_json(bucket, key)

    _ObjectStore.get_obj


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
    import hashlib as _hashlib
    # Size of buffer in bytes
    BUF_SIZE = 65536
    md5 = _hashlib.md5()

    # Read the file in 64 kB blocks
    with open(filename, 'rb') as f:
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
    import hashlib as _hashlib

    return _hashlib.md5(data).hexdigest()


def hash_files(file_list):
    ''' Helper function to hash all the files in
        file_list using MD5

        Args:
            file_list (str): List of files to hash
        Returns:
            list: Returns a list of tuples in the form filename, md5_hash
            
    '''
    # Create a list of tuples for the original files
    hashes = []

    for filepath in file_list:
        md5_hash = get_md5(filepath)
        filename = filepath.split("/")[-1]
        hashes.append((filename, md5_hash))

    return hashes


def write_dataframe(bucket, key, dataframe):
    """ Write the passed dataframe to the object store

    TODO - at the moment this creates a compressred HDF5 file
    from the passed dataframe and then writes that to the
    object store. I feel like it'd be best to get an HDF5 object back
    from Pandas and keep it in memory before passing it to
    the object store for writings. That'd save a lot of reading and
    writing to disk, should have plenty of memory?

        Args:  
            bucket (dict): Bucket to store data
            key (str): Key to data in object store
            dataframe (Pandas.Dataframe): Pandas dataframe to write
        Returns:
            None
    
    """
    import os as _os

    home_path = _os.path.expanduser("~")
    hugs_test_folder = "hugs_tmp/test_hdf5s"
    filename = "testing_dframe.hdf"
    temp_path = _os.path.join(home_path, hugs_test_folder, filename)
    
    # Write to the dataframe to a blosc:lz4 compressed HDF5 file
    dataframe.to_hdf(path=temp_path, key=filename, mode="w", complib="blosc:lz4")
    # Write this HDF5 file to the object store
    filename, size, md5 = store_file(bucket, filename)

    # print(filename, size, md5)
    


# TODO - How to write the HDF5 file to an HDF5 object instead of a HDF5 file 
# on the drive?

def store_file(bucket, filepath):
    """ Write file to the object store

        Args:
            filepath (str): Path of file to write
            to object store
        Returns:
            None
    """
    import os as _os
    # Get the filename from the filepath
    filepath = file.split("/")[-1]
    md5_hash = get_md5(filepath)
    size = _os.path.getsize(filepath)
    filename = filepath.split("/")[-1]
#     # Add to object store
    ObjectStore.set_object_from_file(bucket=bucket, key=filename, filename=filepath)

    # Unsure if this or just no return value?
    return filename, size, md5_hash


def get_bucket(empty=False):
    """ Returns the HUGS bucket
        
        Args:
            empty (bool, default=False): Get an empty bucket
        Returns:
            dict: Bucket
    """
    from HUGS.ObjectStore import get_local_bucket as _get_local_bucket

    return _get_local_bucket(empty=empty)









# def store_raw_data(bucket, filepath):
#     """ Store the raw uploaded data with related metadata
#         such as the uploader, upload date, file format provided
#         by user etc

#         Args:
#             raw_bucket (bytes): The bucket to

#         Returns:
#             tuple (str, int, str): Filename stored, 
#             size in bytes and the MD5 hash of the file

#             Some kind of UUID?

#     """
#     # Get the size and MD5 of the file
#     md5_hash = get_md5(filepath)
#     size = os.path.getsize(filepath)
#     filename = filepath.split("/")[-1]

#     # Add to object store
#     ObjectStore.set_object_from_file(bucket=bucket, key=filename, filename=filepath)

# #     return filename, size, md5_
