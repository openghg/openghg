""" Query the object store for data uploaded by a certain user etc

"""
import hashlib
import os

from objectstore import local_bucket

from Acquire.ObjectStore import ObjectStore, ObjectStoreError
from Acquire.Service import get_service_account_bucket, \
    push_is_running_service, pop_is_running_service

# dateless_key = "/".join(key.split("/")[:-1])


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
    name = ObjectStore.get_all_object_names(bucket, prefix=key)

    if len(name) > 1:
        raise ValueError("There should only be one object")

    return ObjectStore.get_object(bucket, name[0])


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
    name = ObjectStore.get_all_object_names(bucket, prefix=key)

    if len(name) > 1:
        raise ValueError("There should only be one object")

    return ObjectStore.get_object_from_json(bucket, name[0])


def get_object_json(bucket, key):
    """ Gets the object at key in the passed bucket

        Wraps the Acquire get_object_from_json function

        Args:
            bucket(dict): Bucket containing data
            key(str): Key for data in bucket
        Returns:
            Object: Object from store
    """
    return _ObjectStore.get_object_from_json(bucket, key)


def get_abs_filepaths(directory):
    """ Returns the absolute paths of all the files in
        the directory

        Args:
            directory (str): Directory to walk
        Returns:
            list: List of absolute filepaths

    """
    full_filepaths = []
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            full_filepaths.append(os.path.abspath(os.path.join(dirpath, f)))

    return full_filepaths


def get_md5(filename):
    """ Calculates the MD5 sum of the passed file

        Args:
            filename (str): File to hash
        Returns:
            str: MD5 hash of file

    """
    # Size of buffer in bytes
    BUF_SIZE = 65536
    md5 = hashlib.md5()

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
    return hashlib.md5(data).hexdigest()


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
    home_path = os.path.expanduser("~")
    hugs_test_folder = "hugs_tmp/test_hdf5s"
    filename = "testing_dframe.hdf"
    temp_path = os.path.join(home_path, hugs_test_folder, filename)
    
    # Write to the dataframe to a blosc:lz4 compressed HDF5 file
    dataframe.to_hdf(path=temp_path, key=filename, mode="w", complib="blosc:lz4")
    # Write this HDF5 file to the object store
    filename, size, md5 = store_file(bucket, filename)

    # print(filename, size, md5)
    


# TODO - How to write the HDF5 file to an HDF5 object instead of a HDF5 file 
# on the drive?

def get_dataframe(bucket, key):
    """ Gets a dataframe stored as an HDF5 file from the object
        store

        Args:
            bucket (dict): Bucket containing data
            key (str): Key to access dataframe in store
        Returns:
            Pandas.Dataframe: Dataframe from HDF5 file
    """
    import pandas as _pd
    import os as _os

    # Get the file from the object store
    hdf_file = read_object(bucket=bucket, key=key)

    # At the moment write this to a temporary file
    # TODO - must be a better way of doing this
    home_path = _os.path.expanduser("~")
    hugs_test_folder = "hugs_tmp/tmp_hdf5s"
    tmp_file = "tmp.hdf"

    temp_path = _os.path.join(home_path, hugs_test_folder, tmp_file)
    
    with open(temp_path, "wb") as f:
        f.write(hdf_file)

    # Get the dataframe from file
    return _pd.from_hdf(temp_path, key=filename)

def store_file(bucket, filepath):
    """ Write file to the object store

        Args:
            filepath (str): Path of file to write
            to object store
        Returns:
            None
    """
    # Get the filename from the filepath
    filepath = file.split("/")[-1]
    md5_hash = get_md5(filepath)
    size = os.path.getsize(filepath)
    filename = filepath.split("/")[-1]
#     # Add to object store
    ObjectStore.set_object_from_file(
        bucket=bucket, key=filename, filename=filepath)

    # Unsure if this or just no return value?
    return filename, size, md5_hash


def get_bucket():
    """ Returns the HUGS bucket

        Returns:
            dict: Bucket
    """
    from objectstore.local_bucket import get_local_bucket

    return get_local_bucket()


@staticmethod
def read_object(bucket, key):
    """ Reads a file from the object store and returns it
        as a bytes object for downloading or writing
        to file

        Args:
            bucket (dict): Bucket containing the data
            key (str): Key to access data in bucket
        Returns:
            bytes: Binary data contained in object
    """
    from Acquire.ObjectStore import get_object as _get_object

    return _get_object(bucket=bucket, key=key)

@staticmethod
def write_object(bucket, key, data):
    """  Writes a file or object to the object store

        Args:
            bucket (dict): Bucket to store object
            key (str): Key to store data in bucket
            data (bytes): Binary data to store in the object store
        Returns:
            None
    """
    from Acquire.ObjectStore import set_object as _set_object

    _set_object(bucket=bucket, key=key, data=data)








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
