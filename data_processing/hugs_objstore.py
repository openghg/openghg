"""

Query the object store for data uploaded by a certain user etc

"""
import hashlib
import os

import data_processing.local_bucket as local_bucket

from Acquire.ObjectStore import ObjectStore, ObjectStoreError
from Acquire.Service import get_service_account_bucket, \
    push_is_running_service, pop_is_running_service


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
            dataframe: Pandas dataframe to write
        Returns:
            None
    
    """
    home_path = os.path.expanduser("~")
    hugs_test_folder = "hugs_tmp/test_hdf5s"
    filename = "testing_dframe.hdf"
    temp_path = os.path.join(home_path, hugs_test_folder, filename)
    
    # Write to the dataframe to a blosc:lz4 compressed HDF5 file
    dataframe.to_hdf(path=temp_path, key=filename, mode="w", complib=blosc:lz4)
    # Write this HDF5 file to the object store
    filename, size, md5 = write_object(bucket, filename)

    print(filename, size, md5)
    


# TODO - How to write the HDF5 file to an HDF5 object instead of a HDF5 file 
# on the drive?

def get_dataframe(bucket, key, filename):
    """ Gets a dataframe stored as an HDF5 file from the object
        store

        Args:
            bucket (dict): Bucket containing data
            key (str): Key to access datatframe in store
        Returns:
            Pandas.Dataframe: Dataframe from HDF5 file
        """

    # Get the file from the object store
    hdf_file = read_file(bucket=bucket, filename=filename)

    # At the moment write this to a temporary file
    # TODO - must be a better way of doing this
    home_path = os.path.expanduser("~")
    hugs_test_folder = "hugs_tmp/tmp_hdf5s"
    tmp_file = "tmp.hdf"

    temp_path = os.path.join(home_path, hugs_test_folder, tmp_file)
    
    with open(temp_path, "wb") as f:
        f.write(hdf_file)

    # Get the dataframe from file
    return pd.from_hdf(temp_path, key=filename)


def combine_sections():
    """ Combines separate dataframes into a single dataframe for
        processing to NetCDF for output

        Args:
            
        Returns:
            Pandas.Dataframe: Combined dataframes

    """




def write_object(bucket, filepath):
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

    
def read_file(bucket, filename):
    """ Reads a file from the object store and returns it
        as a bytes object for downloading or writing
        to file

        Args:
            bucket (dict): Bucket containing the data
            file (str): Filename to use as a key to get
            the data from the bucket
        Returns:
            bytes: Binary data contained in object

    """
    return ObjectStore.get_object(bucket=bucket, key=filename)


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
