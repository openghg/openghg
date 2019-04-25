""" 
A start on the splitting of data into parts that can be stored in the 
object store

"""

# import Acquire
from data_processing import process_crds as procCRDS

def copy_raw_data(raw_data):
    """ Store a copy of the raw data in the object store.
    What kind of format should this be? Just bz2 it and
    assign metadata and a UID?
    
    """
  

def get_UID():
    """ Returns a random UID for this object

        Something Acquire will do

    """

    return True

def process_metadata():
    """ Extract the metadata from the header of the file 
        and processes it to be stored with the data in the object store

    """



def segment_data(data, schema):
    """ Break apart an xarray.Dataset
    into segments depending on the schema chosen

    This could be some kind of dict? How to store this
    schema?

    """

    


def load_schema(schema):
    """ Load a schema from file


    """

def create_object(segment, metadata):
    """ Create an object that can be stored in the 
    object store from the passed segment, assign 
    metadata etc to it

    """

    get_UID() 
