""" 
A start on the splitting of data into parts that can be stored in the object store




"""

def get_UID():
    """
        Returns a random UID for this object
    """

    return True

def segment_dataset(dataset, schema):
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

