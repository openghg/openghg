""" Segment the data into Datasources

"""

import pandas as pd
import uuid
import datetime
import urllib.parse
import datetime

from processing import _metadata as meta


def calc_time_delta(start, end):
    """ Calculates the time delta between the first and last
        reading

        Unsure if this is needed
    
        Args:
            start (datetime): First measurement
            end (datetime): Last measurement
        Returns:
            timedelta: Timedelta            
    """
    return False

# def save_timeframe(data, sup_cols):
#     """ Creates a Pandas.Dataframe of the time and supplementary
#         columns 

#         Returns:
#             Pandas.Dataframe: Time and supplementary data

#     """
#     return data.iloc[:, 0:sup_cols]

def get_uuid():
    """ Returns a random UUID

        Returns:
            str: Random UUID
    """
    return uuid.uuid4()


def parse_gases(data, skip_cols):
    """ Separates the gases stored in the dataframe in 
        separate dataframes and returns a dictionary of gases
        with an assigned UUID as gas:UUID and a list of the processed
        dataframes

        Args:
            data (Pandas.Dataframe): Dataframe containing all data
            n_cols (int): Number of columns of data for each gas
            skip_cols (int): Number of columns before gas data

    """
    # Get the number of gases in dataframe and number of columns
    # of data present for each gas
    from metadata import Metadata

    n_gases, n_cols = Metadata.gas_info(data=data)
    gases = {}

    for n in range(n_gases):
        # Slice the columns
        gas_data = data.iloc[:, skip_cols + n*n_cols: skip_cols + (n+1)*n_cols]        
        # Reset the column numbers
        gas_data.columns = pd.RangeIndex(gas_data.columns.size)

        gas_name = gas_data[0][0]
        # Store the name and UUID for dataframe to be stored in the metadata dict
        gases[gas_name] = {"UUID": get_uuid(), "data": gas_data}

    return gases


def parse_file(filepath):
    """ This function controls the parsing of the datafile. It calls
        other functions that help to break the datafile apart and
        
        Args:
            filename (str): Name of file to parse
        Returns:
            list: List of gases
    """
    # Read everything
    data = pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

    header = data.head(2)

    # Count the number of columns before measurement data
    skip_cols = sum([header[column][0] == "-" for column in header.columns])

    # Create a dataframe of the time and supplementary data
    # metadata["time_frame"] = data.iloc[:, 0:skip_cols]
    # Get the metadata dictionary - this will be saved as a JSON
    filename = filepath.split("/")[-1]
    metadata = meta.parse_metadata(data=data, filename=filename)

    # Dictionary of gases for saving to object store
    gases = parse_gases(data=data, skip_cols=skip_cols)

    # Dictionary of gas_name:UUID pairs
    gas_metadata = {g: gases[g]["UUID"] for g in gases.keys()}

    metadata["gases"] = gas_metadata

    # Save as part of gases dictionary
    gases["metadata"] = metadata

    # Dictionary of {metadata: ..., gases: {gas: UUID, gas: UUID ...} }
    return gases

    # # Extract the gas name and UUID from the gases dictionary
    # gas_metadata = {}
    # for g in gases.keys():
    #     gas_metadata[g] = gases[g]["UUID"]

    # gas_info = {x for x in gases.keys()}


    # Daterange can just be in the format of
    # YYYYMMDD_YYYYMMDD


def store_data(gas_data):
    """ This function writes the objects to the object
        store and creates a unique key for their storage

        Args:
            metadata (dict): Dictionary containing
            metadata for the upload
            data (Pandas.DataFrame): Data contained within
            dataframes to be saved as HDF files in the object store
        Returns:
            None

        TODO - add return value so we know it's been stored successfully

    """
    metadata = gas_data["metadata"]
    key = key_creator(metadata=metadata)

    # How best to identify the metadata - UUID for this in keypath as well?
    for gas in gas_data["gases"].keys():
        # Append the UID for each gas to the metadata key
        key = urllib.parse.urljoin(key, gas_data[gas]["UUID"])
        # Store it in the object store
        write_dataframe(bucket=bucket, key=key, dataframe=gas_data["data"])
    

def url_join(*args):
    """ Joins given arguments into an filepath style key. Trailing but not leading slashes are
        stripped for each argument.

        Args:
            *args (str): Strings to concatenate into a key to use
            in the object store
        Returns:
            str: A url style key string with arguments separated by forward slashes
    """

    return "/".join(map(lambda x: str(x).rstrip('/'), args))       


def key_creator(metadata):
    """ Creates a key to be used as an identifier for
        data in the object store

        Args:
            metadata (dict): Metadata
        Returns:
            str: Key for use in object store
    """
    # Key could be something like
    # /site/instrument/height/daterange/gas/UID_of_reading

    site = metadata["site"]
    instrument = metadata["instrument"]
    height = None
    if "height" in metadata:
        height = metadata["height"]
    date_range = get_daterange_str(start=metadata["start_datetime"],
                                    end=metadata["end_datetime"])
    uuid = metadata["UUID"]

    if height:
        # return urllib.parse.urljoin(site, instrument, height, date_range, uuid)
        return url_join(site, instrument, height, date_range, uuid)
    else:
        return url_join(site, instrument, date_range, uuid)


def get_daterange_str(start, end):
    """ Creates a string from the start and end datetime
        objects. Used for production of the key
        to store segmented data in the object store.

        Args:  
            start (datetime): Start datetime
            end (datetime): End datetime
        Returns:
            str: Daterange formatted as start_end
            YYYYMMDD_YYYYMMDD
            Example: 20190101_20190201
    """

    start_fmt = start.strftime("%Y%m%d")
    end_fmt = end.strftime("%Y%m%d")
    
    return start_fmt + "_" + end_fmt




    # Now to save the metadata and the data to the object store using their
    # associated IDs - or the /site/instrument/height/daterange/gas/UID_of_reading ?
    # That could be the key
    # 
    #  method
    # that method is far more readable

    # Write the gas data to the d



    # Key like /site/instrument/height/daterange ?
    # Or just a UUID for this block of data?


    # return gas_list
