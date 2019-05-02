import pandas as pd
import uuid
import datetime

# def get_metadata(data_file):

def unanimous(seq):
    """ Checks that all values in an iterable object
        are the same

        Args:
            seq: Iterable object
        Returns
            bool: True if all values are the same

    """
    it = iter(seq.values())
    try:
        first = next(it)
    except StopIteration:
        return True
    else:
        return all(i == first for i in it)


def parse_date_time(date, time):
    """ This function takes two strings and creates a datetime object 
        
        Args:
            date (str): The date in a YYMMDD format
            time (str): The time in the format hhmmss
            Example: 104930 for 10:49:30
        Returns:
            datetime: Datetime object

    """
    if len(date) != 6:
        raise ValueError("Incorrect date format, should be YYMMDD")
    if len(time) != 6:
        raise ValueError("Incorrect time format, should be hhmmss")

    combined = date + time

    return datetime.datetime.strptime(combined, "%y%m%d%H%M%S")


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


def parse_filename(filename):
    """ Extracts the resolution from the passed string

        Args:
            resolution_str (str): Resolution extracted from the filename
        Returns:
            tuple (str, str, str, str): Site, instrument, resolution
            and height (m)

    """
    # Split the filename to get the site and resolution
    split_filename = filename.split(".")

    site = split_filename[0]
    instrument = split_filename[1]
    resolution_str = split_filename[2]
    height = split_filename[3]

    if(resolution_str == "1minute"):
        resolution = "1m"
    elif(resolution_str == "hourly"):
        resolution = "1h"
    
    return site, instrument, resolution, height


def get_gas_columns(data):
    """ Returns the number of columns of data for each gas
        that is present in the dataframe
    
        Args:
            data (Pandas.DataFrame): Measurement data
        Returns:
            int: Number of columns of readings for each gas
            
    """
    # Slice the dataframe
    head_row = data.head(1)

    gases = {}
    # Take the first row of the DataFrame
    gas_row = 0
    # Loop over the gases and find each unique value
    for column in head_row.columns:
        s = head_row[column][gas_row]
        if s != "-":
            gases[s] = gases.get(s, 0) + 1

    # Check that we have the same number of columns for each gas
    if not unanimous(gases):
        raise ValueError(
            "Each gas does not have the same number of columns")

    return list(gases.values())[0]


def parse_metadata(data, filename):
    """ Extracts the metadata from the datafile and creates a dictionary
        that can then be saved to JSON

        Args:
            dataframe (Pandas.DataFrame): Dataframe containing the
            experimental data
            filename (str): Filename for parsing
        Returns:
            dict: Dictionary containing metadata
    """
    # Dict for storage of metadata
    metadata = {}

    # Not a huge fan of these hardcoded values
    # TODO - will these change at some point?
    start_date = data[0][2]
    start_time = data[1][2]
    end_date = data.iloc[-1][0]
    end_time = data.iloc[-1][1]

    # Find gas measured and port used
    type_meas = data[2][2]
    port = data[3][2]

    start = parse_date_time(date=start_date, time=start_time)
    end = parse_date_time(date=end_date, time=end_time)


    # Extract data from the filename
    site, instrument, resolution, height = parse_filename(filename=filename)

    # Parse the dataframe to find the gases - this might be excessive
    # gases, _ = find_gases(data=data)

    metadata["ID"] = get_uuid()
    metadata["site"] = site
    metadata["instrument"] = instrument
    metadata["resolution"] = resolution
    metadata["height"] = height
    metadata["start_datetime"] = start
    metadata["end_datetime"] = end
    metadata["port"] = port
    metadata["type"] = type_meas
    # This will be added later
    # metadata["gases"] = gases
    
    return metadata

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


def parse_gases(data, n_cols, skip_cols):
    """ Separates the gases stored in the dataframe in 
        separate dataframes and returns a dictionary of gases
        with an assigned UUID as gas:UUID and a list of the processed
        dataframes

        Args:
            data (Pandas.Dataframe): Dataframe containing all data
            n_cols (int): Number of columns of data for each gas
            skip_cols (int): Number of columns before gas data

    """
    gas_list = []
    gas_dict = {}
    for n, g in enumerate(gases):
        # Slice the columns
        gas_data = data.iloc[:, skip_cols + n*n_cols: skip_cols + (n+1)*n_cols]
        # Get the name of the gas
        gas_name = gas_data[0][0]
        # Reset the column numbers
        gas_data.columns = pd.RangeIndex(gas_data.columns.size)
        # Store the name and UUID for dataframe to be stored in the metadata dict
        gases[gas_name] = get_uuid()

        # Create a tuple with an UUID and the data
        gas_list.append(gas_data)

    return gas_dict, gas_list


def parse_file(filename):
    """ This function controls the parsing of the datafile. It calls
        other functions that help to break the datafile apart and
        
        Args:
            filename (str): Name of file to parse
        Returns:
            list: List of gases
    """

    # Read everything
    data = pd.read_csv(filename, header=None, skiprows=1, sep=r"\s+")

    header = data.head(2)

    # Count the number of columns before measurement data
    skip_cols = sum([header[column][0] == "-" for column in header.columns])

    # Create a dataframe of the time and supplementary data
    timeframe = data.iloc[:, 0:sup_cols]
    metadata["time_frame"] = get_uuid()

    n_cols = get_gas_columns(data=data)

    # Get the metadata dictionary - this will be saved as a JSON
    metadata = parse_metadata(data=date, filename=filename)

    # Get gas names and UUIDs in gas_info
    # Raw data in gas_dataframes
    gas_info, gas_dataframes = parse_gases(data=data, n_cols=n_cols, skip_cols=skip_cols)
    

    metadata["gases"] = gas_info

    # Daterange can just be in the format of
    # YYYYMMDD_YYYYMMDD/


def store_data(metadata, data):
    """ This function writes the objects to the object
        store and creates a unique key for their storage 

        Args:
            metadata (dict): Dictionary containing
            metadata for the upload
            data (Pandas.DataFrame): Data contained within
            dataframes to be saved as HDF files in the object
            store
        Returns:
            None

    """

    


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
