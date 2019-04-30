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

def get_date_range(start, end):
    """ Takes two tuples and gets a datetime for both

        Args:
            start (tuple (str,str)): Start date and time
            end (tuple (str,str)): End date and time
        Returns:
            tuple (datetime, datetime): Start and end 
            datetime objects

    """
    start_datetime = parse_date_time(*start)
    end_datetime = parse_date_time(*end)

    return start_datetime, end_datetime


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


def find_gases(data):
    """ Find the gases measured in the dataframe
    
        Args:
            data (Pandas.DataFrame): Measurement data
        Returns:
            tuple (dict, int): A tuple containing
            a list of the gases and the number of columns
            of data for each gas. 
            
            Note: this list may have no meaningful order

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
            "Each gas does not have the same number reading of columns")

    n_cols = list(gases.values())[0]

    return gases, n_cols

    # Finding the gases and returning their names a list of the number of columns for each gas
    # Then splitting the gases up using the number of columns for each gas
    # Gas name can be read from the top of the gas dataframe and saved as part of the dictionary the
    # dataframe is put in
    

def parse_metadata(filename, data):
    """ Extracts the metadata from the datafile and creates a dictionary
        that can then be saved to JSON

        Args:
            filename (str): Filename for parsing
            dataframe (Pandas.DataFrame): Dataframe containing the
            experinmental data
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

    start_data = start_date, start_time
    end_data = end_date, end_time

    # Find gas measured and port used
    type_meas = data[2][2]
    port = data[3][2]

    # Start and end datetime objects
    start, end = get_date_range(start=start_data, end=end_data)

    # Extract data from the filename
    site, instrument, resolution, height = parse_filename(filename=filename)

    # Parse the dataframe to find the gases - this might be excessive
    gases, _ = find_gases(data)

    metadata["ID"] = get_uuid()
    metadata["site"] = site
    metadata["instrument"] =
    metadata["resolution"] = resolution
    metadata["height"] = height
    metadata["start_datetime"] = start
    metadata["end_datetime"] = end
    metadata["port"] = port
    metadata["type"] = type_meas
    metadata["gases"] = gases
    
    return metadata


def get_uuid():
    """ Returns a random UUID

        Returns:
            str: Random UUID
    """
    return uuid.uuid4()

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
    # Number of columns before the get to the measurement data
    skip_cols = sum([header[column][0] == "-" for column in header.columns])

    # Get the metadata dictionary
    metadata = parse_metadata(filename, data)
        
    # Get the number of columns of data are present for each gas
    gases, n_cols = find_gases(data)
    
    gas_list = []
    for n, g in enumerate(gases):
        print("Creating gas : ", n)
        gas = {}
        # Slice the columns
        gas_data = data.iloc[:, skip_cols + n*n_cols: skip_cols + (n+1)*n_cols]
        # Reset the column numbers
        gas_data.columns = pd.RangeIndex(gas_data.columns.size)
        gas["name"] = gas_data[0][0]
        gas["metadata_ID"] = metadata["UUID"]
        gas["ID"] = get_uuid()
        gas["data"] = gas_data
        gas_list.append(gas)

    return gas_list