import pandas as pd
import datetime

# def get_metadata(data_file):

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


def parse_file():
    """ This function controls the parsing of the datafile. It calls
        other functions that help to break the datafile apart and 


    """
    # Read everything
    data = pd.read_csv("bsd.picarro.1minute.248m.dat", header=None, skiprows=1, sep=r"\s+")
        
    # Not a huge fan of these hardcoded values
    # TODO - will these change at some point?
    start_date = data[0][2]
    start_time = data[1][2]    
    end_date = data.iloc[-1][0]
    end_time = data.iloc[-1][1]

    start_data = start_date, start_time
    end_data = end_date, end_time

    # Start and end datetime objects
    start, end = get_date_range(start=start_data, end=end_data)

 




parse_file()

# def get_metadata(metadata_frame, daterange):
#     """ This function takes a Pandas dataframe containing the raw meta data

#     """

    # Work through the gases and add them to a set ?

    # info_columns = 0
    # # Count the number of columns we can skip
    # for column in metadata.columns:
    #     if metadata[column][0] == "-":
    #         info_columns += 1

        


    # # Iterate through the columns and read the data
    # for column in metadata.columns:
    #     if metadata[column][0] == "-":
            





    # No need to reinterpret the header string
    # Create header list

    # Meta data
    # --------------------
    # File creation date - is this necessary? Can it harm?
    # daterange in file
    # Type and port?
    # Take the 3 gases from the file and create an xarray from them?
    # Store the metadata as JSON?
    # Make a dict of the header file

    # Either have the data for each gas containing the daterange as well or
    # 

    # site
    # daterange
    # resolution - 1 minute etc accuracy - not this! better word
    # height - 248m etc
    # gases - subkeys 

    # Store the data separately
    # Use the metadata to read the data file that's associated by UID to it

    # Create UIDs
    # store these in some kind of database? 


    # Query the gases in the file

    # Gas data
    # Each gas gets its own UID
    # Within one piece of data take c, stdev and count number
    # This will contain the 
    
    # for i in df_header.columns:
    #     # Here i is an integer starting at 1
    #     # Ignore the metadata - 
    #     if df_header[i][0] != '-':
    #         metadata.append(df_header[i][0].upper() + df_header[i][1])

    #         # This takes in the readings 
    #         if df_header[i][1] == "C":
    #             species.append(df_header[i][0].upper())
    #     else:
    #         header.append(df_header[i][1].upper())

