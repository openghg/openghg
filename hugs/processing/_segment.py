""" Segment the data into Datasources

"""

def get_datasource(raw_data):
    """ Create a Datasource for each gas in the file

        TODO - currently this function will only take data from a single Datasource
        
        Args:
            raw_data (list): List of Pandas.Dataframes
        Returns:
            Datasource: Datasource containing data
    """
    from modules import Datasource
    
    # This can save the parent instrument as a name,UUID tuple within the object
    # Look up the parent instrument by name and find its UUID. If it doesn't exist, create it?
    datasource_id, gas_data = parse_gases(raw_data)

    if Datasource.exists(datasource_id=datasource_id):
        datasource = Datasource.load(uuid=datasource_id)
    else:
        datasource = Datasource.create(name="name")

    for _, dataframes in gas_data:
        for dataframe in dataframes:
            datasource.add_data(dataframe)

    return datasource


def get_split_frequency(data):
    """ Analyses raw data for size and sets a frequency to split the data
        depending on how big the resulting dataframe will be

        Args:
            data (Pandas.Dataframe): Raw data in dataframe
        Returns:
            str: String selecting frequency for data splitting by Groupby
    """

    data_size = data.memory_usage(deep=True).sum()
    # If the data is larger than this it will be split into
    # separate parts
    # For now use 5 MB chunks
    segment_size = 5_242_880  # bytes

    # Get time delta for the first and last date
    start_data = data.first_valid_index()
    end_data = data.last_valid_index()

    num_years = int((end_data - start_data).days / 365.25)
    if num_years < 1:
        num_years = 1

    n_months = 12
    n_weeks = 52
    n_days = 365
    n_hours = 24

    freq = "Y"
    # Try splitting into years
    if data_size / num_years <= segment_size:
        return freq
    elif data_size / (num_years * n_months) <= segment_size:
        freq = "M"
        return freq
    elif data_size / (num_years * n_months * n_weeks) <= segment_size:
        freq = "W"
        return freq
    elif data_size / (num_years * n_months * n_weeks * n_days) <= segment_size:
        freq = "D"
        return freq
    elif data_size / (num_years * n_months * n_weeks * n_days * n_hours) <= segment_size:
        freq = "H"
        return freq


def parse_gases(data):
    """ Separates the gases stored in the dataframe in 
        separate dataframes and returns a dictionary of gases
        with an assigned UUID as gas:UUID and a list of the processed
        dataframes

        Args:
            data (Pandas.Dataframe): Dataframe containing all data
        Returns:
            tuple (str, list): ID of Datasource of this data and a list of tuples containing the name of the gas 
            and a list of sections of the gas dataframe split into sections based on datetime
    """
    from pandas import RangeIndex as _RangeIndex
    from pandas import concat as _concat
    from pandas import Grouper as _Grouper

    # Create an ID for the Datasource
    # Currently just give it a fixed ID
    datasource_ids = ["2e628682-094f-4ffb-949f-83e12e87a603", "2e628682-094f-4ffb-949f-83e12e87a604", 
                        "2e628682-094f-4ffb-949f-83e12e87a605"]

    # Drop any rows with NaNs
    # Reset the index
    # This is now done before creating metadata
    data = data.dropna(axis=0, how="any")
    data.index = _RangeIndex(data.index.size)

    # Get the number of gases in dataframe and number of columns of data present for each gas
    n_gases, n_cols = gas_info(data=data)

    header = data.head(2)
    skip_cols = sum([header[column][0] == "-" for column in header.columns])

    time_cols = 2
    header_rows = 2
    # Dataframe containing the time data for this data input
    time_data = data.iloc[2:, 0:time_cols]

    timeframe = parse_timecols(time_data=time_data)
    timeframe.index = _RangeIndex(timeframe.index.size)

    data_list = []
    for n in range(n_gases):
        # Slice the columns
        gas_data = data.iloc[:, skip_cols + n*n_cols: skip_cols + (n+1)*n_cols]

        # Reset the column numbers
        gas_data.columns = _RangeIndex(gas_data.columns.size)
        gas_name = gas_data[0][0]

        column_names = ["count", "stdev", "n_meas"]
        column_labels = ["%s %s" % (gas_name, l) for l in column_names]

        # Split into years here
        # Name columns
        gas_data.set_axis(column_labels, axis='columns', inplace=True)

        # Drop the first two rows now we have the name
        gas_data.drop(index=gas_data.head(header_rows).index, inplace=True)
        gas_data.index = _RangeIndex(gas_data.index.size)

        # Cast data to float64 / double
        gas_data = gas_data.astype("float64")
        # Concatenate the timeframe and the data
        gas_data = _concat([timeframe, gas_data], axis="columns")

        # TODO - Verify integrity here? Test if this is required
        gas_data.set_index('Datetime', drop=True, inplace=True, verify_integrity=True)

        freq = get_split_frequency(gas_data)
        # Split into sections by year
        group = gas_data.groupby(_Grouper(freq=freq))
        # As some (years, months, weeks) may be empty we don't want those dataframes
        split_frames = [g for _, g in group if len(g) > 0]

        data_list.append((gas_name, split_frames))

    # TODO - this return might be getting a bit complicated - how to simplify it?
    # return datasource_id, data_list
    # TODO - get this to return a datasource uuid for each gas and then the data 
    return False


def parse_timecols(time_data):
    """ Takes a dataframe that contains the date and time 
        and creates a single columned dataframe containing a 
        UTC datetime

        Args:
            time_data (Pandas.Dataframe): Dataframe containing
        Returns:
            timeframe (Pandas.Dataframe): Dataframe containing datetimes set to UTC
    """
    import datetime as _datetime
    from pandas import DataFrame as _DataFrame
    from pandas import to_datetime as _to_datetime

    from Acquire.ObjectStore import datetime_to_datetime as _datetime_to_datetime
    from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

    def t_calc(row, col): return _datetime_to_string(_datetime_to_datetime(
        _datetime.datetime.strptime(row+col, "%y%m%d%H%M%S")))

    time_gen = (t_calc(row, col)
                for row, col in time_data.itertuples(index=False))
    time_list = list(time_gen)

    timeframe = _DataFrame(data=time_list, columns=["Datetime"])

    # Check how these data work when read back out
    timeframe["Datetime"] = _to_datetime(timeframe["Datetime"])

    return timeframe


def gas_info(data):
        """ Returns the number of columns of data for each gas
            that is present in the dataframe
        
            Args:
                data (Pandas.DataFrame): Measurement data
            Returns:
                tuple (int, int): Number of gases, number of
                columns of data for each gas
        """
        # Slice the dataframe
        head_row = data.head(1)

        gases = {}

        # Loop over the gases and find each unique value
        for column in head_row.columns:
            s = head_row[column][0]
            if s != "-":
                gases[s] = gases.get(s, 0) + 1

        # Check that we have the same number of columns for each gas
        if not unanimous(gases):
            raise ValueError(
                "Each gas does not have the same number of columns")

        return len(gases), list(gases.values())[0]


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


# def parse_file(filepath):
#     """ This function controls the parsing of the datafile. It calls
#         other functions that help to break the datafile apart and
        
#         Args:
#             filename (str): Name of file to parse
#         Returns:
#             list: List of gases
#     """
#     # Read everything
#     data = pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

#     # header = data.head(2)

#     # # Count the number of columns before measurement data
#     # skip_cols = sum([header[column][0] == "-" for column in header.columns])

#     # Create a dataframe of the time and supplementary data
#     # metadata["time_frame"] = data.iloc[:, 0:skip_cols]
#     # Get the metadata dictionary - this will be saved as a JSON
#     filename = filepath.split("/")[-1]
#     metadata = meta.parse_metadata(data=data, filename=filename)

#     # Dictionary of gases for saving to object store
#     gases = _parse_gases(data=data, skip_cols=skip_cols)

#     # Dictionary of gas_name:UUID pairs
#     gas_metadata = {g: gases[g]["UUID"] for g in gases.keys()}

#     metadata["gases"] = gas_metadata

#     # Save as part of gases dictionary
#     gases["metadata"] = metadata

#     # Dictionary of {metadata: ..., gases: {gas: UUID, gas: UUID ...} }
#     return gases

#     # # Extract the gas name and UUID from the gases dictionary
#     # gas_metadata = {}
#     # for g in gases.keys():
#     #     gas_metadata[g] = gases[g]["UUID"]

#     # gas_info = {x for x in gases.keys()}


#     # Daterange can just be in the format of
#     # YYYYMMDD_YYYYMMDD
    

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
