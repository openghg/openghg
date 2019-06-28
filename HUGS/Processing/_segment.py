""" Segment the data into Datasources

"""

__all__ = ["get_datasources", "get_split_frequency"]

def get_datasources(gas_data):
    """ Create or get an exisiting Datasource for each gas in the file

        TODO - currently this function will only take data from a single Datasource
        
        Args:
            gas_data (list): List of tuples gas name, datasource_id, Pandas.Dataframe
        Returns:
            Datasource: Datasource containing data
    """
    from HUGS.Modules import Datasource as _Datasource

    datasources = []
    print("Yahyahyah", type(gas_data))
    for gas_name, metadata, datasource_id, data in gas_data:
        if _Datasource.exists(datasource_id=datasource_id):
            datasource = _Datasource.load(uuid=datasource_id)
        else:
            datasource = _Datasource.create(name=gas_name)

        # Add the dataframes to the datasource
        for dataframe in data:
            datasource.add_data(dataframe)
        
        datasources.append(datasource)

    return datasources


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



