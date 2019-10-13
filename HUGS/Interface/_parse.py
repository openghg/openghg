__all__ = ["parse_data"]

def show_results(search_results):
    pass

def parse_data(data):
    """ Converts the data from JSON to Pandas.DataFrames

        Args:
            data (dict): Dictionary of JSON data
        Returns:
            dict: Dict of Pandas DataFrames
    """
    from pandas import read_json

    dataframes = {}
    for key in data:
        dataframes[key] = read_json(data[key])

    return dataframes
