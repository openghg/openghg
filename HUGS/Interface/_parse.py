__all__ = ["parse_data", "get_download_keys"]

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

def get_download_keys(search_results, to_download):
    """ Creates a dictionary of keys to download

        Args:
            search_results (dict): All keys found by search function
            to_download (list): List of keys to downloading using the keys
            found in search_results
        Returns:
            dict: Dictionary of keys to download. Keyed by item in to_download
    """
    return {key: search_results[key]["keys"] for key in to_download}
