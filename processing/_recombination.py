""" Handles the recombination of dataframes stored in the object store
    into the data requested by the user

"""


def combine_sections(dataframes):
    """ Combines separate dataframes into a single dataframe for
        processing to NetCDF for output

        Args:
            dataframes (list): List of dataframes for recombination
        Returns:
            Pandas.Dataframe: Combined dataframes
    """
    import pandas as _pd
    # Get the first column for timeframe comparison
    timeframe = dataframes[0].iloc[:, :1]
    
    for d in dataframes:
        assert len(d.index) == len(timeframe.index)
        d.drop(columns="Datetime", axis="columns", inplace=True)
        timeframe = _pd.concat([timeframe, d], axis=1)

    return timeframe

def convert_to_netcdf(dataframe):
    """ Converts the passed dataframe to netcdf, performs checks
        and returns

        Args:
            dataframe (Pandas.Dataframe): Dataframe for convesion
        Returns:
            bytes: NetCDF file as a bytes array
    """
    from xarray import Dataset as _Dataset

    pass

    # Dataset('inmemory.nc', diskless=True, mode='w')

    # _Dataset.to_netcdf

    

    

    



        
