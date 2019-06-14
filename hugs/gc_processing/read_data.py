import pandas as pd
import datetime
import numpy as np

from old_functions import gc_data_read_original


def gc_data_read_mine(dotC_file, scale={}, units={}):
    species = []

    # Read header
    header = pd.read_csv(dotC_file, skiprows=2, nrows=2, header=None, sep=r"\s+")

    # Create a function to parse the datetime in the data file
    def parser(date): return pd.datetime.strptime(date, '%Y %m %d %H %M')

    # Read the data in and automatically create a datetime column from the 5 columns
    # Dropping the yyyy', 'mm', 'dd', 'hh', 'mi' columns here
    df = pd.read_csv(dotC_file, skiprows=4, sep=r"\s+", index_col=["yyyy_mm_dd_hh_mi"], 
                                parse_dates=[[1, 2, 3, 4, 5]], date_parser=parser)
    df.index.name = "Datetime"

    columns_renamed = {}
    for column in df.columns:
        if "Flag" in column:
            # Cleaner way to do this?
            # Get name of column before this one
            prev_name = df.columns[df.columns.get_loc(column) - 1]
            # Add it to the dictionary for renaming later
            columns_renamed[column] = prev_name + "_flag"
            # Create 2 new columns based on the flag columns
            df[prev_name + " status_flag"] = (df[column].str[0] != "-").astype(int)
            df[prev_name + " integration_flag"] = (df[column].str[1] != "-").astype(int)

    # Rename columns to include the gas this flag represents        
    df.rename(columns=columns_renamed, inplace=True)

    # print(df)
    return df


if __name__ == "__main__":

    data_file = "capegrim-medusa.18.C"
    scale = {}
    units = {}
    #df, species, units, scale = 
    df_old, _, _, _ = gc_data_read_original(data_file, scale=scale, units=units)
    print("\n\n\n")
    df = gc_data_read_mine(data_file, scale=scale, units=units)

    df_old.index.name = "Datetime"


    # print(df, "\n")
    # print(df_old)

    diff = list(set(df.columns) - set(df_old.columns))

    print("New df\n", df.columns, "\n\n\n")

    print("Old df\n", df_old.columns)

    df_old.drop(['yyyy', 'mm', 'dd', 'hh', 'mi'], axis="columns", inplace=True)

    print(df_old.equals(df))

    # print(diff)
    # print(df.columns)

    # print((df != df_old).any(axis="columns"))


    # print(species, "\n")
    # print(units, "\n")
    # print(scale, "\n")

    # print(df)


