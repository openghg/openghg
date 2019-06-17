import pandas as pd
import datetime
import numpy as np

from old_functions import gc_data_read_original


def read_data(filepath):
    # Read header
    header = pd.read_csv(filepath, skiprows=2, nrows=2, header=None, sep=r"\s+")

    # Create a function to parse the datetime in the data file
    def parser(date): return pd.datetime.strptime(date, '%Y %m %d %H %M')
    # Read the data in and automatically create a datetime column from the 5 columns
    # Dropping the yyyy', 'mm', 'dd', 'hh', 'mi' columns here
    df = pd.read_csv(filepath, skiprows=4, sep=r"\s+", index_col=["yyyy_mm_dd_hh_mi"],
                                parse_dates=[[1, 2, 3, 4, 5]], date_parser=parser)
    df.index.name = "Datetime"

    species = []
    columns_renamed = {}
    for column in df.columns:
        if "Flag" in column:
            # Location of this column in a range (0, n_columns-1)
            col_loc = df.columns.get_loc(column)
            # Get name of column before this one for the gas name
            gas_name = df.columns[col_loc - 1]
            # Add it to the dictionary for renaming later
            columns_renamed[column] = gas_name + "_flag"
            # Create 2 new columns based on the flag columns
            df[gas_name + " status_flag"] = (df[column].str[0] != "-").astype(int)
            df[gas_name + " integration_flag"] = (df[column].str[1] != "-").astype(int)

            col_shift = 4
            units[gas_name] = header.iloc[1, col_loc + col_shift]
            scale[gas_name] = header.iloc[0, col_loc + col_shift]
            
            # Ensure the units and scale have been read in correctly
            # Have this in case the column shift between the header and data changes            
            if units[gas_name] == "--" or scale[gas_name] == "--":
                raise ValueError("Error reading units and scale, ensure columns are correct between header and dataframe")

            species.append(gas_name)

    # Rename columns to include the gas this flag represents        
    df.rename(columns=columns_renamed, inplace=True)

    # print(df)
    return df, species, units, scale


if __name__ == "__main__":

    data_file = "capegrim-medusa.18.C"
    scale = {}
    units = {}
    #df, species, units, scale = 
    # df_old, _, _, _ = gc_data_read_original(data_file, scale=scale, units=units)
    # print("\n\n\n")
    df = read_data(data_file)

    # df_old.index.name = "Datetime"


    # # print(df, "\n")
    # # print(df_old)

    # diff = list(set(df.columns) - set(df_old.columns))

    # print("New df\n", df.columns, "\n\n\n")

    # print("Old df\n", df_old.columns)

    # df_old.drop(['yyyy', 'mm', 'dd', 'hh', 'mi'], axis="columns", inplace=True)

    # print(df_old.equals(df))

    # print(diff)
    # print(df.columns)

    # print((df != df_old).any(axis="columns"))


    # print(species, "\n")
    # print(units, "\n")
    # print(scale, "\n")

    # print(df)


