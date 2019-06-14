import json
import pandas as pd


def read_precision(filepath=None):
    filepath = "capegrim-medusa.18.precisions.C"

    def parser(date): return pd.datetime.strptime(date, '%y%m%d')

    # Read precision species
    precision_header = pd.read_csv(
        filepath, skiprows=3, nrows=2, header=None, sep=r"\s+")
    precision_species = precision_header.values[0][1:].tolist()

    # Read precisions
    precision = pd.read_csv(filepath, skiprows=5, header=None, sep=r"\s+",
                            dtype=str, index_col=0, parse_dates=[0], date_parser=parser)

    precision.index.name = "Datetime"
    precision.drop_duplicates(subset="Datetime", inplace=True)

    return precision, precision_species


def read_data(filepath):
    # Read header
    header = pd.read_csv(filepath, skiprows=2, nrows=2,header=None, sep=r"\s+")

    # Create a function to parse the datetime in the data file
    def parser(date): return pd.datetime.strptime(date, '%Y %m %d %H %M')

    # Read the data in and automatically create a datetime column from the 5 columns
    # Dropping the yyyy', 'mm', 'dd', 'hh', 'mi' columns here
    df = pd.read_csv(filepath, skiprows=4, sep=r"\s+", index_col=["yyyy_mm_dd_hh_mi"],
                     parse_dates=[[1, 2, 3, 4, 5]], date_parser=parser)
    df.index.name = "Datetime"

    species = []
    # Store columns to rename
    columns_to_rename = {}
    # Scale
    scale = {}
    units = {}

    for column in df.columns:
        if "Flag" in column:
            # Cleaner way to do this?
            # Get name of column before this one
            prev_col = df.columns[df.columns.get_loc(column) - 1]
            species.append(prev_col)
            # Add it to the dictionary for renaming later
            columns_to_rename[column] = prev_col + "_flag"
            # Create 2 new columns based on the flag columns
            df[prev_col + " status_flag"] = (df[column].str[0] != "-").astype(int)
            df[prev_col + " integration_flag"] = (df[column].str[1] != "-").astype(int)
            # Store the scale and units from the header
            scale[prev_col] = header[i-1][0]
            units[prev_col] = header[i-1][1]

    # Rename columns to include the gas this flag represents
    df.rename(columns=columns_to_rename, inplace=True)

    return df, species, scale, units

def gc():
    
    """
    Process GC data per site and instrument
    Instruments can be:
        "GCMD": GC multi-detector (output will be labeled GC-FID or GC-ECD)
        "GCMS": GC ADS (output GC-ADS)
        "medusa": GC medusa (output GC-MEDUSA)

    Network is the network name for output file.
    """
    
    # Load in the params JSON
    # params_file = "process_gcwerks_parameters.json"
    # with open(params_file, "r") as FILE:
    #     params = json.load(FILE)

    data_file = "capegrim-medusa.18.C"
    precision_file = "capegrim-medusa.18.precisions.C"

    df, species, units, scale = read_data(data_file)
    precision, precision_species = read_precision(precision_file)

    # Merge precisions into dataframe
    for sp in species:
        precision_index = precision_species.index(sp)*2+1

        print(precision_index)

        # df[sp + " repeatability"] = precision[precision_index].astype(float).reindex_like(df, "pad")


    
if __name__ == "__main__":
    gc()
    


    
    

