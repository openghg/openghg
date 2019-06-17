import pandas as pd

def read_precision(filepath=None):
    filepath = "capegrim-medusa.18.precisions.C"

    def parser(date): return pd.datetime.strptime(date, '%y%m%d')
    
    # Read precision species
    precision_header = pd.read_csv(filepath, skiprows=3, nrows=1, header=None, sep=r"\s+")

    precision_species = precision_header.values[0][1:].tolist()

    # Read precisions
    precision = pd.read_csv(filepath, skiprows=5,header=None,sep=r"\s+", 
                            dtype=str,index_col=0,parse_dates=[0],date_parser=parser)

    precision.index.name = "Datetime"
    # Drop any duplicates from the index
    precision = precision.loc[~precision.index.duplicated(keep="first")]

    return precision, precision_species



if __name__ == "__main__":
    precision_read()
