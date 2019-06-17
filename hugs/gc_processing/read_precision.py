import pandas as pd

def precision_read(filepath=None):
    filepath = "capegrim-medusa.18.precisions.C"

    def parser(date): return pd.datetime.strptime(date, '%y%m%d')
    
    # Read precision species
    precision_header = pd.read_csv(filepath, skiprows=3, nrows=2, header=None, sep=r"\s+")
    precision_species = precision_header.values[0][1:].tolist()

    

    # Read precisions
    precision = pd.read_csv(filepath, skiprows=5,header=None,sep=r"\s+", 
                            dtype=str,index_col=0,parse_dates=[0],date_parser=parser)
    
    precision.index.name = "Datetime"
    precision.drop_duplicates(subset="Datetime", inplace=True)

    return precision, precision_species

    



if __name__ == "__main__":
    precision_read()
