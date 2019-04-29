import pandas as pd

def get_metadata(data_file):
    metadata = []

    # Skip the first row which contains the date the file was created on
    # Read the next two rows
    df_header = pd.read_csv(data_file, skiprows=1, nrows=2, header=None, sep=r"\s+")

    # No nee dto reinterpret the header string
    # Create header list

    # Meta data
    # daterange in file
    # type and port?
    # Take the 3 gases from the file and create an xarray from them?

    # Query the gases in the file

    # Gas data
    # Each gas gets its own UID
    # Within one piece of data take c, stdev and count number
    # This will contain the 
    
    for i in df_header.columns:
        # Here i is an integer starting at 1
        # Ignore the metadata - 
        if df_header[i][0] != '-':
            metadata.append(df_header[i][0].upper() + df_header[i][1])

            # This takes in the readings 
            if df_header[i][1] == "C":
                species.append(df_header[i][0].upper())
        else:
            header.append(df_header[i][1].upper())
