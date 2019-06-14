import pandas as pd
import datetime

def gc_data_read(dotC_file, scale = {}, units = {}):

    species = []

    # Read header
    header = pd.read_csv(dotC_file, skiprows=2,nrows=2,header = None,sep=r"\s+")

    # Read data
    parser = lambda date: pd.datetime.strptime(date, '%Y %m %d %H %M')
    df = pd.read_csv(dotC_file, skiprows=4, sep=r"\s+", index_col=["yyyy_mm_dd_hh_mi"], parse_dates=[[1, 2, 3, 4, 5]], date_parser=parser)
    df.index.name = "Datetime"

    # Will this ever be used?
    # df["analysis_time"] = time_analysis
    print(df)

    # Rename flag column with species name
    for i, key in enumerate(df.keys()):
        print(key[0:4])
        # If they key contains Flag
        if key[0:4] == "Flag":
            quality_flag = []
            area_height_flag = []
            print(df[key].values)
            # Iterates over the columns with flag as a header
            for flags in df[key].values:

                # Quality flag
                if flags[0] == "-":
                    quality_flag.append(0)
                else:
                    quality_flag.append(1)

                # Area/height
                if flags[1] == "-":
                    area_height_flag.append(0)  # Area
                else:
                    area_height_flag.append(1)  # Height

            # Add the previous column's name to the flag name
            df = df.rename(columns = {key: list(df.keys())[i-1] + "_flag"})
            
            # Create a new column with the previous column's name and status_flag
            df[list(df.keys())[i-1] + " status_flag"] = quality_flag
            # Same for area height / integration flag
            df[list(df.keys())[i-1] + " integration_flag"] = area_height_flag

            # Read header for scale and units
            scale[list(df.keys())[i-1]] = header[i-1][0]
            units[list(df.keys())[i-1]] = header[i-1][1]
            # Add the names of the species to the list
            species.append(list(df.keys())[i-1])

    return df, species, units, scale


if __name__ == "__main__":

    data_file = "capegrim-medusa.18.C"
    scale = {}
    units = {}
    gc_data_read(data_file, scale=scale, units=units)

    # print(df)


