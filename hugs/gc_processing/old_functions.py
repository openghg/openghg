import pandas as pd
from datetime import datetime as dt

def gc_data_read_original(dotC_file, scale={}, units={}):

    species = []

    # Read header
    header = pd.read_csv(dotC_file, skiprows=2, nrows=2,
                         header=None, sep=r"\s+")

    # Read data
    df = pd.read_csv(dotC_file, skiprows=4, sep=r"\s+")

    # Time index

    time = []
    time_analysis = []
    for i in range(len(df)):
        # sampling time
        time.append(dt(df.yyyy[i], df.mm[i], df.dd[i], df.hh[i], df.mi[i]))
        # Read analysis time
        if "ryyy" in list(df.keys()):
            time_analysis.append(
                dt(df.ryyy[i], df.rm[i], df.rd[i], df.rh[i], df.ri[i]))

    df.index = time
#    df["analysis_time"] = time_analysis

    # Drop duplicates
    df = df.reset_index().drop_duplicates(subset='index').set_index('index')

    # Rename flag column with species name
    for i, key in enumerate(df.keys()):
        if key[0:4] == "Flag":
            quality_flag = []
            area_height_flag = []
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

            df = df.rename(columns={key: list(df.keys())[i-1] + "_flag"})

            df[list(df.keys())[i-1] + " status_flag"] = quality_flag

            df[list(df.keys())[i-1] + " integration_flag"] = area_height_flag

            scale[list(df.keys())[i-1]] = header[i-1][0]

            units[list(df.keys())[i-1]] = header[i-1][1]

            species.append(list(df.keys())[i-1])

    return df, species, units, scale
