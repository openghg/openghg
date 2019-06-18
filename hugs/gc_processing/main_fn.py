from enum import Enum
import fnmatch
import json
import pandas as pd
import xarray as xray

from read_precision import read_precision
from read_data import read_data

# Enum or read from JSON?
# JSON might be easier to change in the future
class sampling_period(Enum):
    GCMD = 75
    GCMS = 1200
    MEDUSA = 1200


class GC:
    def __init__(self):
        """ Some basics

        """
        self.uuid = None
        self.create_datetime = None

    @staticmethod
    def create():
        """ Used to create a GC class

        """
        gc = GC()

    def read_file(self):
        # Load in the parameters dictionary for processing data
        params_file = "process_gcwerks_parameters.json"
        with open(params_file, "r") as FILE:
            self.params = json.load(FILE)

        instrument = "GCMD"
        self.parse_data(instrument=instrument)

        # Read in the parameters file just when reading in the file.
        # Save it but don't save it to the object store as part of this object

    def get_precision(self, instrument):
        """ Get the precision of the instrument in seconds

            Args:
                instrument (str): Instrument name
            Returns:
                int: Precision of instrument in seconds

        """
        return self.params["GC"]["sampling_period"][instrument]

    def get_inlets(self, site):
        """ Get the inlets used at this site

            Args:
                site (str): Site of datasources
            Returns:
                list: List of inlets
        """
        return self.params["GC"][site]["inlets"]

    def parse_data(self, instrument):
        """
        Process GC data per site and instrument
        Instruments can be:
            "GCMD": GC multi-detector (output will be labeled GC-FID or GC-ECD)
            "GCMS": GC ADS (output GC-ADS)
            "medusa": GC medusa (output GC-MEDUSA)
        """
        # Load in the params JSON
        params_file = "process_gcwerks_parameters.json"
        with open(params_file, "r") as FILE:
            params = json.load(FILE)

        data_file = "capegrim-medusa.18.C"
        precision_file = "capegrim-medusa.18.precisions.C"

        df, species, units, scale = self.read_data(data_file)
        precision, precision_species = self.read_precision(precision_file)

        # TODO - tidy this ?
        for sp in species:
            precision_index = precision_species.index(sp) * 2 + 1
            df[sp + " repeatability"] = precision[precision_index].astype(float).reindex_like(df, method="pad")

        # instrument = "GCMD"
        # Apply timestamp correction, because GCwerks currently outputs the centre of the sampling period
        df["new_time"] = df.index - pd.Timedelta(seconds=self.get_precision(instrument)/2.0)
        df.set_index("new_time", inplace=True, drop=True)
        df.index.name = "Datetime"

        site = "CGO"
        inlets = self.get_inlets(site=site)
        self.segment(species=species, site=site, data=df)

    def segment(self, species, site, data):
        """ Splits the dataframe into sections to be stored within individual Datasources

            WIP

            Returns:
                list (str, Pandas.DataFrame): List of tuples of gas name and gas data
        """
        import fnmatch as _fnmatch
        # import re as _re

        gas_data = []

        # Read inlets from the parameters dictionary
        expected_inlets = self.get_inlets(site=site)
        # Get the inlets in the dataframe
        data_inlets = data["Inlet"].unique()
        # Check that each inlet in data_inlet matches one that's given by parameters file
        for data_inlet in data_inlets:
            match = [fnmatch.fnmatch(data_inlet, inlet) for inlet in expected_inlets]
            if True not in match:
                raise ValueError("Inlet mismatch - please ensure correct site is selected. Mismatch between inlet in \
                                  data and inlet in parameters file.")

        for sp in species:
            # If we've only got a single inlet
            if len(data_inlets) == 1:
                data_inlet = data_inlets[0]
                # Not sure we need to save this
                # clean_inlet_height = _re.search(r"\d+m", s).group()
                # Split by date
                if "date" in data_inlet:
                    dates = inlet.split("_")[1:]
                    slice_dict = {time: slice(dates[0], dates[1])}
                    data_sliced = data.loc(slice_dict)
                    dataframe = data_sliced[[sp, sp + " repeatability", sp + " status_flag",  sp + " integration_flag", "Inlet"]]
                    gas_data.append((sp, dataframe))
                else:
                    dataframe = data[[sp, sp + " repeatability", sp + " status_flag",  sp + " integration_flag", "Inlet"]]
                    gas_data.append((sp, dataframe))
            # For multiple inlets
            else:
                for data_inlet in data_inlets:
                    dataframe = data[data["Inlet"] == data_inlet]
                    gas_data.append((sp, dataframe))

        print(gas_data)
        return gas_data


    def read_data(self, filepath):
            # Read header
        header = pd.read_csv(filepath, skiprows=2, nrows=2,
                            header=None, sep=r"\s+")

        # Create a function to parse the datetime in the data file
        def parser(date): return pd.datetime.strptime(date, '%Y %m %d %H %M')
        # Read the data in and automatically create a datetime column from the 5 columns
        # Dropping the yyyy', 'mm', 'dd', 'hh', 'mi' columns here
        df = pd.read_csv(filepath, skiprows=4, sep=r"\s+", index_col=["yyyy_mm_dd_hh_mi"],
                        parse_dates=[[1, 2, 3, 4, 5]], date_parser=parser)
        df.index.name = "Datetime"

        units = {}
        scale = {}

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

    def read_precision(self, filepath=None):
        filepath = "capegrim-medusa.18.precisions.C"

        def parser(date): return pd.datetime.strptime(date, '%y%m%d')

        # Read precision species
        precision_header = pd.read_csv(filepath, skiprows=3, nrows=1, header=None, sep=r"\s+")

        precision_species = precision_header.values[0][1:].tolist()

        # Read precisions
        precision = pd.read_csv(filepath, skiprows=5, header=None, sep=r"\s+",
                                dtype=str, index_col=0, parse_dates=[0], date_parser=parser)

        precision.index.name = "Datetime"
        # Drop any duplicates from the index
        precision = precision.loc[~precision.index.duplicated(keep="first")]

        return precision, precision_species


    # def get_precision(instrument):
    #     """ Get the precision in seconds of the passed instrument

    #         Args:
    #             instrument (str): Instrument precision
    #         Returns:
    #             int: Precision of instrument in seconds
    #     """
    #     return sampling_period[instrument.upper()].value

    # Can split into species?
    # Create a datasource for each species?


# def segment(df, species):
#     """ Segment the data by species
#         Each gas will be a separate Datasource ?

#     """ 
#     # Check which inlet this site has
#     # This function can call multiple functions to segment the dataframe

#     for sp in species:


    
if __name__ == "__main__":
    instrument = "GCMD"
    gc = GC()
    gc.read_file()

    


    
    

