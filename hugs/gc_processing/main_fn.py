from enum import Enum
import json
import pandas as pd

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

        # Read in the parameters file just when reading in the file.
        # Save it but don't save it to the object store as part of this object

    def get_precision_json(self, instrument):
        """ Get the precision of the instrument in seconds

            Args:
                instrument (str): Instrument name
            Returns:
                int: Precision of instrument in seconds

        """
        return self.params["GC"]["sampling_period"][instrument]


    def get_inlets(site):
        """ Get the inlets used at this site

            Args:
                site (str): Site of datasources
            Returns:
                list: List of inlets
        """
        return self.params["GC"][site]["inlets"]

    def gc(self, instrument):
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

    df, species, units, scale = read_data(data_file)
    precision, precision_species = read_precision(precision_file)

    # TODO - tidy this ?
    for sp in species:
        precision_index = precision_species.index(sp) * 2 + 1
        df[sp + " repeatability"] = precision[precision_index].astype(
            float).reindex_like(df, method="pad")

    # instrument = "GCMD"
    # Apply timestamp correction, because GCwerks currently outputs the centre of the sampling period
    df["new_time"] = df.index - \
        pd.Timedelta(seconds=get_precision_json(instrument)/2.0)
    df.set_index("new_time", inplace=True, drop=True)

    get_inlets("HFD")


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
    gc(instrument=instrument)
    


    
    

