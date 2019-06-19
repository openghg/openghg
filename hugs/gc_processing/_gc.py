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
    _gc_root = "GC"

    def __init__(self):
        self._uuid = None
        self._creation_datetime = None
        self._instruments = {}
        self._stored = False

    @staticmethod
    def create():
        """ This function should be used to create GC objects

            Returns:
                GC: GC object 
        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        gc = GC()
        gc._uuid = _create_uuid()
        gc._creation_datetime = _get_datetime_now()

        return gc

    def is_null(self):
        """ Check if this is a null object

            Returns:
                bool: True if object is null
        """
        return self._uuid is None

        
    

    @staticmethod
    def exists(uuid, bucket=None):
        """ Check if an object with the passed UUID exists in 
            the object store

            Args:
                uuid (str): UUID of GC object
                bucket (dict, default=None): Bucket for data storage
            Returns:
                bool: True if object exists
        """
        from objectstore import exists as _exists
        from objectstore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        # Query object store for Instrument
        return _exists(bucket=bucket, uuid=uuid)
        

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        if self.is_null():
            return {}

        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        data = {}
        data["uuid"] = self._uuid
        data["creation_datetime"] = self._creation_datetime
        data["instruments"] = self._instruments
        data["stored"] = self._stored

        return data

    @staticmethod
    def from_data(data, bucket=None):
        """ Create a GC object from data

            Args:
                data (dict): JSON data
                bucket (dict, default=None): Bucket for data storage
        """ 
        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime

        if data is None or len(data) == 0:
            return GC()
        
        gc = GC()
        gc._uuid = data["uuid"]
        gc._creation_datetime = data["creation_datetime"]
        gc._instruments = data["instruments"]
        stored = data["stored"]

        gc._stored = False
        
        return gc

    @staticmethod
    def load(uuid, key=None, bucket=None):
        """
            Args:
                uuid (str): UUID of GC object
                key (str, default=None): Key of object in object store
                bucket (dict, default=None): Bucket to store object
            Returns:
                Datasource: Datasource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from objectstore._hugs_objstore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()
        
        if key is None:
            key = "%s/uuid/%s" % (GC._gc_root, uuid)
            
        data = _ObjectStore.get_object_from_json(bucket=budket, key=key)
        
        return GC.from_data(data=data, bucket=bucket)

    def save(self, bucket=None):
        """ Save this GC object in the object store

            Args:
                bucket (dict): Bucket for data storage
            Returns:
                None
        """
        if self.is_null():
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        from objectstore._hugs_objstore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        self._stored = True
        gc_key = "%s/uuid/%s" % (GC._gc_root, self._uuid)
        _ObjectStore.set_object_from_JSON(bucket=bucket, key=gc_key, data=self.to_data())

    @staticmethod
    def read_file(data_filepath, precision_filepath):
        """ Reads a GC data file by creating a GC object and associated datasources

        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        from modules import Instrument as _Instrument
        from processing import Metadata as _Metadata

        # Load in the parameters dictionary for processing data
        params_file = "process_gcwerks_parameters.json"
        with open(params_file, "r") as FILE:
            self.params = json.load(FILE)

        data_file = "capegrim-medusa.18.C"
        precision_file = "capegrim-medusa.18.precisions.C"

        gc_id = _create_uuid()

        if GC.exists(uuid=gc_id):
            gc = GC.load(uuid=gc_id)
        else:
            gc = GC.create()

        # Where to get this from? User input?
        instrument_name = "GCMD"
        instrument_id = _create_uuid()

        if _Instrument.exists(uuid=instrument_id):
            instrument = _Instrument.load(uuid)
        else:
            instrument = _Instrument.create(name=instrument_name)

        # Do we need this metadata?
        # metadata = _Metadata
        gc.parse_data(data_filepath=datafile, precision_filepath=precision_file, instrument=instrument_name)
        # Save to object store
        gc.save()

        # Read in the parameters file just when reading in the file.
        # Save it but don't save it to the object store as part of this object


   def parse_data(self, data_filepath, precision_filepath, instrument):
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
                    dataframe = dataframe.dropna(axis="index", how="any")
                    gas_data.append((sp, dataframe))
                else:
                    dataframe = data[[sp, sp + " repeatability", sp + " status_flag",  sp + " integration_flag", "Inlet"]]                    
                    dataframe = dataframe.dropna(axis="index", how="any")
                    gas_data.append((sp, dataframe))
            # For multiple inlets
            else:
                for data_inlet in data_inlets:
                    dataframe = data[data["Inlet"] == data_inlet]
                    dataframe = dataframe.dropna(axis="index", how="any")
                    gas_data.append((sp, dataframe))

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

    def add_instrument(self, instrument_id, value):
       """ Add an Instument to this object's dictionary of instruments

            Args:
                instrument_id (str): Instrment UUID
                value (str): Value to describe Instrument
            Returns:
                None
        """
        self._instruments[instrument_id] = value

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

    


    
    

