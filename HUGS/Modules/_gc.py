from enum import Enum as _Enum

__all__ = ["GC"]

# Enum or read from JSON?
# JSON might be easier to change in the future
class sampling_period(_Enum):
    GCMD = 75
    GCMS = 1200
    MEDUSA = 1200

def _test_data():
    """ Return the absolute path for data files used for testing purposes
    """
    import os as _os
    path = _os.path.dirname(_os.path.abspath(__file__)) + _os.path.sep + "../Data"
    print(path)
    return path

class GC:
    _gc_root = "GC"

    def __init__(self):
        self._uuid = None
        self._creation_datetime = None
        self._instruments = {}
        self._stored = False
        self._datasources = []

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
        from HUGS.ObjectStore import exists as _exists
        from HUGS.ObjectStore import get_bucket as _get_bucket

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
        data["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        data["instruments"] = self._instruments
        data["stored"] = self._stored
        data["datasources"] = self._datasources

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
        gc._datasources = data["datasources"]

        gc._stored = False
        
        return gc

    @staticmethod
    def load(uuid, key=None, bucket=None):
        """ Load a GC object from the object store

            Args:
                uuid (str): UUID of GC object
                key (str, default=None): Key of object in object store
                bucket (dict, default=None): Bucket to store object
            Returns:
                Datasource: Datasource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()
        
        if key is None:
            key = "%s/uuid/%s" % (GC._gc_root, uuid)
            
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)
        
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
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        self._stored = True
        gc_key = "%s/uuid/%s" % (GC._gc_root, self._uuid)
        _ObjectStore.set_object_from_json(bucket=bucket, key=gc_key, data=self.to_data())

    @staticmethod
    def read_file(data_filepath, precision_filepath):
        """ Reads a GC data file by creating a GC object and associated datasources

            Args:
                data_filepath (str): Path of data file
                precision_filepath (str): Path of precision file
            Returns:
                TODO - should this really return anything?
                GC: GC object

        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        from HUGS.Modules import Instrument as _Instrument
        from HUGS.Processing import Metadata as _Metadata
        from HUGS.Processing import create_datasources as _create_datasources

        gc_id = _create_uuid()

        if GC.exists(uuid=gc_id):
            gc = GC.load(uuid=gc_id)
        else:
            gc = GC.create()

        print("Remember to update the instrument!")
        # Where to get this from? User input?
        site = "CGO"
        instrument_name = "GCMD"

        gas_data = gc.read_data(data_filepath=data_filepath, precision_filepath=precision_filepath, 
                        site=site, instrument=instrument_name)
    
        # Create Datasources, save them to the object store and get their UUIDs
        datasource_uuids = _create_datasources(gas_data)
        # Add the Datasources to the list of datasources associated with this object
        gc.add_datasources(datasource_uuids)
        # Save object to object store
        gc.save()

        # For now return the GC object for easier testing
        return gc


    def read_data(self, data_filepath, precision_filepath, site, instrument):
        """ Read data from the data and precision files

            Args:
                data_filepath (str): Path of data file
                precision_filepath (str): Path of precision file
                site (str): Name of site
                instrument (str): Identifying data for instrument 
            Returns:
                list: List of tuples (str, dict, str, Pandas.Dataframe)

                Tuple contains species name, species metadata, datasource_uuid and dataframe
        """
        import json as _json
        from pandas import read_csv as _read_csv
        from pandas import datetime as _pd_datetime
        from pandas import Timedelta as _pd_Timedelta

        import HUGS.Processing.Metadata as _Metadata

        # Load in the parameters dictionary for processing data
        params_file = _test_data() + "/process_gcwerks_parameters.json"
        with open(params_file, "r") as FILE:
            self.params = _json.load(FILE)

        # Read header
        header = _read_csv(data_filepath, skiprows=2, nrows=2, header=None, sep=r"\s+")

        # Create a function to parse the datetime in the data file
        def parser(date): return _pd_datetime.strptime(date, '%Y %m %d %H %M')
        # Read the data in and automatically create a datetime column from the 5 columns
        # Dropping the yyyy', 'mm', 'dd', 'hh', 'mi' columns here
        df = _read_csv(data_filepath, skiprows=4, sep=r"\s+", index_col=["yyyy_mm_dd_hh_mi"],
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
        df = df.rename(columns=columns_renamed, inplace=False)

        # Read and parse precisions file
        precision, precision_species = self.read_precision(precision_filepath)

        for sp in species:
            precision_index = precision_species.index(sp) * 2 + 1
            df[sp + " repeatability"] = precision[precision_index].astype(float).reindex_like(df, method="pad")

        # instrument = "GCMD"
        # Apply timestamp correction, because GCwerks currently outputs the centre of the sampling period
        df["new_time"] = df.index - _pd_Timedelta(seconds=self.get_precision(instrument)/2.0)
        df = df.set_index("new_time", inplace=False, drop=True)
        df.index.name = "Datetime"

        self._proc_data = df
        self._species = species
        self._units = units
        self._scale = scale

        # Segment the processed data
        gas_data = self.split(site=site)
    
        return gas_data

    def read_precision(self, filepath):
        """ Read GC precision file

            Args: 
                filepath (str): Path of precision file
        """
        from pandas import read_csv as _read_csv
        from pandas import datetime as _pd_datetime

        # Function for parsing datetime
        def parser(date): return _pd_datetime.strptime(date, '%y%m%d')

        # Read precision species
        precision_header = _read_csv(filepath, skiprows=3, nrows=1, header=None, sep=r"\s+")

        precision_species = precision_header.values[0][1:].tolist()

        # Read precisions
        precision = _read_csv(filepath, skiprows=5, header=None, sep=r"\s+",
                                index_col=0, parse_dates=[0], date_parser=parser)

        precision.index.name = "Datetime"
        # Drop any duplicates from the index
        precision = precision.loc[~precision.index.duplicated(keep="first")]

        return precision, precision_species

    def split(self, site):
        """ Splits the dataframe into sections to be stored within individual Datasources

            Args:
                TODO - cleaner way of doing this?
                site (str): Name of site from which this data originates
            Returns:
                list (tuples): List of tuples of gas name and gas data

                Tuple of species name (str), metadata (dict), datasource_uuid (str), data (Pandas.DataFrame)
        """
        from fnmatch import fnmatch as _fnmatch
        from itertools import compress as _compress
        from uuid import uuid4 as _uuid4

        # Read inlets from the parameters dictionary
        expected_inlets = self.get_inlets(site=site)
        # Get the inlets in the dataframe
        data_inlets = self._proc_data["Inlet"].unique()

        # Check that each inlet in data_inlet matches one that's given by parameters file
        for data_inlet in data_inlets:
            match = [_fnmatch(data_inlet, inlet) for inlet in expected_inlets]
            if True in match:
                # Filter the expected inlets by the ones we've found in data
                # If none of them match processing below will not proceed
                matching_inlets = list(_compress(data_inlets, match))
            else:
                raise ValueError("Inlet mismatch - please ensure correct site is selected. Mismatch between inlet in \
                                  data and inlet in parameters file.")

        # TODO - where to get Datasource UUIDs from?
        # Also what to do in case of multiple inlets - each of these will have a unique ID
        # But may be of the same species ?
        gas_data = []
        for sp in self._species:
            # Check if the data for this species is all NaNs
            if self._proc_data[sp].isnull().all():
                continue

            for inlet in matching_inlets:
                metadata = {"inlet": inlet, "species": sp}
                # If we've only got a single inlet
                if inlet == "any" or inlet == "air":
                    dataframe = self._proc_data[[sp, sp + " repeatability", sp + " status_flag",  sp + " integration_flag", "Inlet"]]
                    dataframe = dataframe.dropna(axis="index", how="any")
                elif "date" in inlet:
                    dates = inlet.split("_")[1:]
                    slice_dict = {time: slice(dates[0], dates[1])}
                    data_sliced = self._proc_data.loc(slice_dict)
                    dataframe = data_sliced[[sp, sp + " repeatability", sp + " status_flag",  sp + " integration_flag", "Inlet"]]
                    dataframe = dataframe.dropna(axis="index", how="any")
                else:
                    # Take only data for this inlet from the dataframe
                    inlet_data = self._proc_data.loc[self._proc_data["Inlet"] == inlet]
                    dataframe = inlet_data[[sp, sp + " repeatability", sp + " status_flag",  sp + " integration_flag", "Inlet"]]
                    dataframe = dataframe.dropna(axis="index", how="any")
                    # TODO - change me
                    datasource_uuid = _uuid4()
                
                gas_data.append((sp, metadata, datasource_uuid, dataframe))

        return gas_data

    def add_instrument(self, instrument_id, value):
        """ Add an Instument to this object's dictionary of instruments

            Args:
                instrument_id (str): Instrument UUID
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

    def add_instrument(self, instrument_id, value):
        """ Add an Instument to this object's dictionary of instruments

            Args:
                instrument_id (str): Instrment UUID
                value (str): Value to describe Instrument
            Returns:
                None
        """
        self._instruments[instrument_id] = value

    def add_datasources(self, datasource_uuids):
        """ Add the passed list of Datasources to the current list

            Args:
                datasource_uuids (list): List of Datasource UUIDs
            Returns:
                None
        """
        self._datasources.extend(datasource_uuids)

    def datasources(self):
        """ Return the list of Datasources for this object

            Returns:
                list: List of Datasources
        """
        return self._datasources
        
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


    
    

