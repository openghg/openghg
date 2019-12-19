from enum import Enum
from HUGS.Modules import BaseModule

__all__ = ["GC"]

# Enum or read from JSON?
# JSON might be easier to change in the future
class sampling_period(Enum):
    GCMD = 75
    GCMS = 1200
    MEDUSA = 1200


def _test_data():
    """ Return the absolute path for data files used for testing purposes
    """
    import os as _os
    path = _os.path.dirname(_os.path.abspath(__file__)) + _os.path.sep + "../Data"
    return path


class GC(BaseModule):
    _gc_root = "GC"
    _gc_uuid = "8cba4797-510c-gcgc-8af1-e02a5ee57489"

    def __init__(self):
        # self._uuid = None
        self._creation_datetime = None
        self._stored = False
        self._datasources = []
        # Keyed by name - allows retrieval of UUID from name
        self._datasource_names = {}
        # Keyed by UUID - allows retrieval of name by UUID
        self._datasource_uuids = {}
        # Hashes of previously uploaded files
        self._file_hashes = {}
        # Site codes for inlet readings
        self._site_codes = {}
        self._params = {}

    def is_null(self):
        """ Check if this is a null object

            Returns:
                bool: True if object is null
        """
        return len(self._datasource_uuids) == 0

    @staticmethod
    def exists(bucket=None):
        """ Check if a GC object is already saved in the object 
            store

            Args:
                bucket (dict, default=None): Bucket for data storage
            Returns:
                bool: True if object exists
        """
        from HUGS.ObjectStore import exists
        from HUGS.ObjectStore import get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = "%s/uuid/%s" % (GC._gc_root, GC._gc_uuid)
        return exists(bucket=bucket, key=key)

    @staticmethod
    def create():
        """ This function should be used to create GC objects

            Returns:
                GC: GC object 
        """
        from Acquire.ObjectStore import get_datetime_now

        gc = GC()
        gc._creation_datetime = get_datetime_now()

        return gc

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        data = {}
        data["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        data["stored"] = self._stored
        data["datasource_uuids"] = self._datasource_uuids
        data["datasource_names"] = self._datasource_names
        data["file_hashes"] = self._file_hashes

        return data

    @staticmethod
    def from_data(data, bucket=None):
        """ Create a GC object from data

            Args:
                data (dict): JSON data
                bucket (dict, default=None): Bucket for data storage
        """ 
        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime

        if not data:
            return GC()
        
        gc = GC()
        gc._creation_datetime = _string_to_datetime(data["creation_datetime"])
        gc._datasource_uuids = data["datasource_uuids"]
        gc._datasource_names = data["datasource_names"]
        gc._file_hashes = data["file_hashes"]
        gc._stored = False

        return gc

    def save(self, bucket=None):
        """ Save this GC object in the object store

            Args:
                bucket (dict, default=None): Bucket for data storage
            Returns:
                None
        """
        from Acquire.ObjectStore import ObjectStore
        from HUGS.ObjectStore import get_bucket

        if bucket is None:
            bucket = get_bucket()
        
        gc_key = "%s/uuid/%s" % (GC._gc_root, GC._gc_uuid)

        self._stored = True
        ObjectStore.set_object_from_json(bucket=bucket, key=gc_key, data=self.to_data())

    @staticmethod
    def load(bucket=None):
        """ Load a GC object from the object store

            Args:
                bucket (dict, default=None): Bucket to store object
            Returns:
                Datasource: Datasource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if not GC.exists():
            return GC.create()

        if bucket is None:
            bucket = _get_bucket()
        
        key = "%s/uuid/%s" % (GC._gc_root, GC._gc_uuid)
            
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)
        
        return GC.from_data(data=data, bucket=bucket)

    @staticmethod
    def read_file(data_filepath, precision_filepath, source_name, site, instrument_name="GCMD", 
                    source_id=None, overwrite=False):
        """ Reads a GC data file by creating a GC object and associated datasources

            Args:
                data_filepath (str, pathlib.Path): Path of data file
                precision_filepath (str, pathlib.Path): Path of precision file
            Returns:
                TODO - should this really return anything?
                GC: GC object

        """
        from HUGS.Processing import assign_data, lookup_gas_datasources
        import json
        from pathlib import Path

        gc = GC.load()

        # TODO - I feel this is messy, should be improved and the _test_data function
        # removed, create a json folder so there's a proper structure?
        # Load in the parameters dictionary for processing data
        
        # params_file = _test_data() + "/process_gcwerks_parameters.json"
        # with open(params_file, "r") as f:
        #     gc._params = json.load(f)
        # Load in the params for code_site, site_code dictionaries for selection
        # of inlets from the above parameters
        if not isinstance(data_filepath, Path):
            data_filepath = Path(data_filepath)

        data, species, metadata = gc.read_data(data_filepath=data_filepath, precision_filepath=precision_filepath, 
                                                site=site, instrument=instrument_name)

        gas_data = gc.split(data=data, site=site, species=species, metadata=metadata)

        # lookup_results = lookup_gas_datasources(gas_data=gas_data, source_name=source_name, source_id=source_id)
        lookup_results = lookup_gas_datasources(lookup_dict=gc._datasource_names, gas_data=gas_data,
                                                source_name=source_name, source_id=source_id)
    
        # Create Datasources, save them to the object store and get their UUIDs
        datasource_uuids = assign_data(gas_data=gas_data, lookup_results=lookup_results, overwrite=overwrite)
        # Add the Datasources to the list of datasources associated with this object
        gc.add_datasources(datasource_uuids)
        # Save object to object store
        gc.save()

        # Return the UUIDs of the datasources in which the data was stored
        return datasource_uuids

    # TODO - move this out to a module so we don't have it in two places, just
    # pass in the dict it searches
    # def lookup_datasources(self, lookup_dict, gas_data, source_name=None, source_id=None):
    #     """ Check which datasources hold data from this source

    #         Args: 
    #             gas_data (list): Gas data to process
    #             source_name (str)
    #         Returns:
    #             dict: Dictionary keyed by source_name. Value of Datasource UUID
    #     """
    #     # If we already have data from these datasources then return that UUID
    #     # otherwise return False
    #     if source_id is not None:
    #         raise NotImplementedError()

    #     results = {}

    #     for species in gas_data:
    #         datasource_name = source_name + "_" + species
    #         results[species] = {}
    #         results[species]["uuid"] = self._datasource_names.get(datasource_name, False)
    #         results[species]["name"] = datasource_name

    #     return results

    def read_data(self, data_filepath, precision_filepath, site, instrument):
        """ Read data from the data and precision files

            Args:
                data_filepath (pathlib.Path): Path of data file
                precision_filepath (pathlib.Path): Path of precision file
                site (str): Name of site
                instrument (str): Identifying data for instrument 
            Returns:
                list: List of tuples (str, dict, str, Pandas.Dataframe)

                Tuple contains species name, species metadata, datasource_uuid and dataframe
        """
        from pandas import read_csv as _read_csv
        from pandas import datetime as _pd_datetime
        from pandas import Timedelta as _pd_Timedelta
        from HUGS.Processing import read_metadata

        # Read header   
        header = _read_csv(data_filepath, skiprows=2, nrows=2, header=None, sep=r"\s+")

        # Create a function to parse the datetime in the data file
        def parser(date): return _pd_datetime.strptime(date, '%Y %m %d %H %M')
        # Read the data in and automatically create a datetime column from the 5 columns
        # Dropping the yyyy', 'mm', 'dd', 'hh', 'mi' columns here
        data = _read_csv(data_filepath, skiprows=4, sep=r"\s+", index_col=["yyyy_mm_dd_hh_mi"],
                         parse_dates=[[1, 2, 3, 4, 5]], date_parser=parser)
        data.index.name = "Datetime"

        metadata = read_metadata(filepath=data_filepath, data=data, data_type="GC")

        units = {}
        scale = {}

        species = []
        columns_renamed = {}
        for column in data.columns:
            if "Flag" in column:
                # Location of this column in a range (0, n_columns-1)
                col_loc = data.columns.get_loc(column)
                # Get name of column before this one for the gas name
                gas_name = data.columns[col_loc - 1]
                # Add it to the dictionary for renaming later
                columns_renamed[column] = gas_name + "_flag"
                # Create 2 new columns based on the flag columns
                data[gas_name + " status_flag"] = (data[column].str[0] != "-").astype(int)
                data[gas_name + " integration_flag"] = (data[column].str[1] != "-").astype(int)

                col_shift = 4
                units[gas_name] = header.iloc[1, col_loc + col_shift]
                scale[gas_name] = header.iloc[0, col_loc + col_shift]

                # Ensure the units and scale have been read in correctly
                # Have this in case the column shift between the header and data changes
                if units[gas_name] == "--" or scale[gas_name] == "--":
                    raise ValueError("Error reading units and scale, ensure columns are correct between header and dataframe")

                species.append(gas_name)

        # Rename columns to include the gas this flag represents
        data = data.rename(columns=columns_renamed, inplace=False)

        # Read and parse precisions file
        precision, precision_species = self.read_precision(precision_filepath)

        for sp in species:
            precision_index = precision_species.index(sp) * 2 + 1
            data[sp + " repeatability"] = precision[precision_index].astype(float).reindex_like(data, method="pad")

        # Apply timestamp correction, because GCwerks currently outputs the centre of the sampling period
        data["new_time"] = data.index - _pd_Timedelta(seconds=self.get_precision(instrument)/2.0)
        data = data.set_index("new_time", inplace=False, drop=True)
        data.index.name = "Datetime"
        
        return (data, species, metadata)

    def read_precision(self, filepath):
        """ Read GC precision file

            Args: 
                filepath (pathlib.Path): Path of precision file
            Returns:
                tuple (Pandas.DataFrame, list): Precision DataFrame and list of species in
                precision data
        """
        from pandas import read_csv as _read_csv
        from pandas import datetime as _pd_datetime

        # Function for parsing datetime
        def parser(date): return _pd_datetime.strptime(date, '%y%m%d')

        # Read precision species
        precision_header = _read_csv(filepath, skiprows=3, nrows=1, header=None, sep=r"\s+")

        precision_species = precision_header.values[0][1:].tolist()

        precision = _read_csv(filepath, skiprows=5, header=None, sep=r"\s+",
                                index_col=0, parse_dates=[0], date_parser=parser)
        precision.index.name = "Datetime"
        # Drop any duplicates from the index
        precision = precision.loc[~precision.index.duplicated(keep="first")]

        return precision, precision_species

    def split(self, data, site, species, metadata):
        """ Splits the dataframe into sections to be stored within individual Datasources

            Args:
                data (Pandas.DataFrame): DataFrame of raw data
                site (str): Name of site from which this data originates
                species (list): List of species contained in data
                metadata (dict): Dictionary of metadata
            Returns:
                list (tuples): List of tuples of gas name and gas data

                Tuple of species name (str), metadata (dict), datasource_uuid (str), data (Pandas.DataFrame)
        """
        from fnmatch import fnmatch
        from itertools import compress

        
        site_code = self.get_site_code(site)
        # Read inlets from the parameters dictionary
        expected_inlets = self.get_inlets(site_code=site_code)
        # Get the inlets in the dataframe
        try:
            data_inlets = data["Inlet"].unique()
        except KeyError:
            raise KeyError("Unable to read inlets from data, please ensure this data is of the GC \
                                    type expected by this processing module")
        # TODO - ask Matt/Rachel about inlets
        matching_inlets = data_inlets

        # # For now just add air to the expected inlets
        # expected_inlets.append("air")
        # # Check that each inlet in data_inlet matches one that's given by parameters file
        # for data_inlet in data_inlets:
        #     match = [fnmatch(data_inlet, inlet) for inlet in expected_inlets]
        #     if True in match:
        #         # Filter the expected inlets by the ones we've found in data
        #         # If none of them match processing below will not proceed
        #         matching_inlets = list(compress(data_inlets, match))
        #     else:
        #         raise ValueError("Inlet mismatch - please ensure correct site is selected. Mismatch between inlet in \
        #                           data and inlet in parameters file.")

        # TODO - where to get Datasource UUIDs from?
        # Also what to do in case of multiple inlets - each of these will have a unique ID
        # But may be of the same spec ?
        gas_data = []
        combined_data = {}

        for spec in species:
            # Check if the data for this spec is all NaNs
            if data[spec].isnull().all():
                continue

            # Create a copy of metadata for local modification
            spec_metadata = metadata.copy()
            spec_metadata["species"] = spec

            for inlet in matching_inlets:
                spec_metadata["inlet"] = inlet
                # If we've only got a single inlet
                if inlet == "any" or inlet == "air":
                    dataframe = data[[spec, spec + " repeatability", spec + " status_flag",  spec + " integration_flag", "Inlet"]]
                    dataframe = dataframe.dropna(axis="index", how="any")
                elif "date" in inlet:
                    dates = inlet.split("_")[1:]
                    slice_dict = {time: slice(dates[0], dates[1])}
                    data_sliced = data.loc(slice_dict)
                    dataframe = data_sliced[[spec, spec + " repeatability", spec + " status_flag",  spec + " integration_flag", "Inlet"]]
                    dataframe = dataframe.dropna(axis="index", how="any")
                else:
                    # Take only data for this inlet from the dataframe
                    inlet_data = data.loc[data["Inlet"] == inlet]
                    dataframe = inlet_data[[spec, spec + " repeatability", spec + " status_flag",  spec + " integration_flag", "Inlet"]]
                    dataframe = dataframe.dropna(axis="index", how="any")

                combined_data[spec] = {"metadata": spec_metadata, "data": dataframe}
        
        return combined_data

    def get_precision(self, instrument):
        """ Get the precision of the instrument in seconds

            Args:
                instrument (str): Instrument name
            Returns:
                int: Precision of instrument in seconds

        """
        if not self._params:
            self.load_params()

        return self._params["GC"]["sampling_period"][instrument]

    def get_inlets(self, site_code):
        """ Get the inlets used at this site

            Args:
                site (str): Site of datasources
            Returns:
                list: List of inlets
        """
        if not self._params:
            self.load_params()

        return self._params["GC"][site_code]["inlets"]

    def load_params(self):
        """ Load the parameters from file

            Returns:
                None
        """
        import json

        params_file = _test_data() + "/process_gcwerks_parameters.json"
        with open(params_file, "r") as f:
            self._params = json.load(f)

    def get_site_code(self, site):
        """ Get the site code

            Args:
                site (str): Name of site
            Returns:
                str: Site code
        """
        import json

        if not self._site_codes:
            site_codes_json = _test_data() + "/site_codes.json"
            with open(site_codes_json, "r") as f:
                d = json.load(f)
                self._site_codes = d["name_code"]

        try:
            site_code = self._site_codes[site.lower()]
        except KeyError:
            raise KeyError("Site not recognized")
        
        return site_code

    def add_datasources(self, datasource_uuids):
        """ Add the passed list of Datasources to the current list

            Args:
                datasource_uuids (dict): Dict of Datasource UUIDs
            Returns:
                None
        """
        self._datasource_names.update(datasource_uuids)
        # Invert the dictionary to update the dict keyed by UUID
        uuid_keyed = {v: k for k, v in datasource_uuids.items()}
        self._datasource_uuids.update(uuid_keyed)

    def datasources(self):
        """ Return the list of Datasources for this object

            Returns:
                list: List of Datasources
        """
        return self._datasource_names

    def remove_datasource(self, uuid):
        """ Remove the Datasource with the given uuid from the list 
            of Datasources

            Args:
                uuid (str): UUID of Datasource to be removed
        """
        del self._datasource_uuids[uuid]

    def clear_datasources(self):
        """ Remove all Datasources from the object

            Returns:
                None
        """
        self._datasource_uuids.clear()
        self._datasource_names.clear()
        self._file_hashes.clear()
            

