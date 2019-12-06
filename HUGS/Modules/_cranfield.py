
__all__ = ["Cranfield"]

class Cranfield:
    """ Interface for processnig Template data

        Instances of Template should be created using the
        CRDS.create() function
        
    """
    _cranfield_root = "Cranfield"
    _cranfield_uuid = "b3addb14-c182-4449-9d99-f7396c8ea624"

    def __init__(self):
        self._creation_datetime = None
        self._stored = False
        # Processed data
        self._proc_data = None
        # Datasource UUIDs
        # Keyed by name - allows retrieval of UUID from name
        self._datasource_names = {}
        # Keyed by UUID - allows retrieval of name by UUID
        self._datasource_uuids = {}
        # Hashes of previously uploaded files
        self._file_hashes = {}

    def is_null(self):
        """ Check if this is a null object

            Returns:
                bool: True if object is null
        """
        return self._datasource_names is None

    @staticmethod
    def create():
        """ This function should be used to create CRDS objects

            Returns:
                CRDS: CRDS object 
        """
        from Acquire.ObjectStore import get_datetime_now

        c = Cranfield()
        c._creation_datetime = get_datetime_now()

        return c

    @staticmethod
    def read_file(data_filepath, source_name=None, source_id=None, overwrite=False):
        """ Creates a CRDS object holding data stored within Datasources

            Args:
                filepath (str): Path of file to load
                data_filepath (str): Filepath of data to be read
                source_name (str): Name of data source
                overwrite (bool, default=False): Should data be overwritten
            Returns:
                Cranfield: Crandfield object
        """
        from HUGS.Processing import assign_data, lookup_gas_datasources
        from HUGS.Util import hash_file
        import os
        from pathlib import Path

        cranfield = Cranfield.load()
        
        file_hash = hash_file(filepath=data_filepath)
        if file_hash in cranfield._file_hashes and not overwrite:
            raise ValueError(f"This file has been uploaded previously with the filename : {cranfield._file_hashes[file_hash]}")
        
        # Ensure we have a string
        data_filepath = Path(data_filepath)
        gas_data = cranfield.read_data(data_filepath=data_filepath)

        if not source_name:
            source_name = data_filepath.stem

        # Check to see if we've had data from these Datasources before
        # TODO - currently just using a simple naming system here - update to use
        # an assigned UUID? Seems safer? How to give each gas a UUID?
        # This could be rolled into the assign_data function?
        lookup_results = lookup_gas_datasources(lookup_dict=cranfield._datasource_names, gas_data=gas_data,
                                                source_name=source_name, source_id=source_id)

        # Add in lookup of datasources for current data
        # This function should split the data into a format that
        # can be given to the Datasources. See the read_data function for this format
        gas_data = cranfield.read_data(data_filepath=data_filepath)

        # Create Datasources, save them to the object store and get their UUIDs
        datasource_uuids = assign_data(gas_data=gas_data, lookup_results=lookup_results, overwrite=overwrite)

        # Add the Datasources to the list of datasources associated with this object
        cranfield.add_datasources(datasource_uuids)

        cranfield.save()

        return datasource_uuids

    def read_data(self, data_filepath, data_type="CRDS"):
        """ Separates the gases stored in the dataframe in 
            separate dataframes and returns a dictionary of gases
           
            Args:
                data_filepath (pathlib.PosixPath): Path to data file
                data_type (str, default=CRDS): Type of data to be processed
            Returns:
                dict: Dictionary containing gas data and metadata
        """
        from pandas import read_csv

        if not data_type == "CRDS":
            raise NotImplementedError("Only CRDS can currently be processed")

        data = read_csv(data_filepath, parse_dates=["Date"], index_col = "Date")

        data = data.rename(columns = {"Methane/ppm": "ch4",
                                     "Methane stdev/ppm": "ch4 variability",
                                        "CO2/ppm": "co2",
                                        "CO2 stdev/ppm": "co2 variability",
                                        "CO/ppm": "co",
                                        "CO stdev/ppm": "co variability"})
        data.index.name = "time"

        # Convert CH4 and CO to ppb
        data["ch4"] = data["ch4"]*1e3
        data["ch4 variability"] = data["ch4 variability"]*1e3
        data["co"] = data["co"]*1e3
        data["co variability"] = data["co variability"]*1e3
        
        metadata = {}
        metadata["site"] = "THB" 
        metadata["instrument"] = "CRDS"
        metadata["time_resolution"] = "1_hour"
        metadata["height"] = "10magl"
        
        # TODO - this feels fragile
        species = [col for col in data.columns if " " not in col]
        
        combined_data = {}
        # Number of columns of data for each species
        n_cols = 2
        for n, sp in enumerate(species):
        # for sp in species:
            # Create a copy of the metadata dict
            species_metadata = metadata.copy()
            species_metadata["species"] = sp

            # Here we don't want to match the co in co2
            # For now we'll just have 2 columns for each species
            # cols = [col for col in data.columns if sp in col]
            gas_data = data.iloc[:, n*n_cols:(n+1)*n_cols]
            # gas_data = data[cols]
            combined_data[sp] = {"metadata": species_metadata, "data": gas_data}

        return combined_data

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string

        data = {}
        data["creation_datetime"] = datetime_to_string(self._creation_datetime)
        data["stored"] = self._stored
        data["datasource_uuids"] = self._datasource_uuids
        data["datasource_names"] = self._datasource_names
        data["file_hashes"] = self._file_hashes
    
        return data

    @staticmethod
    def from_data(data, bucket=None):
        """ Create a CRDS object from data

            Args:
                data (str): JSON data
                bucket (dict, default=None): Bucket for data storage
            Returns:
                CRDS: CRDS object created from data
        """
        from Acquire.ObjectStore import string_to_datetime 
        from HUGS.ObjectStore import get_bucket

        if data is None or len(data) == 0:
            return Cranfield()

        if bucket is None:
            bucket = get_bucket()
        
        c = Cranfield()

        c._creation_datetime = string_to_datetime(data["creation_datetime"])
        stored = data["stored"]

        c._datasource_uuids = data["datasource_uuids"]
        c._datasource_names = data["datasource_names"]
        c._file_hashes = data["file_hashes"]
        c._stored = False
        
        return c

    def save(self, bucket=None):
        """ Save the object to the object store

            Args:
                bucket (dict, default=None): Bucket for data
            Returns:
                None
        """
        from Acquire.ObjectStore import ObjectStore
        from HUGS.ObjectStore import get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = "%s/uuid/%s" % (Cranfield._cranfield_root, Cranfield._cranfield_uuid)

        self._stored = True
        ObjectStore.set_object_from_json(bucket=bucket, key=key, data=self.to_data())

    @staticmethod
    def load(bucket=None):
        """ Load a CRDS object from the datastore using the passed
            bucket and UUID

            Args:
                uuid (str): UUID of CRDS object
                key (str, default=None): Key of object in object store
                bucket (dict, default=None): Bucket to store object
            Returns:
                Datasource: Datasource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore
        from HUGS.ObjectStore import get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = "%s/uuid/%s" % (Cranfield._cranfield_root, Cranfield._cranfield_uuid)
        data = ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return Cranfield.from_data(data=data, bucket=bucket)

    @staticmethod
    def exists(bucket=None):
        """ Query the object store to check if a Cranfield object already exists

            Args:
                bucket (dict, default=None): Bucket for data storage
            Returns:
                bool: True if Cranfield object exists in object store
        """
        from HUGS.ObjectStore import exists
        from HUGS.ObjectStore import get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = "%s/uuid/%s" % (Cranfield._cranfield_root, Cranfield._cranfield_uuid)

        return exists(bucket=bucket, key=key)

    def add_datasources(self, datasource_uuids):
        """ Add the passed list of Datasources to the current list

            Args:
                datasource_uuids (list): List of Datasource UUIDs
            Returns:
                None
        """
        self._datasource_names.update(datasource_uuids)
        # Invert the dictionary to update the dict keyed by UUID
        uuid_keyed = {v: k for k, v in datasource_uuids.items()}
        self._datasource_uuids.update(uuid_keyed)

    def uuid(self):
        """ Return the UUID of this object

            Returns:
                str: UUID of  object
        """
        return Cranfield._crds_uuid

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

            This will also clear any file hashes

            Returns:
                None
        """
        self._datasource_uuids.clear()
        self._datasource_names.clear()
        self._file_hashes.clear()

        self.save()
