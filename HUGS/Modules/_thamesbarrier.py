from HUGS.Modules import BaseModule

__all__ = ["ThamesBarrier"]


class ThamesBarrier(BaseModule):
    """ Interface for processing ThamesBarrier data

    """
    _root = "ThamesBarrier"
    # Use uuid.uuid4() to create a unique fixed UUID for this object
    _uuid = "e708ab3f-ade5-402a-a491-979095d8f7ad"

    def __init__(self):
        from Acquire.ObjectStore import get_datetime_now
        
        self._creation_datetime = get_datetime_now()
        self._stored = False
        # self._datasources = []
        # Keyed by name - allows retrieval of UUID from name
        self._datasource_names = {}
        # Keyed by UUID - allows retrieval of name by UUID
        self._datasource_uuids = {}
        # Hashes of previously uploaded files
        self._file_hashes = {}
        # Holds parameters used for writing attributes to Datasets
        self._tb_params = {}
        # Sampling period of  data in seconds
        self._sampling_period = "NA"

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

    def save(self, bucket=None):
        """ Save the object to the object store

            Args:
                bucket (dict, default=None): Bucket for data
            Returns:
                None
        """
        from Acquire.ObjectStore import ObjectStore, string_to_encoded
        from HUGS.ObjectStore import get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = f"{ThamesBarrier._root}/uuid/{ThamesBarrier._uuid}"

        self._stored = True
        ObjectStore.set_object_from_json(bucket=bucket, key=key, data=self.to_data())

    @staticmethod
    def read_folder(folder_path, extension=".dat", recursive=True):
        """ Read all data matching filter in folder

            Args:
                folder_path (str): Path of folder
                extension (str, default=".dat"): File extension for data files in folder
                recursive (bool, default=True)
            Returns:
                None
        """
        from glob import glob
        from os import path

        datasource_uuids = {}

        # This finds data files in sub-folders
        folder_path = path.join(folder_path, f"./*.{extension}")
        # This finds files in the current folder, get recursive
        # folder_path = _path.join(folder_path, "*.dat")
        filepaths = glob(folder_path, recursive=recursive)

        if not filepaths:
            raise FileNotFoundError("No data files found")

        for fp in filepaths:
            datasource_uuids[fp] = ThamesBarrier.read_file(data_filepath=fp)
        
        return datasource_uuids

    @staticmethod
    def read_file(data_filepath, source_name, source_id=None, overwrite=False):
        """ Reads ThamesBarrier data files and returns the UUIDS of the Datasources
            the processed data has been assigned to

            Args:
                filepath (str or Path): Path of file to load
            Returns:
                list: UUIDs of Datasources data has been assigned to
        """
        from HUGS.Processing import assign_data, lookup_gas_datasources, get_attributes
        from HUGS.Util import hash_file
        from pathlib import Path
        import os

        tb = ThamesBarrier.load()

        # Check if the file has been uploaded already
        file_hash = hash_file(filepath=data_filepath)
        if file_hash in tb._file_hashes and not overwrite:
            raise ValueError(f"This file has been uploaded previously with the filename : {tb._file_hashes[file_hash]}")
        
        data_filepath = Path(data_filepath)
        filename = data_filepath.name

        gas_data = tb.read_data(data_filepath=data_filepath)
        
        for species in gas_data:
            units = tb._tb_params["unit_species"][species]
            scale = tb._tb_params["scale"][species]
            # Unit scales used for each species
            species_scales = tb._tb_params["scale"]
            gas_data[species]["data"] = get_attributes(ds=gas_data[species]["data"], species=species, site="TMB", units=units, scales=species_scales)

        # Check if we've got data from these sources before
        lookup_results = lookup_gas_datasources(lookup_dict=tb._datasource_names, gas_data=gas_data, 
                                                source_name=source_name, source_id=source_id)

        # Assign the data to the correct datasources
        datasource_uuids = assign_data(gas_data=gas_data, lookup_results=lookup_results, overwrite=overwrite)

        # Add the Datasources to the list of datasources associated with this object
        tb.add_datasources(datasource_uuids)

        # Store the hash as the key for easy searching, store the filename as well for
        # ease of checking by user
        tb._file_hashes[file_hash] = filename

        tb.save()

        return datasource_uuids

    def read_data(self, data_filepath):
        """ Separates the gases stored in the dataframe in 
            separate dataframes and returns a dictionary of gases
            with an assigned UUID as gas:UUID and a list of the processed
            dataframes

            Args:
                data_filepath (pathlib.Path): Path of datafile
            Returns:
                dict: Dictionary containing attributes, data and metadata keys
        """
        from pandas import read_csv
        from xarray import Dataset

        data = read_csv(data_filepath, parse_dates=[0], infer_datetime_format=True, index_col=0)
        # Drop NaNs from the data
        data = data.dropna(axis="rows", how="any")

        rename_dict = {}
        if "Methane" in data.columns:
            rename_dict["Methane"] = "CH4"

        data = data.rename(columns=rename_dict)
        data.index.name = "time"

        combined_data = {}
        
        for species in data.columns:
            processed_data = data.loc[:, [species]].to_xarray()

            # Convert methane to ppb
            if species == "CH4":
                processed_data[species] *= 1000
            
            # No averaging applied to raw obs, set variability to 0 to allow get_obs to calculate when averaging    
            processed_data["{} variability".format(species)] = processed_data[species] * 0.0

            site_attributes = self.site_attributes()

            # TODO - add in metadata reading
            combined_data[species] = {"metadata": {}, "data": processed_data, "attributes": site_attributes}

        return combined_data
            

    def site_attributes(self):
        """ Gets the site specific attributes for writing to Datsets

            Returns:
                dict: Dictionary of site attributes
        """
        if not self._tb_params:
            from json import load
            from HUGS.Util import get_datapath

            filepath = get_datapath(filename="attributes.json")

            with open(filepath, "r") as f:
                data = load(f)
                self._tb_params = data["TMB"]

        attributes = self._tb_params["global_attributes"]
        attributes["inlet_height_magl"] = self._tb_params["inlet"]
        attributes["instrument"] = self._tb_params["instrument"]

        return attributes
