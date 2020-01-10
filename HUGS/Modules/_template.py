
__all__ = ["TEMPLATE"]

### To use this template replace:
# - TEMPLATE with new data name in all upper case e.g. CRDS
# - template with new data name in all lower case e.g. crds
# - CHANGEME with a new fixed uuid (at the moment)

from HUGS.Modules import BaseModule

class TEMPLATE(BaseModule):
    """ Interface for processnig TEMPLATE data

        Instances of TEMPLATE should be created using the
        TEMPLATE.create() function
        
    """
    _template_root = "TEMPLATE"
    # Use uuid.uuid4() to create a unique fixed UUID for this object
    _template_uuid = "CHANGEME"

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
        self._template_params = {}
        # Sampling period of TEMPLATE data in seconds
        self._sampling_period = 60

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string

        d = {}
        # These should be able to stay the same
        d["creation_datetime"] = datetime_to_string(self._creation_datetime)
        d["stored"] = self._stored
        d["datasources"] = self._datasources
    
        return d

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

        key = "%s/uuid/%s" % (TEMPLATE._template_root, TEMPLATE._template_uuid)

        self._stored = True
        ObjectStore.set_object_from_json(bucket=bucket, key=key, data=self.to_data())

    @staticmethod
    def read_folder(folder_path, recursive=False):
        """ Read all data matching filter in folder

            Args:
                folder_path (str): Path of folder
        """
        from glob import glob
        from os import path

        # This finds data files in sub-folders
        folder_path = path.join(folder_path, "./*.dat")
        # This finds files in the current folder, get recursive
        # folder_path = _path.join(folder_path, "*.dat")
        filepaths = glob(folder_path, recursive=True)

        if not filepaths:
            raise FileNotFoundError("No data files found")

        for fp in filepaths:
            TEMPLATE.read_file(data_filepath=fp)

     @staticmethod
    def read_file(data_filepath, source_name, site=None, source_id=None, overwrite=False):
        """ Creates a TEMPLATE object holding data stored within Datasources

            Args:
                filepath (str or Path): Path of file to load
            Returns:
                list: UUIDs of Datasources data has been assigned to
        """
        from HUGS.Processing import assign_data, lookup_gas_datasources
        from HUGS.Util import hash_file
        from pathlib import Path
        import os

        template = TEMPLATE.load()

        # Check if the file has been uploaded already
        file_hash = hash_file(filepath=data_filepath)
        if file_hash in template._file_hashes and not overwrite:
            raise ValueError(f"This file has been uploaded previously with the filename : {template._file_hashes[file_hash]}")
        
        data_filepath = Path(data_filepath)
        filename = data_filepath.name

        if not source_name:
            source_name = filename.stem

        if not site:
            site = source_name.split(".")[0]

        # This should return xarray Datasets
        gas_data = template.read_data(data_filepath=data_filepath, site=site)

        # Assign attributes to the xarray Datasets here data here makes it a lot easier to test
        gas_data = template.assign_attributes(data=gas_data, site=site)

        # Check if we've got data from these sources before
        lookup_results = lookup_gas_datasources(lookup_dict=template._datasource_names, gas_data=gas_data, 
                                                source_name=source_name, source_id=source_id)

        # Assign the data to the correct datasources
        datasource_uuids = assign_data(gas_data=gas_data, lookup_results=lookup_results, overwrite=overwrite)

        # Add the Datasources to the list of datasources associated with this object
        template.add_datasources(datasource_uuids)

        # Store the hash as the key for easy searching, store the filename as well for
        # ease of checking by user
        template._file_hashes[file_hash] = filename

        template.save()

        return datasource_uuids

    def read_data(self, data_filepath, site):
        """ Separates the gases stored in the dataframe in 
            separate dataframes and returns a dictionary of gases
            with an assigned UUID as gas:UUID and a list of the processed
            dataframes

            Args:
                data_filepath (pathlib.Path): Path of datafile
            Returns:
                dict: Dictionary containing attributes, data and metadata keys
        """
        from pandas import RangeIndex
        from pandas import concat
        from pandas import read_csv
        from pandas import datetime
        from pandas import NaT

        from HUGS.Processing import get_attributes, read_metadata

        metadata = read_metadata(filepath=data_filepath, data=data, data_type="TEMPLATE")
        # This dictionary is used to store the gas data and its associated metadata
        combined_data = {}

        for n in range(n_gases):
            # Here we can convert the Dataframe to a Dataset and then write the attributes
            # Load in the JSON we need to process attributes
            gas_data = gas_data.to_xarray()

            site_attributes = self.site_attributes(site=site, inlet=inlet)

            # Create a copy of the metadata dict
            species_metadata = metadata.copy()
            species_metadata["species"] = species
            species_metadata["source_name"] = source_name

            combined_data[species] = {"metadata": species_metadata, "data": gas_data, "attributes": site_attributes}

        return combined_data

    