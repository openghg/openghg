""" Module to load emissions maps and break them down into usable chunks
    for saving as Datasources

    
"""
__all__ = ["Footprint"]

class Footprint:
    _footprint_root = "footprint"
    _footprint_uuid = "8cba4797-510c-foot-print-e02a5ee57489"
    
    def __init__(self):
        self._creation_datetime = None
        self._stored = None
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
        return self._datasources is None

    @staticmethod
    def exists(bucket=None):
        """ Check if a Footprint object is already saved in the object 
            store

            Args:
                bucket (dict, default=None): Bucket for data storage
            Returns:
                bool: True if object exists
        """
        from HUGS.ObjectStore import exists as _exists
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        key = "%s/uuid/%s" % (Footprint._footprint_uuid, Footprint._footprint_uuid)
        return _exists(bucket=bucket, key=key)
    
    @staticmethod
    def create():
        """ Used to create Footprint objects

            Returns:
                Footprint: Footprint object
        """
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        footprint = Footprint()
        footprint._creation_datetime = _get_datetime_now()
        
        return footprint
    
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
        # data["uuid"] = self._uuid
        data["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        data["stored"] = self._stored
        data["datasources"] = self._datasources

    @staticmethod
    def from_data(data, bucket=None):
        """ Create a Footprint object from data

            Args:
                data (dict): JSON data
                bucket (dict, default=None): Bucket for data storage
        """ 
        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime

        if data is None or len(data) == 0:
            return Footprint()
        
        footprint = Footprint()
        # gc._uuid = data["uuid"]
        footprint._creation_datetime = _string_to_datetime(data["creation_datetime"])
        footprint._datasources = data["datasources"]

        footprint._stored = False

        return footprint

    def save(self, bucket=None):
        """ Save this Footprint object in the object store

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
        key = "%s/uuid/%s" % (Footprint._footprint_root, Footprint._footprint_uuid)
        _ObjectStore.set_object_from_json(bucket=bucket, key=key, data=self.to_data())

    @staticmethod
    def load(bucket=None):
        """ Load a Footprint object from the object store

            Args:
                bucket (dict, default=None): Bucket to store object
            Returns:
                Datasource: Datasource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if not Footprint.exists():
            return Footprint.create()

        if bucket is None:
            bucket = _get_bucket()
        
        key = "%s/uuid/%s" % (Footprint._footprint_root, Footprint._footprint_root)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)
        
        return Footprint.from_data(data=data, bucket=bucket)
        
    @staticmethod
    def read_file(filepath, metadata):
        """ For a single footprint file we can break it down into chunks of a certain size
            for easier lookup.

            Args:
                filepath (str): Path to NetCDF file containing footprint data
                metadata (dict): Metadata provided by the data provider
            Returns:
                None
        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        import xarray
        from HUGS.Modules import Datasource

        # Get the footprint object we need to load and save the passed files
        if not Footprint.exists():
            footprint = Footprint.create()
        else:
            footprint = Footprint.load()

        dataset = xarray.open_dataset(filepath)

        # We can save this metadata within the NetCDF file
        # Read metadata from the netCDF file
        file_metadata = _read_metadata(dataset)
        # Update the user passed metadata with that extracted from the NetCDF
        metadata.update(file_metadata)

        # Add in a temporary name key until this is connected to user based input
        metadata["name"] = "temp_footprint_name"
        
        # Lookup Datasource UUID using name of site? 
        # TODO - implement lookup

        # datasource_uuid = _lookup_uuid(datasource_name)
        # Create a Datasource for this file - can directly create a Datasource here as we'll
        # only have one per NetCDF?
        datasource = Datasource.create()

        if Datasource.exists(datasource_id=datasource_id):
            datasource = Datasource.load(uuid=datasource_id)
        else:
            datasource = Datasource.create(name=metadata["name"])

        datasource.add_footprint_data(metadata=metadata, data=dataset)
        datasource.save()

        datasource_uuids = {metadata["name"]: datasource.uuid()}

        footprint.add_datasources(datasource_uuids)

        footprint.save()

        return datasource.uuid()
            
        # Start and end datetime is stored in the Datasource
        # self._start_datetime = None
        # self._end_datetime = None
        # Read in the footprint
        # Split into ~ 5 MB chunks? Use get_split_freq to calculate this
        # Just do weeks for now?
        # Each datasource such as WAO-20magl will have a Datasource
        # netcdfs may be split into segments
        # Update the get_split_frequency function to handle datasets as well as dataframes?
        # Maybe separate functions due to the differences? Can always combine them afterwards

    def _read_metadata(dataset):
        """ Read in the metadata held in the passed xarray.Dataset

            Args:
                dataset (xarray.Dataset): Footprint Dataset
           
           Read the date range covered, data variables available, attributes, coordinates

        """
        data_variables = list(dataset.var())
        coordinates = list(dataset.coords)

        metadata = {}
        metadata["data_variables"] = list(dataset.var())
        metadata["coordinates"] = list(dataset.coords)
        metadata["data_type"] = "footprint"
        
        return metadata

    def get_split_frequency(footprint_ds, split_freq="W"):
        """
        """

        group = footprint_ds.groupby("time.week")

        # Get the Datasets for each week's worth of data
        data = [g for _, g in group if len(g) > 0]

    def datasources(self):
        """ Return the list of Datasources for this object

            Returns:
                list: List of Datasources
        """
        return self._datasource_uuids.keys()

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


    # def create_datasource(self, metadata, data):
    #     """ Create Datasources that will hold the footprint data

    #         Args:


    #     """

    #     # Create a datasource for each footprint source
    #     # for x in footprints:
    #         # create datasource
    #         # add_footprint data
    #     add_footprint_data





        
