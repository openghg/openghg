__all__ = ["Footprint"]


class Footprint:
    """ Module to load emissions maps and break them down into usable chunks
        for saving as Datasources
    """

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
        return len(self._datasource_names) == 0

    @staticmethod
    def exists(bucket=None):
        """ Check if a Footprint object is already saved in the object
            store

            Args:
                bucket (dict, default=None): Bucket for data storage
            Returns:
                bool: True if object exists
        """
        from HUGS.ObjectStore import exists, get_bucket

        if bucket is None:
            bucket = get_bucket()

        key = "%s/uuid/%s" % (Footprint._footprint_root, Footprint._footprint_uuid)

        return exists(bucket=bucket, key=key)

    @staticmethod
    def create():
        """ Used to create Footprint objects

            Returns:
                Footprint: Footprint object
        """
        from Acquire.ObjectStore import get_datetime_now

        footprint = Footprint()
        footprint._creation_datetime = get_datetime_now()

        return footprint

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        if self.is_null():
            return {}

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
        """ Create a Footprint object from data

            Args:
                data (dict): JSON data
                bucket (dict, default=None): Bucket for data storage
        """
        from Acquire.ObjectStore import string_to_datetime

        if data is None or len(data) == 0:
            return Footprint()

        footprint = Footprint()
        footprint._creation_datetime = string_to_datetime(data["creation_datetime"])
        footprint._datasource_uuids = data["datasource_uuids"]
        footprint._datasource_names = data["datasource_names"]
        footprint._file_hashes = data["file_hashes"]
        footprint._stored = False

        return footprint

    def save(self, bucket=None):
        """ Save this Footprint object in the object store

            Args:
                bucket (dict): Bucket for data storage
            Returns:
                None
        """
        from HUGS.ObjectStore import get_bucket, set_object_from_json

        if self.is_null():
            return

        if bucket is None:
            bucket = get_bucket()

        self._stored = True
        key = f"{Footprint._footprint_root}/uuid/{Footprint._footprint_uuid}"
        set_object_from_json(bucket=bucket, key=key, data=self.to_data())

    @staticmethod
    def load(bucket=None):
        """ Load a Footprint object from the object store

            Args:
                bucket (dict, default=None): Bucket to store object
            Returns:
                Datasource: Datasource object created from JSON
        """
        from HUGS.ObjectStore import get_bucket, get_object_from_json

        if not Footprint.exists():
            return Footprint.create()

        if bucket is None:
            bucket = get_bucket()

        key = f"{Footprint._footprint_root}/uuid/{Footprint._footprint_uuid}"
        data = get_object_from_json(bucket=bucket, key=key)

        return Footprint.from_data(data=data, bucket=bucket)

    @staticmethod
    def read_file(filepath, source_name):
        """ For a single footprint file we can break it down into chunks of a certain size
            for easier lookup.

            Args:
                filepath (str): Path to NetCDF file containing footprint data
                metadata (dict): Metadata provided by the data provider
            Returns:
                None
        """
        import xarray as xr
        from HUGS.Processing import lookup_footprint_datasources

        footprint = Footprint.load()

        dataset = xr.open_dataset(filepath)

        # We can save this metadata within the NetCDF file?
        # Read metadata from the netCDF file
        metadata = {}
        metadata["name"] = source_name
        # Update the user passed metadata with that extracted from the NetCDF
        file_metadata = footprint._read_metadata(dataset)
        metadata.update(file_metadata)

        datasource_names = footprint.datasource_names()
        lookup_results = lookup_footprint_datasources(
            lookup_dict=datasource_names, source_name=source_name
        )

        datasource_uuids = footprint.assign_data(
            lookup_results=lookup_results,
            source_name=source_name,
            data=dataset,
            metadata=metadata,
        )

        footprint.add_datasources(datasource_uuids)

        footprint.save()

    def assign_data(self, lookup_results, source_name, data, metadata, overwrite=False):
        """ Assign data to a new or existing Datasource

            Args:
                lookup_results (dict): Results of Datasource lookup
                source_name (str): Name of data source
                data (xarray.Dataset): Data
                metadata (dict): Dictionary of metadata
                overwrite (bool, default=False): Should exisiting data be overwritten
            Returns:
                list: List of Datasource UUIDs
        """
        from HUGS.Modules import Datasource

        uuids = {}
        for key in lookup_results:
            uuid = lookup_results[key]["uuid"]
            name = metadata["name"]

            if uuid:
                datasource = Datasource.load(uuid=uuid)
            else:
                datasource = Datasource(name=name)

            datasource.add_data(metadata=metadata, data=data, data_type="footprint")
            datasource.save()

            uuids[name] = datasource.uuid()

        return uuids

    def _read_metadata(self, dataset):
        """ Read in the metadata held in the passed xarray.Dataset

            Args:
                dataset (xarray.Dataset): Footprint Dataset

            Read the date range covered, data variables available, attributes, coordinates

        """
        metadata = {}
        metadata["data_variables"] = list(dataset.var())
        metadata["coordinates"] = list(dataset.coords)
        metadata["data_type"] = "footprint"

        return metadata

    def get_split_frequency(footprint_ds, split_freq="W"):
        """ Currently unused """

        # group = footprint_ds.groupby("time.week")

        # Get the Datasets for each week's worth of data
        # data = [g for _, g in group if len(g) > 0]
        raise NotImplementedError

    def datasources(self):
        """ Return the list of Datasources for this object

            Returns:
                list: List of Datasources
        """
        return list(self._datasource_uuids.keys())

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

    def datasource_names(self):
        """ Returns the dictionary containing the names and UUIDs of
            Datasources associated with this object

            Returns:
                dict: Dictionary of name: UUID of associated Datasources
        """
        return self._datasource_names

    def uuid(self):
        """ Returns the UUID of this object

        """
        return Footprint._footprint_uuid

    # def create_datasource(self, metadata, data):
    #     """ Create Datasources that will hold the footprint data

    #         Args:

    #     """

    #     # Create a datasource for each footprint source
    #     # for x in footprints:
    #         # create datasource
    #         # add_footprint data
    #     add_footprint_data
