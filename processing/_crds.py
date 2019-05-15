class CRDS:
    """ Holds CRDS data within a set of Datasources

        Instances of CRDS should be created using the
        CRDS.create() function
        
    """
    crds_root = "CRDS"

    def __init__(self):
        self._metadata = None
        self._uuid = None
        self._datasources = None
        self._start_datetime = None
        self._end_datetime = None

    @staticmethod
    def create(metadata, datasources, start_datetime, end_datetime):
        """ This function should be used to create CRDS objects

        """
        c = CRDS()

        c._metadata = metadata
        c._datasources = datasources
        c._start_datetime = start_datetime
        c._end_datetime = end_datetime

    @staticmethod
    def read_file(filename):
        """ Creates a CRDS object holding data stored within Datasources

        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now
        from _metadata import Metadata as _Metadata
        from _segment import get_datasources as _get_datasources

        data = pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")        
        
        # Get a Metadata object containing the processed metadata
        # Does this need to be an object? Just a dict?
        metadata = _Metadata.create(filename, data)
        # Data will be contained within the Datasources
        datasources = _get_datasources(data)

        c = CRDS()
        c._uuid = _create_uuid()
        c._creation_datetime = _get_datetime_now()
        c._datasources = datasources
        # Metadata dict
        c._metadata = metadata

        # Ensure the CRDS object knows the datetimes it has
        c._start_datetime = datasources[0].get_start_datetime()
        c._end_datetime = datasources[0].get_end_datetime()

        return c

    def save(self, bucket=None):
        """ Save the object to the object store

            Save the object at a CRDS key
            Then save the datasources stored within the object
            as HDF5 files. 
            How to save the objects containing dataframes as HDF objects

        """

        # Save the object in parts in the datastore and recreate
        # from pieces? Otherwise how to store dataframes
        if self.is_null():
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        from objectstore.hugs_objstore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        crds_key = "%s/uuid/%s" % (CRDS.crds_root, self._uuid)
        _ObjectStore.set_object

        Datasources contain the data

        Datasources can save the dataframes separately at /data
        Datasources themselves at /datasources/

        # Get the UUIDS for the individual datasources in the object store











    def write_file(self, filename):
        """ Collects the data stored in this object and writes it
            to file at filename

            TODO - add control of daterange being written to file from
            data in Datasources

            Args:
                filename (str): Filename to write data to
            Returns:
                None
        """
        data = [] 

        for datasource in self._datasources:
            # Get datas - for now just get the data that's there
            # Can either get the daterange here or in the Datasource.get_data fn
            data.append(datasource.get_data())

            for datetime in d.datetimes_in_data():
                datetimes[datetime] = 1
        
        datetimes = list(datetimes.keys())

        datetimes.sort()

        with open(filename, "w") as FILE:
            FILE.write(metadata)
            # Merge the dataframes
            # If no data for that datetime set as NaN
            # Write these combined tables to the file

    @staticmethod
    def load(name=None, uuid=None, bucket=None):
        pass

    def save(self):
        pass
