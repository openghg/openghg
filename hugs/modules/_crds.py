# from _paths import RootPaths

class CRDS:
    """ Interface for uploading CRDS data

        Instances of CRDS should be created using the
        CRDS.create() function
        
    """
    _crds_root = "CRDS"

    def __init__(self):
        self._uuid = None
        self._instruments = {}
        self._creation_datetime
        # self._labels = {}
        self._stored = False


    def is_null(self):
        """ Check if this is a null object

            Returns:
                bool: True if object is null
        """
        return self._uuid is None

    @staticmethod
    def create():
        """ This function should be used to create CRDS objects

            Returns:
                CRDS: CRDS object 
        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        c = CRDS()
        c._uuid  = _create_uuid()
        c._creation_datetime = _get_datetime_now()

        return c

    @staticmethod
    def read_filelist(filelist):
        """ Returns a number of CRDS objects created from files

            Args:
                filelist (list): List of filenames
            Returns:
                list: List of CRDS objects
        """
        crds_list = []
        for filename in filelist:
            crds_list.append(CRDS.read_file(filename))

        return crds_list

    @staticmethod
    def read_file(filepath):
        """ Creates a CRDS object holding data stored within Datasources

            TODO - currently only works with a single Datasource

            Args:
                filepath (str): Path of file to load
            Returns:
                None
        """
        from pandas import read_csv as _read_csv
        
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string
        
        from processing._metadata import Metadata as _Metadata
        from modules import Instrument as _Instrument

        raw_data = _read_csv(filepath, header=None, skiprows=1, sep=r"\s+")     

        # First check for the CRDS object - should only be one? 
        # Maybe this can depend on the type or something?

        # Load CRDS object from object store
        # CRDS object doesn't actually hold any of the Instrument objects
        # it just remembers them
        
        # Get a random UUID for now
        crds_uuid = _create_uuid()

        if CRDS.exists(uuid=crds_uuid):
            crds = CRDS.load(uuid=crds_uuid)
        else:
            crds = CRDS.create()
        
        # TODO - ID instrument from data/user?
        instrument_name = "instrument_name"
        instrument_id = _create_uuid()

        if _Instrument.exists(instrument_id=instrument_id):
            instrument = _Instrument.load(uuid=instrument_id)
        else:
            instrument = _Instrument.create(name="name")

        filename = filepath.split("/")[-1]
        metadata = _Metadata.create(filename, raw_data)

        instrument.parse_data(raw_data=raw_data, metadata=metadata)
        # Save updated Instrument to object store
        instrument.save()

        # Ensure this Instrument is saved within the object
        crds.add_instrument(instrument._uuid, _datetime_to_string(instrument._creation_datetime))
        crds.save()

        return crds


    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """

        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        d = {}
        d["UUID"] = self._uuid
        d["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        d["instruments"] =  self._instruments
        d["stored"] = self._stored
        # Save UUIDs of associated instruments
        # d["datasources"] = datasource_uuids
        # d["data_start_datetime"] = _datetime_to_string(self._start_datetime)
        # d["data_end_datetime"] = _datetime_to_string(self._end_datetime)
        # This is only set as True when saving this object in the object store

        return d

    @staticmethod
    def from_data(data, bucket=None):
        """ Create a CRDS object from data

            Args:
                data (str): JSON data
                bucket (dict, default=None): Bucket for data storage
            Returns:
                CRDS: CRDS object created from data
        """
        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime
        from objectstore._hugs_objstore import get_bucket as _get_bucket

        if data is None or len(data) == 0:
            return CRDS()

        if bucket is None:
            bucket = _get_bucket()
        
        c = CRDS()
        c._uuid = data["UUID"]
        c._creation_datetime = _string_to_datetime(data["creation_datetime"])
        c._instruments = data["instruments"]
        #  c._instruments[instrument._uuid] = instrument._creation_datetime
        stored = data["stored"]

        # Could load instruments? This could be a lot of instruments
        # c._start_datetime = _string_to_datetime(data["data_start_datetime"])
        # c._end_datetime = _string_to_datetime(data["data_end_datetime"])
        # Now we're loading it in again 
        c._stored = False

        return c

    def save(self, bucket=None):
        """ Save the object to the object store

            Args:
                bucket (dict, default=None): Bucket for data
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

        crds_key = "%s/uuid/%s" % (CRDS._crds_root, self._uuid)

        # Ensure that the Instruments related to this object are stored

        self._stored = True
        _ObjectStore.set_object_from_json(bucket=bucket, key=crds_key, data=self.to_data())

    @staticmethod
    def load(uuid, key=None, bucket=None):
        """ Load a CRDS object from the datastore using the passed
            bucket and UUID

            Args:
                uuid (str): UUID of CRDS object
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
            key = "%s/uuid/%s" % (CRDS._crds_root, uuid)

        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return CRDS.from_data(data=data, bucket=bucket)

    @staticmethod
    def exists(uuid, bucket=None):
        """ Uses an ID of some kind to query whether or not this is a new
            Instrument and should be created

            TODO - update this when I have a clearer idea of how to ID Instruments

            Args:
                instrument_id (str): ID of Instrument
                bucket (dict, default=None): Bucket for data storage
            Returns:
                bool: True if Instrument exists 
        """
        from objectstore import exists as _exists
        from objectstore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        # Query object store for Instrument
        return _exists(bucket=bucket, uuid=uuid)


    def add_instrument(self, instrument_id, value):
        """ Add an Instument to this object's dictionary of instruments

            Args:
                instrument_id (str): Instrment UUID
                value (str): Value to describe Instrument
            Returns:
                None
        """
        self._instruments[instrument_id] = value


    def get_instruments(self):
        """ Get the Instruments associated with this object

            Returns:
                dict: Dictionary of Instrument UUIDs
        """
        return self._instruments



    # def get_daterange(self):
    #     """ Returns the daterange of the data in this object

    #         Returns:
    #             tuple (datetime, datetime): Start and end datetime
    #     """
    #     return self._start_datetime, self._end_datetime 


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

        return False
        # for datasource in self._datasources:
        #     # Get datas - for now just get the data that's there
        #     # Can either get the daterange here or in the Datasource.get_data fn
        #     data.append(datasource.get_data())

        #     for datetime in d.datetimes_in_data():
        #         datetimes[datetime] = 1
        
        # datetimes = list(datetimes.keys())

        # datetimes.sort()

        # with open(filename, "w") as FILE:
        #     FILE.write(metadata)
        #     # Merge the dataframes
        #     # If no data for that datetime set as NaN
        #     # Write these combined tables to the file
