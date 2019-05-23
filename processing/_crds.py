# from _paths import RootPaths

class CRDS:
    """ Holds CRDS data within a set of Datasources

        Instances of CRDS should be created using the
        CRDS.create() function
        
    """
    _crds_root = "CRDS"

    def __init__(self):
        self._metadata = None
        self._uuid = None
        self._datasources = None
        self._start_datetime = None
        self._end_datetime = None

    def is_null(self):
        """ Check if this is a null object

            Returns:
                bool: True if object is null
        """
        return self._uuid is None

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
    def read_file(filepath):
        """ Creates a CRDS object holding data stored within Datasources

        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now
        from processing._metadata import Metadata as _Metadata
        from processing._segment import get_datasources as _get_datasources

        import pandas as _pd

        data = _pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")        
        
        filename = filepath.split("/")[-1]
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

    def to_data(self):
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        datasource_uuids = {d._name: d._uuid for d in self._datasources}

        d = {}
        d["UUID"] = self._uuid
        d["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        d["datasources"] = datasource_uuids
        d["metadata"] = self._metadata.data()
        d["data_start_datetime"] = _datetime_to_string(self._start_datetime)
        d["data_end_datetime"] = _datetime_to_string(self._end_datetime)

        return d

    @staticmethod
    def from_data(data, bucket=None):
        """ Create a CRDS object from data

            Args:
                data (str): JSON data
            Returns:
                CRDS: CRDS object created from data
        """
        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime
        from modules._datasource import Datasource
        from objectstore.hugs_objstore import get_bucket as _get_bucket

        if data is None or len(data) == 0:
            return CRDS()

        if bucket is None:
            bucket = _get_bucket()
        
        c = CRDS()
        c._uuid = data["UUID"]
        c._creation_datetime = _string_to_datetime(data["creation_datetime"])

        datasource_uuids = data["datasources"]
        c._datasources = []

        for _, uuid in datasource_uuids.items():
            c._datasources.append(Datasource.load(bucket=bucket, uuid=uuid))

        c._metadata = data["metadata"]
        c._start_datetime = _string_to_datetime(data["data_start_datetime"])
        c._end_datetime = _string_to_datetime(data["data_end_datetime"])

        return c

    def save(self, bucket=None):
        """ Save the object to the object store

            Args:
                bucket (dict, default=None): Bucket for data
            Returns:
                None

            Save the object at a CRDS key
            Then save the datasources stored within the object
            as HDF5 files. 
            How to save the objects containing dataframes as HDF objects

        """
        if self.is_null():
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        from objectstore.hugs_objstore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        crds_key = "%s/uuid/%s" % (CRDS._crds_root, self._uuid)
        # Get the datasources to save themselves to the object store
        for d in self._datasources:
            d.save(bucket)

        _ObjectStore.set_object_from_json(bucket=bucket, key=crds_key, data=self.to_data())

    @staticmethod
    def load(uuid, bucket=None):
        """ Load a CRDS object from the datastore using the passed
            bucket and UUID

            Args:
                bucket (dict, default=None): Bucket to store object
                uuid (str, default=None): UID of Datasource
            Returns:
                Datasource: Datasource object created from JSON
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from modules._datasource import Datasource
        from objectstore.hugs_objstore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        key = "%s/uuid/%s" % (CRDS._crds_root, uuid)

        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return CRDS.from_data(data=data, bucket=bucket)


    def key_to_daterange(self, key):
        """ Takes a dated key and returns two datetimes for the start and 
            end datetimes for the data

            Args:
                key (str): Key for data in the object store
            Returns:
                tuple (datetime, datetime): Datetimes for start and end of data

        """
        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime

        end_key = key.split("/")[-1]
        dates = end_key.split("_")

        if len(dates) > 2:
            raise ValueError("Invalid date string")

        start = _string_to_datetime(dates[0])
        end = _string_to_datetime(dates[1])

        return start, end

    def search_store(self, bucket, root_path, datetime_begin, datetime_end):
        """ Get all values stored in the object store

            Args:  
                bucket (dict): Bucket holding data
                root_path (str): Select from the enum RootPaths
                For DataSources: datasource
                For Instruments: instrument etc
                datetime_begin (datetime): Start of datetime range
                datetime_end (datetime): End of datetime range
            Returns:
                list: A list of keys for the found data

        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import datetime_to_datetime as _datetime_to_datetime
        from objectstore.hugs_objstore import get_dataframe as _get_dataframe
        from pandas import date_range as _pd_daterange

        datetime_begin = _datetime_to_datetime(datetime_begin)
        datetime_end = _datetime_to_datetime(datetime_end)

        # Something like this?
        # freq = "YS"
        resolution = "%Y"
        # if start_datetime.month != 0 and end_datetime.month != 0:
        #     resolution += "%m"
        #     freq = "MS"
        # if start_datetime.day != 0 and end_datetime.day != 0:
        #     resolution += "%d"
        #     freq = "D"
        # if start_datetime.hour != 0 and end_datetime.hour != 0:
        #     resolution += "%h"
        #     freq = "H"

        # At the moment just have years
        daterange = _pd_daterange(start=datetime_begin, end=datetime_end)

        # path = RootPaths[root_path.upper()]
        
        # TODO - Change this to work with enums?
        path = "data"

        # Get the UUIDs for the data
        data_uuids = [d._uuid for d in self._datasources]

        # TODO - Tidy me
        uuids = []
        for uuid in data_uuids:
            for date in daterange:
                date_string = date.strftime(resolution)
                # Prefix with the year
                prefix = "%s/uuid/%s/%s" % (path, uuid, date_string)

                datakeys = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

                for key in datakeys:
                    _, end = self.key_to_daterange(key)

                    if end.year <= date.year:
                        uuids.append(uuid)

        return uuids

        # datasources = []
        # from objectstore.hugs_objstore import get_dated_object_json as _get_dated_object_json
        # # Get the data
        # for key in keys:
        #     # Get Datasource objects from the object store
        #     # These then in turn can get the dataframes
        #     datasources.append(_get_dated_object_json(key))


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
