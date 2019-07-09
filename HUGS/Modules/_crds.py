# from _paths import RootPaths
__all__ = ["CRDS"]

# TODO - look into what's causing the logging messages in the first place
# This does stop them
import logging
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

class CRDS:
    """ Interface for processnig CRDS data

        Instances of CRDS should be created using the
        CRDS.create() function
        
    """
    _crds_root = "CRDS"
    _crds_uuid = "c2b2126a-29d9-crds-b66e-543bd5a188c2"

    def __init__(self):
        # self._uuid = None
        self._instruments = {}
        self._creation_datetime = None
        # self._labels = {}
        self._stored = False
        # Processed data
        self._proc_data = None
        # Datasource UUIDs
        self._datasources = []


    def is_null(self):
        """ Check if this is a null object

            Returns:
                bool: True if object is null
        """
        return self._datasources is None

    @staticmethod
    def create():
        """ This function should be used to create CRDS objects

            Returns:
                CRDS: CRDS object 
        """
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        c = CRDS()
        # c._uuid = "c2b2126a-29d9-crds-b66e-543bd5a188c2"
        c._creation_datetime = _get_datetime_now()

        return c


    @staticmethod
    def read_folder(folder_path):
        """ Read all data matching filter in folder

            Args:
                folder_path (str): Path of folder
        """
        from glob import glob as _glob
        from os import path as _path

        folder_path = _path.join(folder_path, "*/*.dat")
        filepaths = _glob(folder_path, recursive=True)

        # print(filepaths)

        for fp in filepaths:
            print("Processing %s" % fp.split("/")[-1])
            CRDS.read_file(data_filepath=fp)

        return False


    @staticmethod
    def read_file(data_filepath):
        """ Creates a CRDS object holding data stored within Datasources

            TODO - currently only works with a single Datasource

            Args:
                filepath (str): Path of file to load
            Returns:
                None
        """
        from pandas import read_csv as _read_csv
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string
        from HUGS.Processing import create_datasources as _create_datasources

        # There should only be 1 CRDS object
        c = CRDS.create()
        c.save()

        crds = CRDS.load()

        filename = data_filepath.split("/")[-1]

        gas_data = crds.read_data(data_filepath=data_filepath)

        # Create Datasources, save them to the object store and get their UUIDs
        datasource_uuids = _create_datasources(gas_data)

        # Add the Datasources to the list of datasources associated with this object
        crds.add_datasources(datasource_uuids)

        crds.save()

        return crds

    def read_data(self, data_filepath):
        """ Separates the gases stored in the dataframe in 
            separate dataframes and returns a dictionary of gases
            with an assigned UUID as gas:UUID and a list of the processed
            dataframes

            Args:
                data (Pandas.Dataframe): Dataframe containing all data
            Returns:
                tuple (str, str, list): Name of gas, ID of Datasource of this data and a list Pandas DataFrames for the 
                date-split gas data
        """
        from pandas import RangeIndex as _RangeIndex
        from pandas import concat as _concat
        from pandas import read_csv as _read_csv
        from pandas import datetime as _pd_datetime
        from pandas import NaT as _pd_NaT
        from uuid import uuid4 as _uuid4

        from HUGS.Processing import read_metadata

        # # Create a function to parse the datetime in the data file
        def parse_date(date):
            try:
                return _pd_datetime.strptime(date, '%y%m%d %H%M%S')
            except ValueError:
                return _pd_NaT

        data = _read_csv(data_filepath, header=None, skiprows=1, sep=r"\s+", index_col=["0_1"], 
                            parse_dates=[[0,1]], date_parser=parse_date)
        data.index.name = "Datetime"

        # Drop any rows with NaNs
        # Reset the index
        # This is now done before creating metadata
        data = data.dropna(axis="rows", how="any")

        # Get the number of gases in dataframe and number of columns of data present for each gas
        n_gases, n_cols = self.gas_info(data=data)

        # TODO - at the moment just create a new UUID for each gas
        datasource_ids = [_uuid4() for gas in range(n_gases)]

        header = data.head(2)
        skip_cols = sum([header[column][0] == "-" for column in header.columns])

        # time_cols = 2
        header_rows = 2
        # Dataframe containing the time data for this data input
        # time_data = data.iloc[2:, 0:time_cols]

        # timeframe = self.parse_timecols(time_data=time_data)
        # timeframe.index = _RangeIndex(timeframe.index.size)

        # Create metadata here
        metadata = read_metadata(filename=data_filepath, data=data, data_type="CRDS")

        data_list = []
        for n in range(n_gases):
            datasource_id = datasource_ids[n]
            # Slice the columns
            gas_data = data.iloc[:, skip_cols + n*n_cols: skip_cols + (n+1)*n_cols]

            # Reset the column numbers
            gas_data.columns = _RangeIndex(gas_data.columns.size)
            species = gas_data[0][0]

            column_names = ["count", "stdev", "n_meas"]
            column_labels = ["%s %s" % (species, l) for l in column_names]
            # Name columns
            gas_data = gas_data.set_axis(column_labels, axis='columns', inplace=False)
            # Drop the first two rows now we have the name
            gas_data = gas_data.drop(index=gas_data.head(header_rows).index, inplace=False)
            # Cast data to float64 / double
            gas_data = gas_data.astype("float64")
            
            # Create a copy of the metadata dict
            species_metadata = metadata.copy()
            species_metadata["species"] = species

            data_list.append((species, species_metadata, datasource_id, gas_data))

        return data_list

    def gas_info(self, data):
            """ Returns the number of columns of data for each gas
                that is present in the dataframe
            
                Args:
                    data (Pandas.DataFrame): Measurement data
                Returns:
                    tuple (int, int): Number of gases, number of
                    columns of data for each gas
            """
            from HUGS.Util import unanimous as _unanimous
            # Slice the dataframe
            head_row = data.head(1)

            gases = {}

            # Loop over the gases and find each unique value
            for column in head_row.columns:
                # print(column)
                s = head_row[column][0]
                if s != "-":
                    gases[s] = gases.get(s, 0) + 1

            # Check that we have the same number of columns for each gas
            if not _unanimous(gases):
                raise ValueError("Each gas does not have the same number of columns")

            return len(gases), list(gases.values())[0]

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        d = {}
        # d["UUID"] = self._uuid
        d["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        d["instruments"] =  self._instruments
        d["stored"] = self._stored
        d["datasources"] = self._datasources
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
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if data is None or len(data) == 0:
            return CRDS()

        if bucket is None:
            bucket = _get_bucket()
        
        c = CRDS()
        # c._uuid = data["UUID"]
        c._creation_datetime = _string_to_datetime(data["creation_datetime"])
        c._instruments = data["instruments"]
        #  c._instruments[instrument._uuid] = instrument._creation_datetime
        stored = data["stored"]

        c._datasources = data["datasources"]

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
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        crds_key = "%s/uuid/%s" % (CRDS._crds_root, CRDS._crds_uuid)

        self._stored = True
        _ObjectStore.set_object_from_json(bucket=bucket, key=crds_key, data=self.to_data())

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
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        key = "%s/uuid/%s" % (CRDS._crds_root, CRDS._crds_uuid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return CRDS.from_data(data=data, bucket=bucket)

    @staticmethod
    def exists(bucket=None):
        """ Uses an ID of some kind to query whether or not this is a new
            Instrument and should be created

            TODO - update this when I have a clearer idea of how to ID Instruments

            Args:
                instrument_id (str): ID of Instrument
                bucket (dict, default=None): Bucket for data storage
            Returns:
                bool: True if Instrument exists
        """
        from HUGS.ObjectStore import exists as _exists
        from HUGS.ObjectStore import get_bucket as _get_bucket
        
        if bucket is None:
            bucket = _get_bucket()

        key = "%s/uuid/%s" % (CRDS._crds_root, CRDS._crds_uuid)

        # Query object store for Instrument
        return _exists(bucket=bucket, key=key)

    def add_datasources(self, datasource_uuids):
        """ Add the passed list of Datasources to the current list

            Args:
                datasource_uuids (list): List of Datasource UUIDs
            Returns:
                None
        """
        self._datasources.extend(datasource_uuids)

    def uuid(self):
        """ Return the UUID of this object

            Returns:
                str: UUID of  object
        """
        return CRDS._crds_uuid

    def datasources(self):
        """ Return the list of Datasources for this object

            Returns:
                list: List of Datasources
        """
        return self._datasources
