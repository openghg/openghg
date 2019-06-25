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
        self._creation_datetime = None
        # self._labels = {}
        self._stored = False
        # Processed data
        self._proc_data = None


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
    def read_file(data_filepath):
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
        
        # from processing._metadata import Metadata as _Metadata
        from HUGS.Modules import Instrument as _Instrument

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

        if _Instrument.exists(uuid=instrument_id):
            instrument = _Instrument.load(uuid=instrument_id)
        else:
            instrument = _Instrument.create(name="name")

        filename = data_filepath.split("/")[-1]
        # metadata = _Metadata.create(filename, raw_data)

        # Parse the data here
        # Parse the gases
        # Save get gas_name datasource iD and data
        # Pass this gas_data list to the instrument for storage in Datasources
        gas_data = crds.parse_data(data_filepath=data_filepath)

        # Add the data to the instrument after processing
        instrument.add_data(gas_data)

        # Save updated Instrument to object store
        instrument.save()

        # Ensure this Instrument is saved within the object
        crds.add_instrument(instrument.get_uuid(), _datetime_to_string(instrument.get_creation_datetime()))
        crds.save()

        return crds


    def parse_data(self, data_filepath):
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

        from uuid import uuid4 as _uuid4

        # Create an ID for the Datasource
        # Currently just give it a fixed ID
        # datasource_ids = ["2e628682-094f-4ffb-949f-83e12e87a603", "2e628682-094f-4ffb-949f-83e12e87a604", 
        #                     "2e628682-094f-4ffb-949f-83e12e87a605"]
        data = _read_csv(data_filepath, header=None, skiprows=1, sep=r"\s+")
        # Drop any rows with NaNs
        # Reset the index
        # This is now done before creating metadata
        data = data.dropna(axis=0, how="any")
        data.index = _RangeIndex(data.index.size)

        # Get the number of gases in dataframe and number of columns of data present for each gas
        n_gases, n_cols = self.gas_info(data=data)

        # TODO - at the moment just create a new UUID for each gas
        datasource_ids = [_uuid4() for gas in range(n_gases)]

        header = data.head(2)
        skip_cols = sum([header[column][0] == "-" for column in header.columns])

        time_cols = 2
        header_rows = 2
        # Dataframe containing the time data for this data input
        time_data = data.iloc[2:, 0:time_cols]

        timeframe = self.parse_timecols(time_data=time_data)
        timeframe.index = _RangeIndex(timeframe.index.size)

        data_list = []
        for n in range(n_gases):
            datasource_id = datasource_ids[n]
            # Slice the columns
            gas_data = data.iloc[:, skip_cols + n*n_cols: skip_cols + (n+1)*n_cols]

            # Reset the column numbers
            gas_data.columns = _RangeIndex(gas_data.columns.size)
            gas_name = gas_data[0][0]

            column_names = ["count", "stdev", "n_meas"]
            column_labels = ["%s %s" % (gas_name, l) for l in column_names]

            # Split into years here
            # Name columns
            gas_data.set_axis(column_labels, axis='columns', inplace=True)

            # Drop the first two rows now we have the name
            gas_data.drop(index=gas_data.head(header_rows).index, inplace=True)
            gas_data.index = _RangeIndex(gas_data.index.size)

            # Cast data to float64 / double
            gas_data = gas_data.astype("float64")
            # Concatenate the timeframe and the data
            # Pandas concat here
            gas_data = _concat([timeframe, gas_data], axis="columns")

            # TODO - Verify integrity here? Test if this is required
            gas_data.set_index('Datetime', drop=True, inplace=True, verify_integrity=True)

            data_list.append((gas_name, datasource_id, gas_data))

        return data_list

    def parse_timecols(self, time_data):
        """ Takes a dataframe that contains the date and time 
            and creates a single columned dataframe containing a 
            UTC datetime

            Args:
                time_data (Pandas.Dataframe): Dataframe containing
            Returns:
                timeframe (Pandas.Dataframe): Dataframe containing datetimes set to UTC
        """
        import datetime as _datetime
        from pandas import DataFrame as _DataFrame
        from pandas import to_datetime as _to_datetime

        from Acquire.ObjectStore import datetime_to_datetime as _datetime_to_datetime
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        def t_calc(row, col): return _datetime_to_string(_datetime_to_datetime(
            _datetime.datetime.strptime(row+col, "%y%m%d%H%M%S")))

        time_gen = (t_calc(row, col)
                    for row, col in time_data.itertuples(index=False))
        time_list = list(time_gen)

        timeframe = _DataFrame(data=time_list, columns=["Datetime"])

        # Check how these data work when read back out
        timeframe["Datetime"] = _to_datetime(timeframe["Datetime"])

        return timeframe

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
                s = head_row[column][0]
                if s != "-":
                    gases[s] = gases.get(s, 0) + 1

            # Check that we have the same number of columns for each gas
            if not _unanimous(gases):
                raise ValueError(
                    "Each gas does not have the same number of columns")

            return len(gases), list(gases.values())[0]

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
        from HUGS.ObjectStore import get_bucket as _get_bucket

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
        from HUGS.ObjectStore import get_bucket as _get_bucket

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
        from HUGS.ObjectStore import get_bucket as _get_bucket

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
        from HUGS.ObjectStore import exists as _exists
        from HUGS.ObjectStore import get_bucket as _get_bucket
        
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
