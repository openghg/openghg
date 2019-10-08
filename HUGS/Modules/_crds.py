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
        self._creation_datetime = None
        self._stored = False
        self._datasources = []

    def is_null(self):
        """ Check if this is a null object

            Returns:
                bool: True if object is null
        """
        return self._datasources is None

    @staticmethod
    def exists(bucket=None):
        """ Query the object store to check if a CRDS object already exists

            Args:
                bucket (dict, default=None): Bucket for data storage
            Returns:
                bool: True if CRDS object exists in object store
        """
        from HUGS.ObjectStore import exists as _exists
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        key = "%s/uuid/%s" % (CRDS._crds_root, CRDS._crds_uuid)

        return _exists(bucket=bucket, key=key)

    @staticmethod
    def create():
        """ This function should be used to create CRDS objects

            Returns:
                CRDS: CRDS object 
        """
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now
        c = CRDS()
        c._creation_datetime = _get_datetime_now()

        return c

    def to_data(self):
        """ Return a JSON-serialisable dictionary of object
            for storage in object store

            Returns:
                dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        d = {}
        d["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        d["stored"] = self._stored
        d["datasources"] = self._datasources

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
        c._creation_datetime = _string_to_datetime(data["creation_datetime"])
        stored = data["stored"]

        c._datasources = data["datasources"]
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

        # TODO - these will have to be manually added on first setup
        # then this can be removed
        if not CRDS.exists():
            return CRDS.create()

        if bucket is None:
            bucket = _get_bucket()

        key = "%s/uuid/%s" % (CRDS._crds_root, CRDS._crds_uuid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return CRDS.from_data(data=data, bucket=bucket)

    @staticmethod
    def read_folder(folder_path, recursive=False):
        """ Read all data matching filter in folder

            Args:
                folder_path (str): Path of folder
        """
        from glob import glob as _glob
        from os import path as _path

        # TODO - Remove this
        crds = CRDS.create()
        crds.save()

        # This finds data files in sub-folders
        folder_path = _path.join(folder_path, "./*.dat")
        # This finds files in the current folder, get recursive
        # folder_path = _path.join(folder_path, "*.dat")
        filepaths = _glob(folder_path, recursive=True)

        if len(filepaths) == 0:
            raise FileNotFoundError("No data files found")

        for fp in filepaths:
            CRDS.read_file(data_filepath=fp)

    @staticmethod
    def read_file(data_filepath, source_id):
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

        crds = CRDS.load()

        filename = data_filepath.split("/")[-1]

        gas_data = crds.read_data(data_filepath=data_filepath)

        # Create Datasources, save them to the object store and get their UUIDs
        datasource_uuids = _create_datasources(gas_data)

        # Add the Datasources to the list of datasources associated with this object
        crds.add_datasources(datasource_uuids)

        crds.save()

        return datasource_uuids

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

        # Function to parse the datetime format found in the datafile
        def parse_date(date):
            try:
                return _pd_datetime.strptime(date, '%y%m%d %H%M%S')
            except ValueError:
                return _pd_NaT

        data = _read_csv(data_filepath, header=None, skiprows=1, sep=r"\s+", index_col=["0_1"],
                            parse_dates=[[0,1]], date_parser=parse_date)
        data.index.name = "Datetime"

        # Drop any rows with NaNs
        # This is now done before creating metadata
        data = data.dropna(axis="rows", how="any")

        # Get the number of gases in dataframe and number of columns of data present for each gas
        n_gases, n_cols = self.gas_info(data=data)

        # TODO - at the moment just create a new UUID for each gas
        datasource_ids = [_uuid4() for gas in range(n_gases)]

        header = data.head(2)
        skip_cols = sum([header[column][0] == "-" for column in header.columns])

        header_rows = 2
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
                s = head_row[column][0]
                if s != "-":
                    gases[s] = gases.get(s, 0) + 1

            # Check that we have the same number of columns for each gas
            if not _unanimous(gases):
                raise ValueError("Each gas does not have the same number of columns. Please ensure data"
                                 "is of the CRDS type expected by this module")

            return len(gases), list(gases.values())[0]
        
    @staticmethod
    def data_check(data_filepath):
        """ Checks that the passed datafile can be read by this processing
            object

            Args:
                data_filepath (str): Data file path
            Returns:
                bool: True if data can be read

        """
        read_metadata()

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

    def remove_datasource(self, uuid):
        """ Remove the Datasource with the given uuid from the list 
            of Datasources

            Args:
                uuid (str): UUID of Datasource to be removed
        """
        self._datasources.remove(uuid)

    def clear_datasources(self):
        """ Remove all Datasources from the object

            Returns:
                None
        """
        self._datasources = []


    
