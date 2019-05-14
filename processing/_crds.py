class CRDS:
    """ Holds CRDS data within a set of Datasources

        Instances of CRDS should be created using the
        CRDS.create() function
        
    """
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
        c._end_datetime = data[0].get_end_datetime()

        return c

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
        
        # 
        # 
        # datetimes = {}

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
