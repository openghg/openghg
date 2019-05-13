class CRDS:
    def __init__(self):
        self._metadata = None
        self._datasources = None
    
    @staticmethod
    # def create(metadata, datasources, start_datetime, end_datetime): <- Which one of these?
    def create(datasources, start_datetime, end_datetime):
        c = CRDS()
        # Get the metadata - is this from the datasources?
        # Create a CRDS object from some datasources and the
        # start and end datetimes contained within the data

    @staticmethod
    def read_file(filename):
        # Load the data in and create a dataframe
        data = 
        # Parse filename data
        
        # Get datasources
        # as Pandas dataframes
        metadata = _get_metadata(filename)
        # Data will be contained within the Datasources
        datasources = _get_datasources(filename)

        c = CRDS()
        c._datasources = datasources
        # Metadata dict
        c._metadata = metadata

        # Insert metadata parse stuff here
        # Columns of data -  get on with it!
        data = _read_file(filename)

        if(len(data) != len(datasources):
            raise IOError("Something is terribly wrong...")
        
        # Save the data for each datasource into the Datasource object
        for i in range(0, len(data)):
            # This add_data function adds it to the object store
            datasources[i].add_data(data[i])

        # Ensure the CRDS object knows the datetimes it has
        c._start_datetime = data[0].earliest_datetime()
        c._end_datetime = data[0].latest_datetime()

        return c


    def write_file(self, filename):
        data = [] 
        datetimes = {}
        for datasource in self._datasources:
            # Get datas
            d = datasource.get_data(start=self._start_datetime, end=self._end_datetime)
            data.append(d)

            for datetime in d.datetimes_in_data():
                datetimes[datetime] = 1
        
        datetimes = list(datetimes.keys())

        datetimes.sort()

        with open(filename, "w") as FILE:
            FILE.write(metadata)
            # Merge the dataframes
            # If no data for that datetime set as NaN
            # Write these combined tables to the file


        
