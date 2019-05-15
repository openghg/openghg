
__all__ = ["Metadata"]

class Metadata:
    """ Processes and holds metadata for raw data

        Metadata objects should be created using the
        Metadata.create() function
    """
    def __init__(self):
        self._data = None
        self._uuid = None
        self._creation_datetime = None

    @staticmethod
    def create(filename, data):
        """ Process the metadata and create a JSON serialisable 
            dictionary for saving to object store

            Args:
                filename (str): Filename to process
                data (Pandas.Dataframe): Raw data
            Returns:
                dict: Dictionary of metadata
        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        m = Metadata()

        # Dict for storage of metadata
        m._data = {}

        # Not a huge fan of these hardcoded values
        # TODO - will these change at some point?
        start_date = data[0][2]
        start_time = data[1][2]
        end_date = data.iloc[-1][0]
        end_time = data.iloc[-1][1]

        # Find gas measured and port used
        type_meas = data[2][2]
        port = data[3][2]

        start = m._parse_date_time(date=start_date, time=start_time)
        end = m._parse_date_time(date=end_date, time=end_time)

        # Extract data from the filename
        site, instrument, resolution, height = m._parse_filename(filename=filename)

        # Parse the dataframe to find the gases - this might be excessive
        # gases, _ = find_gases(data=data)
        m._uuid = _create_uuid()
        m._creation_datetime = _get_datetime_now()
        m._data["site"] = site
        m._data["instrument"] = instrument
        m._data["resolution"] = resolution
        m._data["height"] = height
        m._data["start_datetime"] = start
        m._data["end_datetime"] = end
        m._data["port"] = port
        m._data["type"] = type_meas
        # This will be added later
        # metadata["gases"] = gases

        return m

    def is_null(self):
        """Return whether this object is null
        
            Returns:
                bool: True if object is null
        """
        return self._uuid is None

    def data(self):
        """ Return the dictionary containing metadata

            Returns:
                dict: Metadata dictionary
        """
        return self._data

    # need these to save to the Object store?
    # def save():
    #     pass
    
    # def load():
    #     pass
        

    def parse_date_time(self, date, time):
        """ This function takes two strings and creates a datetime object 
            
            Args:
                date (str): The date in a YYMMDD format
                time (str): The time in the format hhmmss
                Example: 104930 for 10:49:30
            Returns:
                datetime: Datetime object

        """
        import datetime as _datetime

        if len(date) != 6:
            raise ValueError("Incorrect date format, should be YYMMDD")
        if len(time) != 6:
            raise ValueError("Incorrect time format, should be hhmmss")

        combined = date + time

        return _datetime.datetime.strptime(combined, "%y%m%d%H%M%S")


    def parse_filename(self, filename):
        """ Extracts the resolution from the passed string

            Args:
                resolution_str (str): Resolution extracted from the filename
            Returns:
                tuple (str, str, str, str): Site, instrument, resolution
                and height (m)

        """
        # Split the filename to get the site and resolution
        split_filename = filename.split(".")

        site = split_filename[0]
        instrument = split_filename[1]
        resolution_str = split_filename[2]
        height = split_filename[3]

        if(resolution_str == "1minute"):
            resolution = "1m"
        elif(resolution_str == "hourly"):
            resolution = "1h"

        return site, instrument, resolution, height
