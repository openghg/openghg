
__all__ = ["read_metadata"]

def read_metadata(filename, data, data_type):
        """ Process the metadata and create a JSON serialisable 
            dictionary for saving to object store

            Args:
                filename (str): Filename to process
                data (Pandas.DataFrame): Raw data
                data_type (str): Data typw (CRDS, GC etc) to parse
            Returns:
                dict: Dictionary of metadata
        """
        from HUGS.Processing import DataTypes as _DataTypes

        # In case we're passed a filepath, just take the filename
        filename = filename.split("/")[-1]

        data_type = _DataTypes[data_type.upper()].name

        if data_type == "CRDS":
            metadata = _parse_CRDS(filename=filename, data=data)
        elif data_type == "GC":
            metadata = _parse_GC(filename=filename, data=data)

        return metadata

def _parse_CRDS(filename, data):
        """ Parse CRDS files and create a metadata dict

            Args:
                filename (str): Name of data file
                data (Pandas.DataFrame): Raw data
            Returns:
                dict: Dictionary containing metadata
        """
        # Not a huge fan of these hardcoded values
        # TODO - will these change at some point?
        # start_date = str(data[0][2])
        # start_time = str(data[1][2])
        # end_date = str(data.iloc[-1][0])
        # end_time = str(data.iloc[-1][1])

        # Find gas measured and port used
        type_meas = data[2][2]
        port = data[3][2]

        # start = self.parse_date_time(date=start_date, time=start_time)
        # end = self.parse_date_time(date=end_date, time=end_time)

         # Split the filename to get the site and resolution
        split_filename = filename.split(".")

        site = split_filename[0]
        instrument = split_filename[1]
        resolution_str = split_filename[2]
        height = split_filename[3]

        if(resolution_str == "1minute"):
            resolution = "1_minute"
        elif(resolution_str == "hourly"):
            resolution = "1_hour"
 
        # Parse the dataframe to find the gases - this might be excessive
        # gases, _ = find_gases(data=data)
        metadata = {}
        metadata["site"] = site
        metadata["instrument"] = instrument
        metadata["time_resolution"] = resolution
        metadata["height"] = height
        metadata["port"] = port
        metadata["type"] = type_meas
        
        return metadata


def _parse_GC(filename, data):
    """ Parse GC files and create a metadata dict

        Args:
            filename (str): Name of data file
            data (Pandas.DataFrame): Raw data
        Returns:
            dict: Dictionary containing metadata
    """
    split_filename = filename.split(".")
    site = split_filename[0].split("-")[0]
    instrument = split_filename[0].split("-")[1]

    metadata = {}
    metadata["site"] = site
    metadata["instrument"] = instrument

    return metadata





