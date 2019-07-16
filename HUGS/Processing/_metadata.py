
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
        # Find gas measured and port used
        type_meas = data[2][2]
        port = data[3][2]

         # Split the filename to get the site and resolution
        split_filename = filename.split(".")

        if len(split_filename) < 4:
            raise ValueError("Error reading metadata from filename. The expected format is {site}.{instrument}.{time resolution}.{height}.dat")

        site = split_filename[0]
        instrument = split_filename[1]
        resolution_str = split_filename[2]
        height = split_filename[3]

        if(resolution_str == "1minute"):
            resolution = "1_minute"
        elif(resolution_str == "hourly"):
            resolution = "1_hour"
        else:
            resolution = "Not read"
 
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
    # If we haven't been able to split the filename raise an error
    split_hyphen = split_filename[0].split("-")
    if len(split_hyphen) < 2:
        raise ValueError("Error reading metadata from filename. The expected format is {site}-{instrument}.{number}.C")

    site = split_hyphen[0]
    instrument = split_hyphen[1]

    metadata = {}
    metadata["site"] = site
    metadata["instrument"] = instrument

    return metadata