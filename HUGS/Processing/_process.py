__all__ = ["proc"]


def proc(data_file, precision_filepath=None, data_type="CRDS"):
    """ Parses the passed filename using the passed data_type as a hint for
        processing

        Args:
            file_data (str): Name of file for processing
            data_type (str): Type of data to be processed (CRDS, GC etc)
        Returns:
            WIP
            list: List of Datasources
    """
    from HUGS.Processing import DataTypes as _DataTypes
    from HUGS.Util import load_object as _load_object

    # Load in tuple here for GC data and precision filenames?
    data_type = _DataTypes[data_type.upper()].name

    # Load in the the class used to process the data file/s
    processing_obj = _load_object(class_name=data_type)

    if data_type == "CRDS":
        obj = processing_obj.read_file(data_filepath=data_file)
    elif data_tpe == "GC":
        obj = proc_obj.read_file(data_filepath=data_file, precision_filepath=precision_filepath)

    # Return a summary of the data
    return obj.datasources()



