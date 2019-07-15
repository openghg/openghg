__all__ = ["proc"]


def proc(data_file, precision_filepath=None, data_type="CRDS"):
    """ Passes the passed filename(s) to the correct processing
        object depending on the data_type argument.

        Args:
            file_data (str): Name of file for processing
            precision_filepath (str, default=None): Name of precision file for GC data
            data_type (str): Type of data to be processed (CRDS, GC etc)
        Returns:
            WIP
            list: List of Datasources
    """
    from HUGS.Processing import DataTypes as _DataTypes
    from HUGS.Util import load_object as _load_object

    data_type = _DataTypes[data_type.upper()].name
    # Load in the the class used to process the data file/s
    processing_obj = _load_object(class_name=data_type)

    if data_type == "CRDS":
        obj = processing_obj.read_file(data_filepath=data_file)
    elif data_tpe == "GC":
        obj = proc_obj.read_file(data_filepath=data_file, precision_filepath=precision_filepath)

    # Return a summary of the data
    # TODO - modify me to return something useful
    return obj.datasources()



