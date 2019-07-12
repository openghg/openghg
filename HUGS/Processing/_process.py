__all__ = ["process"]


def process(file_data, data_type):
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

    obj = processing_obj.read_file(data_filepath=filepath)
    # if data_type == "CRDS":
    # elif data_tpe == "GC":
    #     obj = proc_obj.read_file(data_filepath=filepath, precision_filepath=precision_filepath)

    return obj.datasources()



