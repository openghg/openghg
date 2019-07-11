__all__ = ["process"]


def process(file_data, data_type):
    """ Parses the passed filename using the passed data_type as a hint for
        processing

        Args:
            file_data (str): Name of file for processing
            data_type (str): Type of data to be processed (CRDS, GC etc)
        Returns:
            None
    """
    from HUGS.Processing import DataTypes as _DataTypes
    from HUGS.Util import load_object as _load_object

    # Load in tuple here for GC data and precision filenames?
    data_type = _DataTypes[data_type.upper()].name

    # Load in the the class used to process the data file/s
    proc_obj = _load_object(class_name=data_type)

    if data_type == "CRDS":
        obj = proc_obj.read_file(data_filepath=filepath)
    
    
    # else GC





    return False
