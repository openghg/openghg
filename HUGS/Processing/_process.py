__all__ = ["process_data"]


def process_data(data_file, source_name, precision_filepath=None, data_type="CRDS", 
                    site=None, instrument_name=None, overwrite=False):
    """ Passes the passed filename(s) to the correct processing
        object depending on the data_type argument.

        Args:
            file_data (str): Name of file for processing
            precision_filepath (str, default=None): Name of precision file for GC data
            data_type (str, default="CRDS"): Type of data to be processed (CRDS, GC etc)
            overwrite (bool, default=False): Should existing and overlapping data be overwritten
        Returns:
            list: List of Datasources
    """
    from HUGS.Processing import DataTypes as _DataTypes
    from HUGS.Util import load_object as _load_object

    data_type = _DataTypes[data_type.upper()].name
    # Load in the the class used to process the data file/s
    processing_obj = _load_object(class_name=data_type)

    # TODO - improve this so the correct module is loaded - maybe don't rely on the above?

    if data_type == "CRDS":
        datasource_uuids = processing_obj.read_file(data_filepath=data_file, source_name=source_name, overwrite=overwrite)
    elif data_type == "GC":
        if site is None:
            raise ValueError("Site must be specified when reading GC data")

        datasource_uuids = processing_obj.read_file(data_filepath=data_file, precision_filepath=precision_filepath,
                                                    source_name=source_name, instrument_name=instrument_name, site=site, 
                                                    overwrite=overwrite)

    return datasource_uuids
