__all__ = ["process_data"]


def process_data(
    data_file,
    source_name,
    data_type="CRDS",
    site=None,
    instrument_name=None,
    overwrite=False,
):
    """ Passes the passed filename(s) to the correct processing
        object depending on the data_type argument.

        Args:
            data_file (str, tuple (str, str)): Paths of file(s) for processing
            source_name (str): Name of source
            data_type (str, default="CRDS"): Type of data to be processed (CRDS, GC etc)
            overwrite (bool, default=False): Should existing and overlapping data be overwritten
        Returns:
            list: List of Datasources
    """
    from HUGS.Processing import DataTypes
    from HUGS.Util import load_object

    data_type = DataTypes[data_type.upper()].name
    # Load in the the class used to process the data file/s
    processing_obj = load_object(class_name=data_type)

    if data_type == "GC":
        if site is None:
            raise ValueError("Site must be specified when reading GC data")

        try:
            data, precision = data_file
        except (TypeError, ValueError) as error:
            raise TypeError("Ensure data and precision files are passed as a tuple\n", error)

        datasource_uuids = processing_obj.read_file(
            data_filepath=data,
            precision_filepath=precision,
            source_name=source_name,
            instrument_name=instrument_name,
            site=site,
            overwrite=overwrite,
        )
    else:
        datasource_uuids = processing_obj.read_file(
            data_filepath=data_file,
            site=site,
            source_name=source_name,
            overwrite=overwrite,
        )

    return datasource_uuids
