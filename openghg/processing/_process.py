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
    from openghg.modules import ObsSurface

    processing_obj = ObsSurface.load()

    if data_type == "GC":
        try:
            data, precision = data_file
        except (TypeError, ValueError) as error:
            raise TypeError("Ensure data and precision files are passed as a tuple\n", error)

    result = processing_obj.read_file(filepath=data_file, data_type=data_type, site=site, instrument=instrument_name)

    return result
