from Acquire.Client import Authorisation, PAR
from Acquire.Service import get_this_service

from HUGS.Processing import process_data

def process(args):
    """ Take a PAR from an uploaded file and process the data

        Args:
            args (dict): Dictionary of JSON serialised objects to be
            used by processing functions
        Returns:
            dict: Dictionary of results of processing
    """
    data_type = args["data_type"]

    data_par = PAR.from_data(args["par"]["data"])
    data_secret = args["par_secret"]["data"]

    auth = args["authorisation"]
    authorisation = Authorisation.from_data(auth)

    # Verify that this process had authorisation to be called
    authorisation.verify("process")

    hugs = get_this_service(need_private_access=True)

    data_secret = hugs.decrypt_data(data_secret)
    data_filename = data_par.resolve(secret=data_secret)
    # Here we're downloading the data to the tmp directory
    # Be good if we could load it directly from the object store
    data_file = data_filename.download(dir="/tmp")

    if data_type == "GC":
        precision_par = PAR.from_data(args["par"]["precision"])
        precision_secret = args["par_secret"]["precision"]
        precision_secret = hugs.decrypt_data(precision_secret)
        precision_filename = precision_par.resolve(precision_secret)
        precision_file = precision_filename.download(dir="/tmp")
        site = args["site"]
        instrument = args["instrument"]
    else:
        precision_file = None
        site = None
        instrument = None

    if "overwrite" in args:
        overwrite = args["overwrite"]
    else:
        overwrite = False

    source_name = args["source_name"]

    results = process_data(data_file=data_file, source_name=source_name, precision_filepath=precision_file, 
                            data_type=data_type, site=site, instrument_name=instrument, overwrite=overwrite)

    return {"results": results}
