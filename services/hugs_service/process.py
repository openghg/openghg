from Acquire.Client import Authorisation, PAR
from Acquire.Service import get_this_service

from HUGS.Processing import Process

# Take a PAR from an uploaded file and process the data
def process(args):
    data_type = args["data_type"]

    par = PAR.from_data(args["file_par"])
    par_secret = args["par_secret"]

    hugs = get_this_service(need_private_access=True)
    par_secret = hugs.decrypt_data(par_secret)
    
    file = par.resolve(secret=par_secret)
    result["file"] = str(file)

    # This could be a dict to handle the different types of data files we'll be
    # uploading. How to handle processing of two files by GC?
    # Just work with CRDS for now?
    filenames = file.download(dir"/tmp/hugs")
    result["filename"] = filenames

    res = process(some_kind_of_args_here)

    with open(filename, "r") as f:
        lines = f.readlines()
    
    result["uploaded"] = lines

    return result





