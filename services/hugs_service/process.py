from Acquire.Client import Authorisation, PAR
from Acquire.Service import get_this_service

from HUGS.Processing import process as _hugs_process

# Take a PAR from an uploaded file and process the data
def process(args):
    data_type = args["data_type"]
    auth = args["authorisation"]
    authorisation = Authorisation.from_data(auth)
        
    authorisation.verify("process")

    # For GC
    # PAR for data will be at args["par"]["data"]
    # If it exists precision will be at args["par"]["data"]

    # Have a PAR for each file
    par = PAR.from_data(args["file_par"])
    par_secret = args["par_secret"]

    hugs = get_this_service(need_private_access=True)
    par_secret = hugs.decrypt_data(par_secret)

    file = par.resolve(secret=par_secret)

    filename = file.download(dir="/tmp")

    data_type = "CRDS"
    # process(file_data, data_type):
    results = _hugs_process(filepath=filename, precision_filepath=data_type)

    return {"results": results}

    # This could be a dict to handle the different types of data files we'll be
    # uploading. How to handle processing of two files by GC?
    # Just work with CRDS for now?
    # filenames = file.download(dir="/tmp/hugs")
    # result["filename"] = filenames
    # print(results)


    # with open(filename, "r") as f:
    #     lines = f.readlines()
    
    # result["uploaded"] = lines

    # return result





