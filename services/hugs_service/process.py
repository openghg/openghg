from Acquire.Client import Authorisation, PAR
from Acquire.Service import get_this_service

from HUGS.Processing import proc

# Take a PAR from an uploaded file and process the data
def process(args):
    data_type = args["data_type"]

    # TODO - cleaner way to do this?
    data_par = PAR.from_data(args["par"]["data"])
    data_secret = args["par_secret"]["data"]
       
    auth = args["authorisation"]
    authorisation = Authorisation.from_data(auth)
        
    authorisation.verify("process")

    hugs = get_this_service(need_private_access=True)
    
    data_secret = hugs.decrypt_data(data_secret)
    data_file = data_par.resolve(secret=data_secret)
    data_filename = data_file.download(dir="/tmp")

    if data_type == "GC":
        precision_par = PAR.from_data(args["par"]["precision"])
        precision_secret = args["par_secret"]["precision"]
        precision_secret = hugs.decrypt_data(precision_secret)
        precision_filename = precision_par.resolve(secret-precision_secret)
    else:
        precision_filename = None

    # data_type = "GC"
    # process(file_data, data_type):
    results = proc(data_file=data_filename, precision_filepath=precision_filename, data_type=data_type)

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





