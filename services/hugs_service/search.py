from HUGS.ObjectStore import get_object_names, get_bucket
from HUGS.Processing import gas_search

def search(args):
    species = args["species"]
    data_type = args["data_type"]

    if "start_datetime" in args:
        start_datetime = args["start_datetime"]
    else:
        start_datetime = None

    if "end_datetime" in args:
        end_datetime = args["end_datetime"]
    else:
        end_datetime = None

    results = gas_search(species=species, data_type=data_type, 
                            start_datetime=start_datetime, end_datetime=end_datetime)

    return {"results" : results}