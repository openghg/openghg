from HUGS.ObjectStore import get_object_names, get_bucket

def listobjects(args):

    try:
        prefix = args["prefix"]
    except:
        prefix = None

    bucket = get_bucket()
    results = get_object_names(bucket=bucket, prefix=prefix)

    return {"results": results}

