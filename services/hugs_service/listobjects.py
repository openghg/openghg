from HUGS.ObjectStore import get_bucket, get_object_names


def listobjects(args):

    try:
        prefix = args["prefix"]
    except KeyError:
        prefix = None

    bucket = get_bucket()
    results = get_object_names(bucket=bucket, prefix=prefix)

    return {"results": results}
