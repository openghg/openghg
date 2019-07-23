
def remove_objects(args):
    from HUGS.ObjectStore import get_bucket as _get_bucket
    from Acquire.ObjectStore import ObjectStore as _ObjectStore

    if "keys" in args:
        keys = args["keys"]
    else:
        raise KeyError("No keys found")

    bucket = _get_bucket()

    for key in keys:
        _ObjectStore.delete_object(bucket=bucket, key=key)
