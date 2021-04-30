def remove_objects(args):
    from openghg.objectstore import get_bucket, delete_object

    if "keys" in args:
        keys = args["keys"]
        if not keys:
            raise ValueError("No keys in list")
    else:
        raise KeyError("No keys found")

    bucket = get_bucket()

    for key in keys:
        delete_object(bucket=bucket, key=key)
