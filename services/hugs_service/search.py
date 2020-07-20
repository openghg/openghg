def search(args):
    from Acquire.ObjectStore import string_to_datetime
    from HUGS.Processing import set_primary

    if "start_datetime" in args:
        start_datetime = _string_to_datetime(args["start_datetime"])
    else:
        start_datetime = None

    if "end_datetime" in args:
        end_datetime = _string_to_datetime(args["end_datetime"])
    else:
        end_datetime = None

    search_terms = args["search_terms"]
    locations = args["locations"]
    data_type = args["data_type"]

    results = _hugs_search(
        search_terms=search_terms,
        locations=locations,
        data_type=data_type,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )

    return {"results": results}
