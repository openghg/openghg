def search(args):
    from Acquire.ObjectStore import string_to_datetime
    from HUGS.Processing import search as hugs_search

    if "start_datetime" in args:
        start_datetime = string_to_datetime(args["start_datetime"])
    else:
        start_datetime = None

    if "end_datetime" in args:
        end_datetime = string_to_datetime(args["end_datetime"])
    else:
        end_datetime = None

    species = args.get("species")
    locations = args["locations"]

    inlet = args.get("inlet")
    instrument = args.get("instrument")

    results = hugs_search(
        locations=locations,
        species=species,
        inlet=inlet,
        instrument=instrument,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )

    return {"results": results}
