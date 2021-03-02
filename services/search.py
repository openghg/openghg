def search(args):
    from Acquire.ObjectStore import string_to_datetime
    from openghg.processing import search as hugs_search

    if "start_date" in args:
        start_date = string_to_datetime(args["start_date"])
    else:
        start_date = None

    if "end_date" in args:
        end_date = string_to_datetime(args["end_date"])
    else:
        end_date = None

    species = args.get("species")
    locations = args["locations"]

    inlet = args.get("inlet")
    instrument = args.get("instrument")

    results = hugs_search(
        locations=locations,
        species=species,
        inlet=inlet,
        instrument=instrument,
        start_date=start_date,
        end_date=end_date,
    )

    return {"results": results}
