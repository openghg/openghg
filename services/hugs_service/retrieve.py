def retrieve(args):
    """ Calls the HUGS function to retrieve data stored at the given key
        and combine them into a single Pandas DataFrame for download / visualization

        Args:
            args (dict): Dictionary of arguments
        Returns:
            dict: Dictionary of results

    """
    from HUGS.Processing import recombine_sections
    from Acquire.ObjectStore import datetime_to_string
    from json import dumps as json_dumps

    if "keys" in args:
        key_dict = args["keys"]
    else:
        raise KeyError("Keys required for data retrieval")

    if "return_type" in args:
        return_type = args["return_type"]
    else:
        return_type = "json"

    if not isinstance(key_dict, dict):
        raise TypeError(
            "Keys must be passed in dictionary format. For example {bsd_co2: [key_list]}"
        )

    combined_data = {}
    for key in key_dict:
        combined = recombine_sections(key_dict[key])

        if return_type == "json":
            dataset_dict = combined.to_dict()

            # Need to convert the time data to string and then back again on the other side
            # See https://github.com/pydata/xarray/issues/2656
            datetime_data = dataset_dict["coords"]["time"]["data"]
            # Convert the datetime object to string
            for i, _ in enumerate(datetime_data):
                datetime_data[i] = datetime_to_string(datetime_data[i])

            json_data = json_dumps(dataset_dict, indent=4)
            combined_data[key] = json_data
        else:
            raise NotImplementedError("Not yet implemented")

    return {"results": combined_data}
