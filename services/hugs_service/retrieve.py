
def retrieve(args):
    """ Calls the HUGS function to retrieve data stored at the given keyd
        and combine them into a single Pandas DataFrame for download / visualization

        Args:
            args (dict): Dictionary of arguments
        Returns:
            dict: Dictionary of results

    """
    from HUGS.Processing import recombine_sections as _recombine_sections
    from collections import defaultdict as _defaultdict

    if "keys" in args:
        key_dict = args["keys"]
    else:
        raise KeyError("Keys required for data retrieval")

    if "return_type" in args:
        return_type = args["return_type"]
    else:
        return_type = "json"

    # Recombine the data by key and save to a dictionary for returning
    combined_data = {}
    for key in key_dict:
        combined = _recombine_sections(key_dict[key])
        if return_type == "json":
            combined_data[key] = combined.to_json()
        else:
            raise NotImplementedError("Not yet implemented")

    return {"results": combined_data}
