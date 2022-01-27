from typing import Dict


__all__ = ["retrieve"]


def retrieve(args: Dict) -> Dict:
    """ Calls the OpenGHG function to retrieve data stored at the given key
        and combine them into a single Pandas DataFrame for download / visualization

        Args:
            args: Dictionary of arguments
        Returns:
            dict: Dictionary of results
    """
    from openghg.store import recombine_multisite

    keys = args["keys"]
    # Get a dictionary of xarray Datasets
    data = recombine_multisite(keys=keys, sort=True)
    # We want each Dataset as bytes
    xr_bytes = {key: ds.to_netcdf() for key, ds in data.items()}

    return {"results": xr_bytes}

        # for daterange in dateranges:
        #     # Create a key for this range
        #     data_keys = key_dict[key][daterange]
        #     # Retrieve the data from the object store and combine into a NetCDF
        #     combined = recombine_sections(data_keys)

        #     # Here just return the NetCDF as bytes
        #     if return_type == "binary":
        #         netcdf_bytes = combined.to_netcdf()
        #     elif return_type == "json":
        #         dataset_dict = combined.to_dict()

        #         # Need to convert the time data to string and then back again on the other side
        #         # See https://github.com/pydata/xarray/issues/2656
        #         datetime_data = dataset_dict["coords"]["time"]["data"]
        #         # Convert the datetime object to string
        #         for i, _ in enumerate(datetime_data):
        #             datetime_data[i] = datetime_to_string(datetime_data[i])

        #         json_data = json_dumps(dataset_dict, indent=4)
        #         combined_data[key][daterange] = json_data
        #     else:
        #         raise NotImplementedError("Not yet implemented")

    
