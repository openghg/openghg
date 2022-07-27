import json
from io import BytesIO
from typing import Dict, Optional, Union

from openghg.dataobjects import ObsData
from openghg.retrieve import get_obs_surface as local_get_obs_surface
from openghg.util import decompress, decompress_str, hash_bytes, running_locally
from pandas import Timestamp
from xarray import load_dataset


def get_obs_surface(
    site: str,
    species: str,
    inlet: str = None,
    start_date: Union[str, Timestamp] = None,
    end_date: Union[str, Timestamp] = None,
    average: str = None,
    network: str = None,
    instrument: str = None,
    calibration_scale: str = None,
    keep_missing: bool = False,
    skip_ranking: bool = False,
) -> Optional[ObsData]:
    """This is the equivalent of the get_obs function from the ACRG repository.

    Usage and return values are the same whilst implementation may differ.

    Args:
        site: Site of interest e.g. MHD for the Mace Head site.
        species: Species identifier e.g. ch4 for methane.
        start_date: Output start date in a format that Pandas can interpret
        end_date: Output end date in a format that Pandas can interpret
        inlet: Inlet label
        average: Averaging period for each dataset. Each value should be a string of
        the form e.g. "2H", "30min" (should match pandas offset aliases format).
        keep_missing: Keep missing data points or drop them.
        network: Network for the site/instrument (must match number of sites).
        instrument: Specific instrument for the sipte (must match number of sites).
        calibration_scale: Convert to this calibration scale
    Returns:
        ObsData or None: ObsData object if data found, else None
    """
    from openghg.cloud import call_function

    if not running_locally():
        to_post: Dict[str, Union[str, Dict]] = {}

        to_post["function"] = "get_obs_surface"

        search_terms = {
            "site": site,
            "species": species,
            "keep_missing": keep_missing,
            "skip_ranking": skip_ranking,
        }

        if inlet is not None:
            search_terms["inlet"] = inlet
        if start_date is not None:
            search_terms["start_date"] = start_date
        if end_date is not None:
            search_terms["end_date"] = end_date
        if average is not None:
            search_terms["average"] = average
        if network is not None:
            search_terms["network"] = network
        if instrument is not None:
            search_terms["instrument"] = instrument
        if calibration_scale is not None:
            search_terms["calibration_scale"] = calibration_scale

        to_post["search_terms"] = search_terms

        result = call_function(data=to_post)

        content = result["content"]
        found = content["found"]

        if found:
            binary_data = decompress(data=content["data"])

            file_metadata = content["file_metadata"]
            sha1_hash_data = file_metadata["data"]["sha1_hash"]

            if sha1_hash_data != hash_bytes(data=binary_data):
                raise ValueError("Hash mismatch between local SHA1 and remote SHA1.")

            buf = BytesIO(binary_data)
            json_str = decompress_str(data=content["metadata"])
            metadata = json.loads(json_str)
            dataset = load_dataset(buf)

            return ObsData(data=dataset, metadata=metadata)
        else:
            return None
    else:
        return local_get_obs_surface(
            site=site,
            species=species,
            start_date=start_date,
            end_date=end_date,
            inlet=inlet,
            average=average,
            network=network,
            instrument=instrument,
            calibration_scale=calibration_scale,
            keep_missing=keep_missing,
            skip_ranking=skip_ranking,
        )
