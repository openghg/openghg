from typing import Dict, List, Union
from json import loads, dump
from openghg.dataobjects import ObsData


def to_dashboard(
    data: Union[ObsData, List[ObsData]], selected_vars: List, downsample_n: int = 5, filename: str = None
) -> Union[Dict, None]:
    """Takes a Dataset produced by OpenGHG and outputs it into a JSON
    format readable by the OpenGHG dashboard or a related project.

    Args:
        data: Dataset produced by OpenGHG
        selected_vars: The variables to want to export
        downsample_n: Take every nth value from the data
        filename: filename to write output to
    Returns:
        None
    """
    if not isinstance(data, list):
        data = [data]

    to_export = {}
    for obs in data:
        site_name = obs.metadata["site"]
        dataset = obs.data
        df = dataset.to_dataframe()
        selected_df = df[selected_vars]
        selected_df = selected_df.iloc[::downsample_n]

        to_export[site_name] = loads(selected_df.to_json())

    if filename is not None:
        with open(filename, "w") as f:
            dump(obj=to_export, fp=f)
        return None
    else:
        return to_export
