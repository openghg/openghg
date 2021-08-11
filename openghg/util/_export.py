from typing import Dict, List
from json import loads, dump


def to_dashboard(data: Dict, selected_vars: List, filename: str, downsample_n: int = 5) -> None:
    """Takes a Dataset produced by OpenGHG and outputs it into a JSON
    format readable by the OpenGHG dashboard or a related project.

    Args:
        data: Dataset produced by OpenGHG
        selected_vars: The variables to want to export
        filename: filename to write output to
        downsample_n: Take every nth value from the data
    Returns:
        None
    """
    if not isinstance(data, dict):
        raise TypeError("Please pass a dictionary of data keyed as site: Dataset")

    to_export = {}
    for site, dataset in data.items():
        df = dataset.to_dataframe()
        selected_df = df[selected_vars]
        selected_df = selected_df.iloc[::downsample_n]

        to_export[site] = loads(selected_df.to_json())

    with open(filename, "w") as f:
        dump(obj=to_export, fp=f)
