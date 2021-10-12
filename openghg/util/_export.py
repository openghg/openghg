from addict import Dict as aDict
from typing import Dict, List, Union
from json import loads, dump

# from openghg.dataobjects import ObsData


__all__ = ["to_dashboard"]


# def to_dashboard(
#     data: Union[ObsData, List[ObsData]], selected_vars: List, downsample_n: int = 5, filename: str = None
# ) -> Union[Dict, None]:
#     """Takes a Dataset produced by OpenGHG and outputs it into a JSON
#     format readable by the OpenGHG dashboard or a related project.

#     Args:
#         data: Dataset produced by OpenGHG
#         selected_vars: The variables to want to export
#         downsample_n: Take every nth value from the data
#         filename: filename to write output to
#     Returns:
#         None
#     """
#     if not isinstance(data, list):
#         data = [data]

#     to_export = aDict()
#     for obs in data:
#         site_name = str(obs.metadata["site"]).upper()
#         dataset = obs.data
#         metadata = obs.metadata

#         df = dataset.to_dataframe()
#         selected_df = df[selected_vars]
#         # TODO - fix dashboard so it isn't so fragile
#         # Make sure the variable names are uppercase as the dashboard expects
#         rename_dict = {k: k.upper() for k in selected_vars}
#         selected_df = selected_df.rename(columns=rename_dict)

#         selected_df = selected_df.iloc[::downsample_n]

#         species = metadata["species"].lower()

#         to_export[site_name][species]["data"] = loads(selected_df.to_json())
#         to_export[site_name][species]["metadata"] = obs.metadata

#     if filename is not None:
#         with open(filename, "w") as f:
#             dump(obj=to_export, fp=f)
#         return None
#     else:
#         return to_export


def to_dashboard(
    data: Dict, selected_vars: List, downsample_n: int = 5, filename: str = None
) -> Union[Dict, None]:
    """Takes a Dataset produced by OpenGHG and outputs it into a JSON
    format readable by the OpenGHG dashboard or a related project.

    This also exports a separate file with the locations of the sites
    for use with map selector component.

    Note - this function does not currently support export of data from multiple
    inlets.

    Args:
        data: Dictionary of retrieved data
        selected_vars: The variables to want to export
        downsample_n: Take every nth value from the data
        filename: filename to write output to
    Returns:
        None
    """
    to_export = aDict()

    selected_vars = [str(c).lower() for c in selected_vars]

    for site, species_data in data.items():
        for species, inlet_data in species_data.items():
            for inlet, data in inlet_data.items():
                dataset = data.data
                metadata = data.metadata
                df = dataset.to_dataframe()
                # We just want the selected variables
                to_extract = [c for c in df.columns if c in selected_vars]

                if not to_extract:
                    continue

                df = df[to_extract]
                # Make sure the variable names are uppercase as the dashboard expects
                rename_dict = {k: k.lower() for k in selected_vars}
                df = df.rename(columns=rename_dict)
                # Downsample the data
                df = df.iloc[::downsample_n]

                network = metadata["network"]

                site_data = to_export[network.lower()][species.lower()][site.lower()]

                site_data["data"] = loads(df.to_json())
                site_data["metadata"] = data.metadata

                # We only want data from one inlet
                break

    if filename is not None:
        with open(filename, "w") as f:
            dump(obj=to_export, fp=f)
        return None
    else:
        return to_export
