""" Segment the data into Datasources

"""
from typing import Dict

__all__ = ["get_split_frequency", "assign_data"]


def assign_data(data_dict: Dict, lookup_results: Dict, overwrite: bool, data_type: str = "timeseries") -> Dict[str, str]:
    """Assign data to a Datasource. This will either create a new Datasource
    Create or get an existing Datasource for each gas in the file

        Args:
            data_dict: Dictionary containing data and metadata for species
            lookup_results: Dictionary of lookup results]
            overwrite: If True overwrite current data stored
        Returns:
            dict: Dictionary of UUIDs of Datasources data has been assigned to keyed by species name
    """
    from openghg.modules import Datasource

    uuids = {}

    for key in data_dict:
        metadata = data_dict[key]["metadata"]
        data = data_dict[key]["data"]

        # Our lookup results and gas data have the same keys
        uuid = lookup_results[key]

        # TODO - Could this be done somewhere else? It doesn't feel quite right it
        # being here

        # Add the read metadata to the Dataset attributes being careful
        # not to overwrite any attributes that are already there
        to_add = {k: v for k, v in metadata.items() if k not in data.attrs}
        data.attrs.update(to_add)

        # If we have a UUID for this Datasource load the existing object
        # from the object store
        if uuid is False:
            datasource = Datasource()
        else:
            datasource = Datasource.load(uuid=uuid)

        # Add the dataframe to the datasource
        datasource.add_data(metadata=metadata, data=data, overwrite=overwrite, data_type=data_type)
        # Save Datasource to object store
        datasource.save()

        uuids[key] = datasource.uuid()

    return uuids


# def assign_footprint_data(footprint_data: Dict, lookup_results: Dict, overwrite: bool) -> Dict:
#     """ Create Datasources for the passed footprint data

#         Args:
#             data: xarray Dataset of footprint data
#             metadata: Associated metadata
#             datasource_uid: The UUID of the datasource if we've processed footprint data from this
#             source before, otherwise False
#         Returns:
#             dict: Dictionary containing Datasource UUIDs
#     """
#     from openghg.modules import Datasource

#     uuids = {}

#     # Add in copying of attributes, or add attributes to the metadata at an earlier state.
#     for key in footprint_data:
#         metadata = footprint_data[key]["metadata"]
#         data = footprint_data[key]["data"]

#         # Our lookup results and gas data have the same keys
#         uuid = lookup_results[key]

#         # TODO - Could this be done somewhere else? It doesn't feel quite right it
#         # being here

#         # Add the read metadata to the Dataset attributes being careful
#         # not to overwrite any attributes that are already there
#         to_add = {k: v for k, v in metadata.items() if k not in data.attrs}
#         data.attrs.update(to_add)

#         # If we have a UUID for this Datasource load the existing object
#         # from the object store
#         if uuid:
#             datasource = Datasource.load(uuid=uuid)
#         else:
#             datasource = Datasource()

#         # TODO - can we just ad
#         # Add the dataframe to the datasource
#         datasource.add_footprint_data(data=data, metadata=metadata, overwrite=overwrite)
#         # Save Datasource to object store
#         datasource.save()

#         uuids[key] = datasource.uuid()

#     return uuids

# def assign_emissions_data(data: Dataset, metadata: Dict, datasource_uid: Union[str, bool]) -> str:
#     """ Create Datasources for the passed flux data

#         Args:
#             data: xarray Dataset of footprint data
#             metadata: Associated metadata
#             datasource_uid: The UUID of the datasource if we've processed flux data from this
#             source before, otherwise False
#         Returns:
#             str: UUID of Datasource
#     """
#     from openghg.modules import Datasource

#     if datasource_uid is not False:
#         datasource = Datasource.load(uuid=datasource_uid)
#     else:
#         datasource = Datasource()

#     # Add the read metadata to the Dataset attributes being careful
#     # not to overwrite any attributes that are already there
#     to_add = {k: v for k, v in metadata.items() if k not in data.attrs}
#     data.attrs.update(to_add)

#     datasource.add_emissions_data(data=data, metadata=metadata)
#     datasource.save()

#     return datasource.uuid()


def get_split_frequency(data):
    """Analyses raw data for size and sets a frequency to split the data
    depending on how big the resulting dataframe will be

    Args:
        data (Pandas.Dataframe): Raw data in dataframe
        Note: DataFrame must have a Datetime index
    Returns:
        str: String selecting frequency for data splitting by Groupby
    """
    data_size = data.memory_usage(deep=True).sum()
    # If the data is larger than this it will be split into
    # separate parts
    # For now use 5 MB chunks
    segment_size = 5_242_880  # bytes

    # Get time delta for the first and last date
    start_data = data.first_valid_index()
    end_data = data.last_valid_index()

    num_years = int((end_data - start_data).days / 365.25)
    if num_years < 1:
        num_years = 1

    n_months = 12
    n_weeks = 52
    n_days = 365
    n_hours = 24

    freq = "Y"
    # Try splitting into years
    if data_size / num_years <= segment_size:
        return freq
    # Months
    elif data_size / (num_years * n_months) <= segment_size:
        freq = "M"
        return freq
    # Weeks
    elif data_size / (num_years * n_months * n_weeks) <= segment_size:
        freq = "W"
        return freq
    elif data_size / (num_years * n_months * n_weeks * n_days) <= segment_size:
        freq = "D"
        return freq
    elif data_size / (num_years * n_months * n_weeks * n_days * n_hours) <= segment_size:
        freq = "H"
        return freq
