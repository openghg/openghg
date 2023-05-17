""" Segment the data into Datasources

"""
from typing import Dict, List, Optional
import logging

__all__ = ["assign_data"]


logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def assign_data(
    data_dict: Dict,
    lookup_results: Dict,
    data_type: str,
    overwrite: bool,
    update_keys: Optional[List] = None,
) -> Dict[str, Dict]:
    """Assign data to a Datasource. This will either create a new Datasource
    Create or get an existing Datasource for each gas in the file

        Args:
            data_dict: Dictionary containing data and metadata for species
            lookup_results: Dictionary of lookup results]
            data_type: Type of data, one of ["surface", "emissions", "met", "footprints", "eulerian_model"].
            overwrite: If True overwrite current data stored
            update_keys: Keys from datasource which should be updated.
        Returns:
            dict: Dictionary of UUIDs of Datasources data has been assigned to keyed by species name
    """
    from openghg.store.base import Datasource

    uuids = {}

    for key in data_dict:
        metadata = data_dict[key]["metadata"]
        data = data_dict[key]["data"]

        # Our lookup results and gas data have the same keys
        uuid = lookup_results[key]

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

        new_datasource = uuid is False
        uuids[key] = {"uuid": datasource.uuid(),
                      "new": new_datasource}

        # Only add "version" if this is missing or changed
        version = datasource.latest_version()
        version_key = "latest_version"
        if version_key not in metadata or version != metadata[version_key]:
            uuids[key]["version"] = version

        # Only add "update" if datasource is not new and keys should be updated
        if not new_datasource and update_keys is not None:
            update_metadata = {}
            for key_to_update in update_keys:
                d_meta = datasource._metadata
                if key_to_update in d_meta:
                    update_metadata[key_to_update] = d_meta[key_to_update]
                else:
                    logger.warning(f"Unable to update '{key_to_update}' key in metastore."
                                   " Not present on Datasource.")

            if update_metadata:
                uuids[key]["update"] = update_metadata

    return uuids


# def assign_footprint_data(footprint_data: Dict, lookup_results: Dict, overwrite: bool) -> Dict:
#     """ Create Datasources for the passed footprints data

#         Args:
#             data: xarray Dataset of footprints data
#             metadata: Associated metadata
#             datasource_uid: The UUID of the datasource if we've processed footprints data from this
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
#             data: xarray Dataset of footprints data
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
