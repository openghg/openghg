from openghg.modules import BaseModule
from typing import Dict, Optional, Union
from pathlib import Path

__all__ = ["FOOTPRINTS"]


class FOOTPRINTS(BaseModule):
    """This class is used to process footprint model output"""

    _root = "Footprints"
    _uuid = "62db5bdf-c88d-4e56-97f4-40336d37f18c"

    def __init__(self):
        from Acquire.ObjectStore import get_datetime_now

        self._creation_datetime = get_datetime_now()
        self._stored = False
        # How we identify a
        self._datasource_hashes = {}
        # Hashes of previously uploaded files
        self._file_hashes = {}

    @staticmethod
    def read_file(
        self,
        data_filepath: Union[str, Path],
        site: str,
        network: str,
        height: str,
        model_params: Dict,
        retrieve_met: Optional[bool] = False,
        overwrite: Optional[bool] = False,
    ) -> Dict:
        """Reads footprint data files and returns the UUIDS of the Datasources
        the processed data has been assigned to

        Args:
            data_filepath: Path of file to load
            site: Site name
            network: Network name
            height: Height above ground level in metres
            model_params: Model run parameters
            retrieve_met: Whether to also download meterological data for this footprint area
            overwrite: Overwrite any currently stored data
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        # from openghg.processing import assign_attributes
        from xarray import open_dataset
        from openghg.util import hash_file, timestamp_tzaware
        from openghg.processing import assign_footprint_data

        fp = FOOTPRINTS.load()

        file_hash = hash_file(filepath=data_filepath)
        if file_hash in fp._file_hashes and not overwrite:
            raise ValueError(f"This file has been uploaded previously with the filename : {fp._file_hashes[file_hash]}.")
        else:
            fp._file_hashes[file_hash]

        data_filepath = Path(data_filepath)
        fp_data = open_dataset(data_filepath)

        # Need to read the metadata from the footprint and then store it
        # Do we need to chunk the footprint / will a Datasource store it correctly?
        metadata = {}

        metadata["data_type"] = "footprint"
        metadata["start_date"] = str(timestamp_tzaware(fp_data.time[0]))
        metadata["end_date"] = str(timestamp_tzaware(fp_data.time[-1]))

        metadata["max_longitude"] = float(fp_data["lon_high"].max())
        metadata["min_longitude"] = float(fp_data["lon_high"].min())

        metadata["max_latitude"] = float(fp_data["lat_high"].max())
        metadata["min_latitude"] = float(fp_data["lat_high"].min())

        metadata["heights"] = list(fp_data.height.values)

        metadata["model_paramters"] = model_params

        # Check if we've seen data from this site before
        site_hash = fp._get_site_hash(site=site, network=network, height=height)
        if site_hash in fp._datasource_hashes:
            datasource_uid = False
        else:
            datasource_uid = fp._datasource_hashes[site_hash]

        # Then we want to assign the data
        uid = assign_footprint_data(data=fp_data, metadata=metadata, datasource_uid=datasource_uid)

        # Record the datasource uuid
        fp._datasource_hashes[site_hash] = uid
        # Record the file hash in case we see this file again
        fp._file_hashes[file_hash] = data_filepath.name

        return {str(data_filepath.name): uid}

    def _get_metdata():
        """This retrieves the metadata for this footprint"""
        raise NotImplementedError()

    def _get_site_hash(self, site, network, height):
        from openghg.util import hash_string
        import re

        terms = [site, network, height]
        safer_terms = []
        for term in terms:
            # Make sure we don't have any spaces and it's lowercase
            safer = re.sub(r"\s+", "", term, flags=re.UNICODE).lower()
            # Make sure we only have alphanumeric values
            if re.match(r"^\w+$", safer) is None:
                raise ValueError("Please ensure site, network and height arguments only contain alphanumeric values.")

            safer_terms.append(safer)

        combined_str = "_".join(safer_terms)

        return hash_string(to_hash=combined_str)
