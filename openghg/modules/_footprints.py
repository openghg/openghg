from openghg.modules import BaseModule
from typing import Dict, Optional, Union
from pathlib import Path
from pandas import Timestamp

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
        self._datasource_uuids = {}
        # TODO - remove this - currently here for compatibility with other 
        # storage objects
        self._datasource_names = {}
        # Hashes of previously uploaded files
        self._file_hashes = {}
        self._rank_data = {}

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
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
            filepath: Path of file to load
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

        file_hash = hash_file(filepath=filepath)
        if file_hash in fp._file_hashes and not overwrite:
            raise ValueError(f"This file has been uploaded previously with the filename : {fp._file_hashes[file_hash]}.")

        filepath = Path(filepath)
        fp_data = open_dataset(filepath)

        # Need to read the metadata from the footprint and then store it
        # Do we need to chunk the footprint / will a Datasource store it correctly?
        metadata = {}

        metadata["data_type"] = "footprint"
        metadata["site"] = site
        metadata["network"] = network
        metadata["height"] = height

        metadata["start_date"] = str(timestamp_tzaware(fp_data.time[0].values))
        metadata["end_date"] = str(timestamp_tzaware(fp_data.time[-1].values))

        metadata["max_longitude"] = round(float(fp_data["lon_high"].max()), 5)
        metadata["min_longitude"] = round(float(fp_data["lon_high"].min()), 5)
        metadata["max_latitude"] = round(float(fp_data["lat_high"].max()), 5)
        metadata["min_latitude"] = round(float(fp_data["lat_high"].min()), 5)

        metadata["heights"] = [float(h) for h in fp_data.height.values]
        # Do we also need to save all the variables we have available in this footprint?
        metadata["variables"] = list(fp_data.keys())

        metadata["model_parameters"] = model_params

        # Check if we've seen data from this site before
        site_hash = fp._get_site_hash(site=site, network=network, height=height)

        if site_hash in fp._datasource_uuids:
            datasource_uid = fp._datasource_uuids[site_hash]
        else:
            datasource_uid = False

        # Then we want to assign the data
        # This only returns the UID string, not a dictionary including the name
        # This behaviour is different to assign_data which will be changed soon
        uid = assign_footprint_data(data=fp_data, metadata=metadata, datasource_uid=datasource_uid)

        fp.add_datasources(datasource_uuids={site_hash: uid})
        # Record the file hash in case we see this file again
        fp._file_hashes[file_hash] = filepath.name

        fp.save()

        return {str(filepath.name): uid}

    def to_data(self) -> Dict:
        """ Return a JSON-serialisable dictionary of object
        for storage in object store

        Returns:
            dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string

        data = {}
        data["creation_datetime"] = datetime_to_string(self._creation_datetime)
        data["stored"] = self._stored
        data["datasource_uuids"] = self._datasource_uuids
        data["datasource_names"] = self._datasource_names
        data["file_hashes"] = self._file_hashes
        data["rank_data"] = self._rank_data

        return data

    def save(self) -> None:
        """ Save the object to the object store

        Returns:
            None
        """
        from openghg.objectstore import get_bucket, set_object_from_json

        bucket = get_bucket()

        obs_key = f"{FOOTPRINTS._root}/uuid/{FOOTPRINTS._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    def search(self, site: str, network: str, start_date: Optional[Union[str, Timestamp]], end_date: Optional[Union[str, Timestamp]]):
        """ Search for a footprint from a specific site and network, return a dictionary of data
            so the user can choose
        """
        raise NotImplementedError()

    def retrieve(self, uuid, dates):
        """

        """
        raise NotImplementedError()

    def _get_metdata():
        """This retrieves the metadata for this footprint"""
        raise NotImplementedError()

    def _get_site_hash(self, site, network, height):
        from openghg.util import hash_string
        import re

        # Extract only the number from the height
        try:
            height = re.findall(r"\d+(?:\.\d+)?", height)[0]
        except IndexError:
            raise ValueError("Cannot read height string, please check it contains the correct value.")

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
