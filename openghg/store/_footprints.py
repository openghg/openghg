from openghg.store.base import BaseStore
from typing import DefaultDict, Dict, List, Optional, Union, NoReturn
from pathlib import Path
from pandas import Timestamp
from xarray import Dataset

__all__ = ["Footprints"]


class Footprints(BaseStore):
    """This class is used to process footprints model output"""

    _root = "Footprints"
    _uuid = "62db5bdf-c88d-4e56-97f4-40336d37f18c"

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        site: str,
        height: str,
        domain: str,
        model: str,
        metmodel: Optional[str] = None,
        species: Optional[str] = None,
        network: Optional[str] = None,
        retrieve_met: bool = False,
        overwrite: bool = False,
        high_res: bool = False,
        # model_params: Optional[Dict] = None,
    ) -> Dict[str, str]:
        """Reads footprints data files and returns the UUIDS of the Datasources
        the processed data has been assigned to

        Args:
            filepath: Path of file to load
            site: Site name
            network: Network name
            height: Height above ground level in metres
            domain: Domain of footprints
            model_params: Model run parameters
            retrieve_met: Whether to also download meterological data for this footprints area
            overwrite: Overwrite any currently stored data
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        from collections import defaultdict
        from xarray import open_dataset
        from openghg.util import (
            hash_file,
            timestamp_tzaware,
            timestamp_now,
            clean_string,
        )
        from openghg.store import assign_data

        filepath = Path(filepath)

        site = clean_string(site)
        network = clean_string(network)
        height = clean_string(height)
        domain = clean_string(domain)

        fp = Footprints.load()

        file_hash = hash_file(filepath=filepath)
        if file_hash in fp._file_hashes and not overwrite:
            print(
                f"This file has been uploaded previously with the filename : {fp._file_hashes[file_hash]} - skipping."
            )

        fp_data = open_dataset(filepath)

        # Need to read the metadata from the footprints and then store it
        # Do we need to chunk the footprints / will a Datasource store it correctly?
        metadata: Dict[str, Union[str, float, List[float]]] = {}

        metadata["data_type"] = "footprints"
        metadata["site"] = site
        metadata["height"] = height
        metadata["domain"] = domain
        metadata["model"] = model

        if species is not None:
            metadata["species"] = clean_string(species)

        if network is not None:
            metadata["network"] = clean_string(network)

        if metmodel is not None:
            metadata["metmodel"] = clean_string(metmodel)

        metadata["start_date"] = str(timestamp_tzaware(fp_data.time[0].values))
        metadata["end_date"] = str(timestamp_tzaware(fp_data.time[-1].values))

        metadata["max_longitude"] = round(float(fp_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(fp_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(fp_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(fp_data["lat"].min()), 5)
        metadata["time_resolution"] = "standard_time_resolution"

        # If it's a high resolution footprints file we'll have two sets of lat/long values
        if high_res:
            try:
                metadata["max_longitude_high"] = round(float(fp_data["lon_high"].max()), 5)
                metadata["min_longitude_high"] = round(float(fp_data["lon_high"].min()), 5)
                metadata["max_latitude_high"] = round(float(fp_data["lat_high"].max()), 5)
                metadata["min_latitude_high"] = round(float(fp_data["lat_high"].min()), 5)
                metadata["time_resolution"] = "high_time_resolution"
            except KeyError:
                raise KeyError("Unable to find lat_high or lon_high data.")

        metadata["heights"] = [float(h) for h in fp_data.height.values]
        # Do we also need to save all the variables we have available in this footprints?
        metadata["variables"] = list(fp_data.keys())

        # if model_params is not None:
        #     metadata["model_parameters"] = model_params

        # Set the attributes of this Dataset
        fp_data.attrs = {"author": "OpenGHG Cloud", "processed": str(timestamp_now())}

        # This might seem longwinded now but will help when we want to read
        # more than one footprints at a time
        key = "_".join((site, domain, model, height))

        footprint_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
        footprint_data[key]["data"] = fp_data
        footprint_data[key]["metadata"] = metadata

        # This will be removed when we process multiple files
        keyed_metadata = {key: metadata}

        lookup_results = fp.datasource_lookup(metadata=keyed_metadata)

        data_type = "footprints"
        datasource_uuids: Dict[str, str] = assign_data(
            data_dict=footprint_data,
            lookup_results=lookup_results,
            overwrite=overwrite,
            data_type=data_type,
        )

        fp.add_datasources(datasource_uuids=datasource_uuids, metadata=keyed_metadata)

        # Record the file hash in case we see this file again
        fp._file_hashes[file_hash] = filepath.name

        fp.save()

        return datasource_uuids

    def lookup_uuid(self, site: str, domain: str, model: str, height: str) -> Union[str, bool]:
        """Perform a lookup for the UUID of a Datasource

        Args:
            site: Site code
            domain: Domain
            model: Model name
            height: Height
        Returns:
            str or dict: UUID or False if no entry
        """
        uuid = self._datasource_table[site][domain][model][height]

        return uuid if uuid else False

    def set_uuid(self, site: str, domain: str, model: str, height: str, uuid: str) -> None:
        """Record a UUID of a Datasource in the datasource table

        Args:
            site: Site code
            domain: Domain
            model: Model name
            height: Height
            uuid: UUID of Datasource
        Returns:
            None
        """
        self._datasource_table[site][domain][model][height] = uuid

    def datasource_lookup(self, metadata: Dict) -> Dict:
        """Find the Datasource we should assign the data to

        Args:
            metadata: Dictionary of metadata
        Returns:
            dict: Dictionary of datasource information
        """
        # TODO - I'll leave this as a function for now as the way we read footprints may
        # change in the near future
        # GJ - 2021-04-20
        lookup_results = {}

        for key, data in metadata.items():
            site = data["site"]
            model = data["model"]
            height = data["height"]
            domain = data["domain"]

            result = self.lookup_uuid(site=site, domain=domain, model=model, height=height)

            if not result:
                result = False

            lookup_results[key] = result

        return lookup_results

    def add_datasources(self, datasource_uuids: Dict, metadata: Dict) -> None:
        """Add the passed list of Datasources to the current list

        Args:
            datasource_uuids: Datasource UUIDs
            metadata: Metadata for each species
        Returns:
            None
        """
        for key, uid in datasource_uuids.items():
            md = metadata[key]
            site = md["site"]
            model = md["model"]
            height = md["height"]
            domain = md["domain"]

            result = self.lookup_uuid(site=site, domain=domain, model=model, height=height)

            if result and result != uid:
                raise ValueError("Mismatch between assigned uuid and stored Datasource uuid.")
            else:
                self.set_uuid(site=site, domain=domain, model=model, height=height, uuid=uid)
                self._datasource_uuids[uid] = key

    def save(self) -> None:
        """Save the object to the object store

        Returns:
            None
        """
        from openghg.objectstore import get_bucket, set_object_from_json

        bucket = get_bucket()

        obs_key = f"{Footprints._root}/uuid/{Footprints._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    def search(
        self,
        site: str,
        network: str,
        start_date: Optional[Union[str, Timestamp]],
        end_date: Optional[Union[str, Timestamp]],
    ) -> NoReturn:
        """Search for a footprints from a specific site and network, return a dictionary of data
        so the user can choose
        """
        raise NotImplementedError()

    def retrieve(self, uuid: str, dates: str) -> NoReturn:
        """"""
        raise NotImplementedError()

    def _get_metdata(self) -> NoReturn:
        """This retrieves the metadata for this footprints"""
        raise NotImplementedError()
