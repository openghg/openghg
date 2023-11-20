from __future__ import annotations

import logging

from typing import List, Union, Optional, Dict

from openghg.store.base import BaseStore
from pandas import Timestamp

import os
import glob

import xarray as xr
from openghg.dataobjects import METData
from openghg.util import to_lowercase

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)


class METStore(BaseStore):
    """Controls the storage and retrieveal of meteorological data.

    Currently met data is retrieved from the ECMWF Copernicus data storage
    archive and then cached locally.
    """

    _root = "METStore"
    _uuid = "9fcabd0c-9b68-4ab4-a116-bc30a4472d67"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def __init__(self, bucket: str) -> None:
        super().__init__(bucket)

        self.required_keys = ["site", "network", "start_date", "end_date"]

        self._datasource_uuids_dict: Dict[str, str] = {}

    # def save(self) -> None:
    #     """Save the object to the object store
    #     TODO is this redundant?
    #     Args:
    #         bucket: Bucket for data
    #     Returns:
    #         None
    #     """
    #     raise NotImplementedError("We are working to replace the MetStore.")
    #     from openghg.objectstore import get_bucket, set_object_from_json

    #     bucket = get_bucket()

    #     obs_key = f"{METStore._root}/uuid/{METStore._uuid}"

    #     self._stored = True
    #     set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    def retrieve(
        self,
        site: str,
        network: str,
        years: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        variables: Optional[List[str]] = None,
        save_path: Optional[str] = None,
    ) -> Union[METData, None]:
        """Retrieve data from the local METStore, or from a
        remote store if not found locally

        Args:
            site: Three letter site code
            network: Network name
            years: Year(s) required
            start_date and end_date: full-date string or month/year strings defining the beginning and end of period to retrieve (either years or start_date and end_date are required)
            variables: list of variables to retrieve
            save_path: path to save met data. Defaults to openghg/metdata
        Returns:
            METData: METData object holding data and metadata
        """
        from openghg.retrieve.met import retrieve_met
        from pandas import Timestamp

        # check right date
        if years is None and (start_date is None or end_date is None):
            raise AttributeError("You must pass either the argument years or both start_date and end_date")

        if years is None:
            start_date = Timestamp(start_date)
            end_date = Timestamp(end_date)
        else:
            if not isinstance(years, list):
                years = [years]
            else:
                years = sorted(years)

            start_date = Timestamp(f"{years[0]}-1-1")
            end_date = Timestamp(f"{years[-1]}-12-31")

        # store = METStore.load()

        logger.info("Retrieving")

        # check the local store
        result = self.search(site=site, network=network, start_date=start_date, end_date=end_date)

        # If not found in the local store, retrieve from the Copernicus store and save
        if result is None:

            result = retrieve_met(
                site=site,
                network=network,
                start_date=start_date,
                end_date=end_date,
                save_path=save_path,
                variables=variables,
            )

            logger.info("Storing")

            metadata = {
                "site": site,
                "network": network,
                "variables": result.metadata["variable"],
                "product_type": result.metadata["product_type"],
                "format": result.metadata["format"],
                "pressure_level": result.metadata["pressure_level"],
                "area": result.metadata["area"],
                "start_date": str(start_date),
                "end_date": str(end_date),
            }
            result = METData(data=result.data, metadata=metadata)
            self._store(result)
        else:
            logger.info("File already exists in the local store")

        return result

    def populate(self, store_path: Optional[str] = None) -> None:
        """
        Populate MetStore with existing met files.
        Args:
            store_path: path to folder with met data. Defaults to openghg/metdata
        """

        default_store_path = os.path.join(os.getcwd().split("openghg")[0], "openghg/metdata")
        store_path = default_store_path if store_path is None else store_path

        files = glob.glob(os.path.join(store_path, "Met_*.nc"))

        min_keys = len(self.required_keys)

        successful_files_added = 0
        sites_added = []

        for file in files:
            try:
                dataset = xr.open_dataset(file)

            except KeyError:
                logger.warn(f"Could not open file {file}")
                continue

            # check required metadata is present in attributes
            required_metadata = {
                k.lower(): to_lowercase(v) for k, v in dataset.attrs.items() if k in self.required_keys
            }

            if len(required_metadata) < min_keys:
                logger.warn(
                    f"The metadata for file {file} doesn't contain enough information, we need: {self.required_keys}"
                )
                continue

            metadata = dict((key, str(dataset.attrs[key])) for key in list(dataset.attrs.keys()))

            data = METData(data=dataset, metadata=metadata)
            self._store(met_data=data)

            successful_files_added += 1
            sites_added.append(metadata["site"])

        logger.info(f"Added {successful_files_added} files for {len(set(sites_added))} different sites")

    def search(
        self,
        site: str,
        network: str,
        start_date: Union[str, Timestamp],
        end_date: Union[str, Timestamp],
    ) -> Union[METData, None]:
        """Search the stored MET data

        Args:
            site: Site code
            network: Network name
            start_date: Start date
            end_date: End date

        Returns:
            METData or None: METData object if found else None
        """
        from openghg.store.base import Datasource

        datasources = (
            Datasource.load(uuid=uuid, bucket=self._bucket, shallow=True)
            for uuid in self._datasource_uuids_dict
        )

        for datasource in datasources:
            if datasource.search_metadata(site=site, network=network, find_all=True):
                if datasource.in_daterange(start_date=start_date, end_date=end_date):
                    data = next(iter(datasource.data().values()))

                    return METData(data=data, metadata=datasource.metadata())

        return None

    # def store_data(
    #     self,
    #     data: Dict,
    #     overwrite: bool = False,
    #     force: bool = False,
    #     required_metakeys: Optional[Sequence] = None,
    # ) -> Optional[Dict]:
    #     """Store data in metedata - note that this function might be redundant against self._store
    #     Args:
    #         data: Dictionary of data in standard format, see the data spec under
    #             Development -> Data specifications in the documentation
    #         overwrite: If True overwrite currently stored data
    #         force: Force adding of data even if this is identical to data stored (checked based on previously retrieved file hashes).
    #         required_metakeys: Keys in the metadata we should use to store this metadata in the object store
    #             if None it defaults to:
    #                 TODO update this
    #     Returns:
    #         Dict or None:
    #     """
    #     from openghg.util import hash_retrieved_data

    #     # Very rudimentary hash of the data and associated metadata
    #     data = {"met": data}
    #     hashes = hash_retrieved_data(to_hash=data)

    #     # Find the keys in data we've seen before
    #     if force:
    #         file_hashes_to_compare = set()
    #     else:
    #         file_hashes_to_compare = {next(iter(v)) for k, v in hashes.items() if k in self._retrieved_hashes}

    #     if len(file_hashes_to_compare) == len(data):
    #         logger.warning("Note: There is no new data to process.")
    #         return None

    #     keys_to_process = set(data.keys())
    #     if file_hashes_to_compare:
    #         # TODO - add this to log
    #         logger.warning(f"Note: We've seen {file_hashes_to_compare} before. Processing new data only.")
    #         keys_to_process -= file_hashes_to_compare

    #     to_process = {k: v for k, v in data.items() if k in keys_to_process}

    #     if required_metakeys is None:
    #         required_metakeys = self.required_keys

    #     # Create Datasources, save them to the object store and get their UUIDs
    #     data_type = "met"
    #     # This adds the parsed data to new or existing Datasources by performing a lookup
    #     # in the metastore
    #     datasource_uuids = self.assign_data(
    #         data=to_process,
    #         overwrite=overwrite,
    #         data_type=data_type,
    #         required_keys=required_metakeys,
    #     )

    #     self.store_hashes(hashes=hashes)
    #     self._datasource_uuids_dict.append(datasource_uuids)

    #     return datasource_uuids

    def store_hashes(self, hashes: Dict) -> None:
        """Store hashes of data retrieved from Copernicus. This takes the full dictionary of hashes, removes the ones we've seen before and adds the new.

        Args:
            hashes: Dictionary of hashes provided by the hash_retrieved_data function
        Returns:
            None
        """
        new = {k: v for k, v in hashes.items() if k not in self._retrieved_hashes}
        self._retrieved_hashes.update(new)

    def _store(self, met_data: METData) -> None:
        """Store MET data within a Datasource
        This function might replace store_data
        Args:
            met_data: Dataset
        Returns:
            None
        """
        # raise NotImplementedError("We are working to replace the MetStore.")

        # from openghg.store import load_metastore  # ,assign_data, datasource_lookup

        metadata = met_data.metadata

        # metastore = load_metastore(key=self._metakey)

        # met = METStore.load()
        from openghg.store.base import Datasource

        datasource = Datasource()
        datasource.add_data(metadata=metadata, data=met_data.data, data_type="met")
        datasource.save(bucket=self._bucket)

        date_str = f"{metadata['start_date']}_{metadata['end_date']}"

        name = "_".join((metadata["site"], metadata["network"], date_str))
        self._datasource_uuids_dict[datasource.uuid()] = name

        # met.add_single_datasource(uuid={name: datasource.uuid()}, data=met_data, metastore=metastore)

        # met.save()
        # metastore.close()

        # Write this updated object back to the object store
        self.save()

    def see_datasources(self) -> Dict[str, str]:
        """Return the list of Datasources UUIDs associated with this object
        Equivalent to datasources in Base, but edited to work around typechecking

        Returns:
            list: List of Datasource UUIDs
        """
        return self._datasource_uuids_dict
