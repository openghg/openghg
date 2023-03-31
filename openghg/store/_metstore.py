from __future__ import annotations

from typing import TYPE_CHECKING, List, Union, Optional

from openghg.store.base import BaseStore
from pandas import Timestamp
import pandas as pd

if TYPE_CHECKING:
    from openghg.dataobjects import METData


class METStore(BaseStore):
    """Controls the storage and retrieveal of meteorological data.

    Currently met data is retrieved from the ECMWF Copernicus data storage
    archive and then cached locally.
    """

    _root = "METStore"
    _uuid = "9fcabd0c-9b68-4ab4-a116-bc30a4472d67"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"


    def save(self) -> None:
        """Save the object to the object store

        Args:
            bucket: Bucket for data
        Returns:
            None
        """
        from openghg.objectstore import get_bucket, set_object_from_json

        bucket = get_bucket()

        obs_key = f"{METStore._root}/uuid/{METStore._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    @staticmethod
    def retrieve(site: str, network: str, years: Optional[Union[str, List[str]]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, variables: Optional[List[str]] = None, key_path: Optional[str] = None, save_path: Optional[str] = None,) -> METData:
        """Retrieve data from either the local METStore or from a
        remote store if we don't have it locally

        Args:
            site: Three letter site code
            network: Network name
            years: Year(s) required
        Returns:
            METData: METData object holding data and metadata
        """
        from openghg.retrieve.met import retrieve_met
        from pandas import Timestamp

        # check right date 
        if years==None and (start_date==None or end_date==None):
            raise AttributeError("You must pass either the argument years or both start_date and end_date")

        if years is not None:
            if not isinstance(years, list):
                years = [years]
            else:
                years = sorted(years)
            
            start_date = Timestamp(f"{years[0]}-1-1")
            end_date = Timestamp(f"{years[-1]}-12-31")
        
        else:
            start_date = Timestamp(start_date)
            end_date = Timestamp(end_date)            


        store = METStore.load()

        # We'll just do full years for now, I don't think it's a huge amount of data (currently)

        print("Retrieving")
        result = store.search(site=site, network=network, start_date=start_date, end_date=end_date)
        
        #print(f"search result {result}")
        # Retrieve from the Copernicus store
        if result is None:

            data = retrieve_met(site=site, network=network, start_date=start_date, end_date=end_date, save_path=save_path, variables=variables, key_path=key_path)

            print("Storing")
            store._store(data)
            store.save()
        else:
            print("File already exists")

        return result

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
        from openghg.dataobjects import METData
        from openghg.store.base import Datasource

        datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in self._datasource_uuids)
        #print(self._datasource_uuids)
        # We should only get one datasource here currently
        for datasource in datasources:
            if datasource.search_metadata(site=site, network=network, find_all=True):
                if datasource.in_daterange(start_date=start_date, end_date=end_date):
                    data = next(iter(datasource.data().values()))
                    return METData(data=data, metadata=datasource.metadata())

        return None

    def _store(self, met_data: METData) -> None:
        """Store MET data within a Datasource

        Here we do some retrieve on the request JSON to
        make the metadata more easily searchable and of a similar
        format to Datasources used in other modules of OpenGHG.

        Args:
            met_data: Dataset
        Returns:
            None
        """
        from openghg.store.base import Datasource

        metadata = met_data.metadata
        
        from openghg.store import assign_data, datasource_lookup, load_metastore
        metastore = load_metastore(key=self._metakey)
        
        met = METStore.load()


        datasource = Datasource()
        datasource.add_data(metadata=metadata, data=met_data.data, data_type="met")
        datasource.save()
        

        date_str = f"{metadata['start_date']}_{metadata['end_date']}"

        name = "_".join((metadata["site"], metadata["network"], date_str))
        self._datasource_uuids[datasource.uuid()] = name

        met.add_datasources(uuids=self._datasource_uuids[datasource.uuid()], data=met_data, metastore=metastore)

        met.save()
        metastore.close()

        
        # Write this updated object back to the object store
        self.save()
