from openghg.modules import BaseModule
from pandas import Timestamp
from typing import Dict, List, Tuple, Optional, Union

from openghg.modules import METData


class METStore(BaseModule):
    """Controls the storage and retrieveal of meteorological data.

    Currently met data is retrieved from the ECMWF Copernicus data storage
    archive and then cached locally.
    """

    _root = "METStore"
    _uuid = "9fcabd0c-9b68-4ab4-a116-bc30a4472d67"

    def __init__(self):
        from Acquire.ObjectStore import get_datetime_now
        from collections import defaultdict

        self._creation_datetime = get_datetime_now()
        self._stored = False

        # We want to created a nested dictionary
        def nested_dict():
            return defaultdict(nested_dict)

        self._datasource_table = nested_dict()
        # Keyed by name - allows retrieval of UUID from name
        self._datasource_names = {}  
        # Keyed by UUID - allows retrieval of name by UUID
        self._datasource_uuids = {}
        # Hashes of retrieved data to ensure we aren't overwriting / duplicating
        # already stored data. Keyed by hash
        self._hashes = {}

    def to_data(self) -> Dict:
        """Return a JSON-serialisable dictionary of object
        for storage in object store

        Returns:
            dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string

        data = {}
        data["creation_datetime"] = datetime_to_string(self._creation_datetime)
        data["stored"] = self._stored
        data["datasource_table"] = self._datasource_table
        data["datasource_uuids"] = self._datasource_uuids
        data["datasource_names"] = self._datasource_names
        data["file_hashes"] = self._hashes

        return data

    def save(self, bucket: Optional[Dict] = None) -> None:
        """Save the object to the object store

        Args:
            bucket: Bucket for data
        Returns:
            None
        """
        from openghg.objectstore import get_bucket, set_object_from_json

        if bucket is None:
            bucket = get_bucket()

        obs_key = f"{METStore._root}/uuid/{METStore._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    @staticmethod
    def retrieve(site: str, network: str, years: Union[str, List[str]]) -> METData:
        """Retrieve data from either the local METStore or from a
        remote store if we don't have it locally

        Args:
            site: Three letter site code
            network: Network name
            year: Year(s) required
        Returns:
            METData: METData object holding data and metadata
        """
        from openghg.modules import retrieve_met
        from pandas import Timestamp

        if not isinstance(years, list):
            years = [years]
        else:
            years = sorted(years)

        store = METStore.load()

        # We'll just do full years for now, I don't think it's a huge amount of data (currently)
        start_date = Timestamp(f"{years[0]}-1-1")
        end_date = Timestamp(f"{years[-1]}-12-31")

        result = store.search(search_terms=[site, network], start_date=start_date, end_date=end_date)

        # Retrieve from the Copernicus store
        if result is None:
            result = retrieve_met(site=site, network=network, years=years)

            store._store(met_data=result)

        return result

    def search(
        self, search_terms: Union[str, List, Tuple], start_date: Union[str, Timestamp], end_date: Union[str, Timestamp]
    ) -> Union[METData, None]:
        """Search the stored MET data

        Args:
            search_terms: Search term(s)
        Returns:
            METData or None: METData object if found else None
        """
        from openghg.modules import Datasource, METData

        datasources = [Datasource.load(uuid=uuid, shallow=True) for uuid in self._datasource_uuids]

        # We should only get one datasource here currently
        for datasource in datasources:
            if datasource.search_metadata(search_terms=search_terms, find_all=True):
                if datasource.in_daterange(start_date=start_date, end_date=end_date):
                    data = next(iter(datasource.data().values()))
                    return METData(data=data, metadata=datasource.metadata())

    def _store(self, met_data) -> None:
        """Store MET data within a Datasource

        Here we do some processing on the request JSON to
        make the metadata more easily searchable and of a similar
        format to Datasources used in other modules of OpenGHG.

        """
        from openghg.modules import Datasource

        metadata = met_data.metadata

        datasource = Datasource()
        datasource.add_data(metadata=metadata, data=met_data.data, data_type="met")
        datasource.save()

        date_str = f"{metadata['start_date']}_{metadata['end_date']}"

        name = "_".join((metadata["site"], metadata["network"], date_str))
        self._datasource_uuids[datasource.uuid()] = name
        # Write this updated object back to the object store
        self.save()
