from openghg.modules import BaseModule
from typing import Dict, List, Tuple, Optional, Union

from openghg.modules import METData


class METStore(BaseModule):
    """ Controls the storage and retrieveal of meteorological data.

        Currently met data is retrieved from the ECMWF Copernicus data storage
        archive and then cached locally.
    """
    _root = "METStore"
    _uuid = "9fcabd0c-9b68-4ab4-a116-bc30a4472d67"

    def __init__(self):
        from Acquire.ObjectStore import get_datetime_now

        self._creation_datetime = get_datetime_now()
        self._stored = False
        # Keyed by name - allows retrieval of UUID from name
        self._datasource_names = {}
        # Keyed by UUID - allows retrieval of name by UUID
        self._datasource_uuids = {}
        # Hashes of retrieved data to ensure we aren't overwriting / duplicating
        # already stored data. Keyed by hash
        self._hashes = {}

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
        data["hashes"] = self._hashes

        return data

    def save(self, bucket: Optional[Dict] = None) -> None:
        """ Save the object to the object store

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
        """ Retrieve data from either the local METStore or from a 
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

        start_date = Timestamp(f"{years[0]}-1-1")
        end_date = Timestamp(f"{years[-1]}-12-31")

        result = store.search(search_terms=[site, network], daterange=years)

        # Retrieve from the Copernicus store
        if result is None:
            result = retrieve_met(site=site, network=network, years=years)

            store.store(met_data=result)          

        return result

    def search(self, search_terms: Union[str, List, Tuple]) -> METData:
        """ Search the stored MET data

            Args:
                search_terms: Search term(s)
            Returns:
                METData: METData object
        """
        from openghg.modules import Datasource, METData

        datasources = [Datasource.load(uuid=d.uuid(), shallow=True) for d in self._datasource_uuids]

        # We should only get one datasource here currently
        met = None
        for datasource in datasources:
            if datasource.search_metadata(search_terms=search_terms, find_all=True):
                if datasource.in_daterange()
                met = METData(data=datasource.data(), metadata=datasource.metadata())

                return met

        return met

    def store(self, met_data) -> None:
        """ Store MET data within a Datasource

            Here we do some processing on the request JSON to
            make the metadata more easily searchable and of a similar
            format to Datasources used in other modules of OpenGHG.

        """
        from openghg.modules import Datasource
        from pandas import Timestamp

        metadata = met_data.metadata
        # Adding in some abilities we'll need in the future when we do more
        # complex searching for MET data over time periods
        try:
            metadata["start_date"] = str(Timestamp(f"{metadata['year'][0]}-1-1"))
            metadata["end_date"] = str(Timestamp(f"{metadata['year'][0]}-12-31"))
        except (KeyError, IndexError):
            metadata["start_date"] = "NA"
            metadata["end_date"] = "NA"

        datasource = Datasource()
        datasource.add_data(metadata=metadata, data=met_data.data, data_type="met")
        datasource.save()

        name = "_".join(metadata["site"], metadata["network"], metadata["year"])
        self._datasource_uuids[datasource.uuid()] = name
