from __future__ import annotations

from typing import TYPE_CHECKING

from openghg.store.base import BaseStore
from pandas import Timestamp

if TYPE_CHECKING:
    from openghg.dataobjects import METData


class METStore(BaseStore):
    """Controls the storage and retrieveal of meteorological data.

    Currently met data is retrieved from the ECMWF Copernicus data storage
    archive and then cached locally.
    """

    _root = "METStore"
    _uuid = "9fcabd0c-9b68-4ab4-a116-bc30a4472d67"

    def save(self) -> None:
        """Save the object to the object store

        Args:
            bucket: Bucket for data
        Returns:
            None
        """
        raise NotImplementedError("We are working to replace the MetStore.")
        from openghg.objectstore import get_bucket, set_object_from_json

        bucket = get_bucket()

        obs_key = f"{METStore._root}/uuid/{METStore._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    @staticmethod
    def retrieve(site: str, network: str, years: str | list[str]) -> METData:
        """Retrieve data from either the local METStore or from a
        remote store if we don't have it locally

        Args:
            site: Three letter site code
            network: Network name
            years: Year(s) required
        Returns:
            METData: METData object holding data and metadata
        """
        raise NotImplementedError("We are working to replace the MetStore.")
        from openghg.retrieve.met import retrieve_met
        from pandas import Timestamp

        if not isinstance(years, list):
            years = [years]
        else:
            years = sorted(years)

        store = METStore.load()

        # We'll just do full years for now, I don't think it's a huge amount of data (currently)
        start_date = Timestamp(f"{years[0]}-1-1")
        end_date = Timestamp(f"{years[-1]}-12-31")

        result = store.search(site=site, network=network, start_date=start_date, end_date=end_date)

        # Retrieve from the Copernicus store
        if result is None:
            result = retrieve_met(site=site, network=network, years=years)

            store._store(met_data=result)

        return result

    def search(
        self,
        site: str,
        network: str,
        start_date: str | Timestamp,
        end_date: str | Timestamp,
    ) -> METData | None:
        """Search the stored MET data

        Args:
            site: Site code
            network: Network name
            start_date: Start date
            end_date: End date

        Returns:
            METData or None: METData object if found else None
        """
        raise NotImplementedError("We are working to replace the MetStore.")
        from openghg.dataobjects import METData
        from openghg.store.base import Datasource

        datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in self._datasource_uuids)

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
        raise NotImplementedError("We are working to replace the MetStore.")
        from openghg.store.base import Datasource

        metadata = met_data.metadata

        datasource = Datasource()
        datasource.add_data(metadata=metadata, data=met_data.data, data_type="met")
        datasource.save()

        date_str = f"{metadata['start_date']}_{metadata['end_date']}"

        name = "_".join((metadata["site"], metadata["network"], date_str))
        self._datasource_uuids[datasource.uuid()] = name
        # Write this updated object back to the object store
        self.save()
