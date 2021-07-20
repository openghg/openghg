from openghg.modules import BaseModule
from pathlib import Path
from typing import Any, DefaultDict, Dict, Optional, Union
from xarray import Dataset

__all__ = ["Emissions"]


class Emissions(BaseModule):
    """This class is used to process surface observation data"""

    _root = "Emissions"
    _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"

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

        obs_key = f"{Emissions._root}/uuid/{Emissions._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        species: str,
        source: str,
        domain: str,
        date: str,
        high_time_resolution: Optional[bool] = False,
        period: Optional[str] = None,
        overwrite: Optional[bool] = False,
    ):
        """Read emissions file

        Args:
            filepath: Path of emissions file
            species: Species name
            domain: Emissions domain
            source: Emissions source
            high_time_resolution: If this is a high resolution file
            period: Period of measurements, if not passed this is inferred from the time coords
            overwrite: Should this data overwrite currently stored data.
        """
        from collections import defaultdict
        from xarray import open_dataset
        from openghg.processing import assign_data
        from openghg.util import clean_string, hash_file, timestamp_tzaware, timestamp_now

        species = clean_string(species)
        source = clean_string(source)
        domain = clean_string(domain)
        date = clean_string(date)

        filepath = Path(filepath)

        em_store = Emissions.load()

        file_hash = hash_file(filepath=filepath)
        if file_hash in em_store._file_hashes and not overwrite:
            raise ValueError(f"This file has been uploaded previously with the filename : {em_store._file_hashes[file_hash]}.")

        em_data = open_dataset(filepath)

        # Some attributes are numpy types we can't serialise to JSON so convert them
        # to their native types here
        attrs = {}
        for key, value in em_data.attrs.items():
            try:
                attrs[key] = value.item()
            except AttributeError:
                attrs[key] = value

        author_name = "OpenGHG Cloud"
        em_data.attrs["author"] = author_name

        metadata = {}
        metadata.update(attrs)

        metadata["species"] = species
        metadata["domain"] = domain
        metadata["source"] = source
        metadata["date"] = date
        metadata["author"] = author_name
        metadata["processed"] = str(timestamp_now())

        metadata["start_date"] = str(timestamp_tzaware(em_data.time[0].values))
        metadata["end_date"] = str(timestamp_tzaware(em_data.time[-1].values))

        metadata["max_longitude"] = round(float(em_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(em_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(em_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(em_data["lat"].min()), 5)

        metadata["time_resolution"] = "high" if high_time_resolution else "standard"

        if period is not None:
            metadata["time_period"] = period

        key = "_".join((species, source, domain, date))

        emissions_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
        emissions_data[key]["data"] = em_data
        emissions_data[key]["metadata"] = metadata

        keyed_metadata = {key: metadata}

        lookup_results = em_store.datasource_lookup(metadata=keyed_metadata)

        data_type = "emissions"
        datasource_uuids = assign_data(
            data_dict=emissions_data, lookup_results=lookup_results, overwrite=overwrite, data_type=data_type
        )

        em_store.add_datasources(datasource_uuids=datasource_uuids, metadata=keyed_metadata)

        # Record the file hash in case we see this file again
        em_store._file_hashes[file_hash] = filepath.name

        em_store.save()

        return datasource_uuids

    def lookup_uuid(self, species: str, source: str, domain: str, date: str) -> Union[str, bool]:
        """Perform a lookup for the UUID of a Datasource

        Args:
            site: Site code
            domain: Domain
            model: Model name
            height: Height
        Returns:
            str or dict: UUID or False if no entry
        """
        uuid = self._datasource_table[species][source][domain][date]

        return uuid if uuid else False

    def set_uuid(self, species: str, source: str, domain: str, date: str, uuid: str) -> None:
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
        self._datasource_table[species][source][domain][date] = uuid

    def datasource_lookup(self, metadata: Dict) -> Dict[str, Union[str, bool]]:
        """Find the Datasource we should assign the data to

        Args:
            metadata: Dictionary of metadata
        Returns:
            dict: Dictionary of datasource information
        """
        # TODO - I'll leave this as a function for now as the way we read emissions may
        # change in the near future
        # GJ - 2021-04-20
        lookup_results = {}

        for key, data in metadata.items():
            species = data["species"]
            source = data["source"]
            domain = data["domain"]
            date = data["date"]

            lookup_results[key] = self.lookup_uuid(species=species, source=source, domain=domain, date=date)

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
            species = md["species"]
            source = md["source"]
            domain = md["domain"]
            date = md["date"]

            result = self.lookup_uuid(species=species, source=source, domain=domain, date=date)

            if result and result != uid:
                raise ValueError("Mismatch between assigned uuid and stored Datasource uuid.")
            else:
                self.set_uuid(species=species, source=source, domain=domain, date=date, uuid=uid)
                self._datasource_uuids[uid] = key
