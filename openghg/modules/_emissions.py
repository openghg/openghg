from openghg.modules import BaseModule
from pathlib import Path
from typing import Dict, Optional, Union

__all__ = ["Emissions"]


class Emissions(BaseModule):
    """This class is used to process surface observation data"""

    _root = "Emissions"
    _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"

    def __init__(self):
        from openghg.util import timestamp_now
        from collections import defaultdict

        self._creation_datetime = timestamp_now()
        self._stored = False
        # Keyed by name - allows retrieval of UUID from name
        self._datasource_names = {}
        # Keyed by UUID - allows retrieval of name by UUID
        self._datasource_uuids = {}
        # Hashes of previously uploaded files
        self._file_hashes = {}
        # Keyed by UUID
        self._rank_data = defaultdict(dict)

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
        data["datasource_uuids"] = self._datasource_uuids
        data["datasource_names"] = self._datasource_names
        data["file_hashes"] = self._file_hashes
        data["rank_data"] = self._rank_data

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

        obs_key = f"{Emissions._root}/uuid/{Emissions._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        species: str,
        domain: str,
        source: str,
        high_time_resolution: bool,
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
        from xarray import open_dataset, infer_freq
        from openghg.processing import assign_emissions_data
        from openghg.util import hash_file, timestamp_tzaware, timestamp_now

        em_store = Emissions.load()

        file_hash = hash_file(filepath=filepath)
        if file_hash in em_store._file_hashes and not overwrite:
            raise ValueError(f"This file has been uploaded previously with the filename : {em_store._file_hashes[file_hash]}.")

        filepath = Path(filepath)
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
        metadata["author"] = author_name
        metadata["processed"] = str(timestamp_now())

        metadata["start_date"] = str(timestamp_tzaware(em_data.time[0].values))
        metadata["end_date"] = str(timestamp_tzaware(em_data.time[-1].values))

        metadata["max_longitude"] = round(float(em_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(em_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(em_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(em_data["lat"].min()), 5)

        metadata["time_resolution"] = "high_resolution" if high_time_resolution else "standard_resolution"

        if period is not None:
            metadata["time_period"] = period
        else:
            metadata["time_period"] = infer_freq(em_data.time)

        # Check if we've seen data from this site before
        em_hash = em_store._get_emissions_hash(species=species, domain=domain)

        if em_hash in em_store._datasource_uuids:
            datasource_uid = em_store._datasource_uuids[em_hash]
        else:
            datasource_uid = False

        uid = assign_emissions_data(data=em_data, metadata=metadata, datasource_uid=datasource_uid)

        em_store.add_datasources(datasource_uuids={em_hash: uid})

        # Record the file hash in case we see this file again
        em_store._file_hashes[file_hash] = filepath.name

        em_store.save()

        return {str(filepath.name): uid}

    def _get_emissions_hash(self, species, domain, **kwargs):
        from openghg.util import hash_string
        import re

        terms = [species, domain]
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
