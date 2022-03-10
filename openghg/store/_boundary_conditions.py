from openghg.store.base import BaseStore
from pathlib import Path
from typing import DefaultDict, Dict, Optional, Union
from xarray import Dataset

__all__ = ["BoundaryConditions"]


class BoundaryConditions(BaseStore):
    """This class is used to process boundary condition data"""

    _root = "BoundaryConditions"
    # _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"

    def save(self) -> None:
        """Save the object to the object store

        Returns:
            None
        """
        from openghg.objectstore import get_bucket, set_object_from_json

        bucket = get_bucket()
        obs_key = f"{BoundaryConditions._root}/uuid/{BoundaryConditions._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        species: str,
        bc_input: str,
        domain: str,
        date: str,
        period: Optional[str] = None,
        overwrite: bool = False,
    ) -> Dict:
        """Read boundary conditions file

        Args:
            filepath: Path of boundary conditions file
            species: Species name
            bc_input: Input used to create boundary conditions. For example:
              - a model name such as "MOZART" or "CAMS"
              - a description such as "UniformAGAGE" (uniform values based on AGAGE average)
            domain: Region for boundary conditions
            period: Period of measurements, if not passed this is inferred from the time coords
            overwrite: Should this data overwrite currently stored data.
        Returns:
            dict: Dictionary of datasource UUIDs data assigned to
        """
        from collections import defaultdict
        from xarray import open_dataset
        from openghg.store import assign_data
        from openghg.util import (
            clean_string,
            hash_file,
            pairwise,
            timestamp_tzaware,
            timestamp_now,
        )

        species = clean_string(species)
        bc_input = clean_string(bc_input)
        domain = clean_string(domain)
        date = clean_string(date)

        filepath = Path(filepath)

        bc_store = BoundaryConditions.load()

        file_hash = hash_file(filepath=filepath)
        if file_hash in bc_store._file_hashes and not overwrite:
            print(
                f"This file has been uploaded previously with the filename : {bc_store._file_hashes[file_hash]} - skipping."
            )

        bc_data = open_dataset(filepath)

        # Some attributes are numpy types we can't serialise to JSON so convert them
        # to their native types here
        attrs = {}
        for key, value in bc_data.attrs.items():
            try:
                attrs[key] = value.item()
            except AttributeError:
                attrs[key] = value

        author_name = "OpenGHG Cloud"
        bc_data.attrs["author"] = author_name

        metadata = {}
        metadata.update(attrs)

        metadata["species"] = species
        metadata["domain"] = domain
        metadata["boundary_condition_input"] = bc_input
        metadata["date"] = date
        metadata["author"] = author_name
        metadata["processed"] = str(timestamp_now())

        ### TODO: Currently ACRG boundary conditions are split by month rather than yearly
        # Need to add in code to handle this below.
        #  - Could do this based on filename? This is linked to stored data for 
        # current ACRG data.
        n_dates = bc_data.time.size

        # This covers the whole year
        if n_dates == 1:
            year = timestamp_tzaware(bc_data.time[0].values).year
            year_start = timestamp_tzaware(f"{year}-1-1-00:00:00")
            year_end = timestamp_tzaware(f"{year}-12-31-23:59:59")

            start_date = year_start
            end_date = year_end
            freq = "annual"
        # We have values for each month / week
        elif n_dates == 12:
            # Check they're successive months
            timestamps = [timestamp_tzaware(t) for t in bc_data.time.values]

            for a, b in pairwise(timestamps):
                if a.month != b.month - 1:
                    raise ValueError("We expect successive months for the data")

            if timestamps[0].year != timestamps[-1].year:
                raise ValueError("We expect a single year of data")

            year = timestamps[0].year

            year_start = timestamp_tzaware(f"{year}-1-1-00:00:00")
            year_end = timestamp_tzaware(f"{year}-12-31-23:59:59")

            start_date = year_start
            end_date = year_end
            freq = "month"
        # Work run through the timestamps and check for gap between them ?
        # Add something to metadata for this?
        else:
            timestamps = [timestamp_tzaware(t) for t in bc_data.time.values]
            timestamps.sort()

            frequency = set()

            for a, b in pairwise(timestamps):
                delta = b - a
                frequency.add(delta)

            start_date = timestamps[0]
            end_date = timestamps[-1]

            if len(frequency) == 1:
                freq = str(frequency.pop())
            else:
                freq = "varies"

        ###### Section above needs to be updated #######

        metadata["start_date"] = str(start_date)
        metadata["end_date"] = str(end_date)

        metadata["max_longitude"] = round(float(bc_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(bc_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(bc_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(bc_data["lat"].min()), 5)
        metadata["min_height"] = round(float(bc_data["height"].min()), 5)
        metadata["max_height"] = round(float(bc_data["height"].max()), 5)

        metadata["frequency"] = freq

        if period is not None:
            metadata["time_period"] = period

        key = "_".join((species, bc_input, domain, date))

        boundary_conditions_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
        boundary_conditions_data[key]["data"] = bc_data
        boundary_conditions_data[key]["metadata"] = metadata

        keyed_metadata = {key: metadata}

        lookup_results = bc_store.datasource_lookup(metadata=keyed_metadata)

        data_type = "boundary_conditions"
        datasource_uuids = assign_data(
            data_dict=boundary_conditions_data,
            lookup_results=lookup_results,
            overwrite=overwrite,
            data_type=data_type,
        )

        bc_store.add_datasources(datasource_uuids=datasource_uuids, metadata=keyed_metadata)

        # Record the file hash in case we see this file again
        bc_store._file_hashes[file_hash] = filepath.name

        bc_store.save()

        return datasource_uuids

    def lookup_uuid(self, species: str, bc_input: str, domain: str, date: str) -> Union[str, bool]:
        """Perform a lookup for the UUID of a Datasource

        Args:
            species: Site code
            bc_input: Input identifier for boundary conditions
            domain: Domain
            date: Date of original file
        Returns:
            str or dict: UUID or False if no entry
        """
        uuid = self._datasource_table[species][bc_input][domain][date]

        return uuid if uuid else False

    def set_uuid(self, species: str, bc_input: str, domain: str, date: str, uuid: str) -> None:
        """Record a UUID of a Datasource in the datasource table

        Args:
            species: Site code
            bc_input: Input identifier for boundary conditions
            domain: Domain
            date: Date of original file
            uuid: UUID of Datasource
        Returns:
            None
        """
        self._datasource_table[species][bc_input][domain][date] = uuid

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
            bc_input = data["boundary_condition_input"]
            domain = data["domain"]
            date = data["date"]

            lookup_results[key] = self.lookup_uuid(species=species, bc_input=bc_input, domain=domain, date=date)

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
            bc_input = md["boundary_condition_input"]
            domain = md["domain"]
            date = md["date"]

            result = self.lookup_uuid(species=species, bc_input=bc_input, domain=domain, date=date)

            if result and result != uid:
                raise ValueError("Mismatch between assigned uuid and stored Datasource uuid.")
            else:
                self.set_uuid(species=species, bc_input=bc_input, domain=domain, date=date, uuid=uid)
                self._datasource_uuids[uid] = key