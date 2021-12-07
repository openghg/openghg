from openghg.store.base import BaseStore
from pathlib import Path
from typing import DefaultDict, Dict, Optional, Union
from xarray import Dataset

__all__ = ["EulerianModel"]


# TODO: Currently built around these keys but will probably need more unique distiguishers for different setups
# model name
# species
# date (start_date)
# ...
# setup (included as option for now)


class EulerianModel(BaseStore):
    """This class is used to process Eulerian model data"""

    _root = "EulerianModel"
    _uuid = "63ff2365-3ba2-452a-a53d-110140805d06"

    def save(self) -> None:
        """Save the object to the object store

        Args:
            bucket: Bucket for data
        Returns:
            None
        """
        from openghg.objectstore import get_bucket, set_object_from_json

        bucket = get_bucket()

        obs_key = f"{EulerianModel._root}/uuid/{EulerianModel._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        model: str,
        species: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        setup: Optional[str] = None,
        overwrite: bool = False,
    ) -> Dict:
        """Read Eulerian model output

        Args:
            filepath: Path of Eulerian model species output
            model: Eulerian model name
            species: Species name
            start_date: Start date (inclusive) associated with model run
            end_date: End date (exclusive) associated with model run
            setup: Additional setup details for run
            overwrite: Should this data overwrite currently stored data.
        """
        # TODO: As written, this currently includes some light assumptions that we're dealing with GEOSChem SpeciesConc format.
        # May need to split out into multiple modules (like with ObsSurface) or into separate retrieve functions as needed.

        from collections import defaultdict
        from openghg.util import (
            clean_string,
            hash_file,
            timestamp_now,
            timestamp_tzaware,
        )
        from openghg.store import assign_data
        from xarray import open_dataset
        from pandas import Timestamp as pd_Timestamp

        model = clean_string(model)
        species = clean_string(species)
        start_date = clean_string(start_date)
        end_date = clean_string(end_date)
        setup = clean_string(setup)

        filepath = Path(filepath)

        em_store = EulerianModel.load()

        file_hash = hash_file(filepath=filepath)
        if file_hash in em_store._file_hashes and not overwrite:
            raise ValueError(
                f"This file has been uploaded previously with the filename : {em_store._file_hashes[file_hash]}."
            )

        em_data = open_dataset(filepath)

        # Check necessary 4D coordinates are present and rename if necessary (for consistency)
        check_coords = {
            "time": ["time"],
            "lat": ["lat", "latitude"],
            "lon": ["lon", "longitude"],
            "lev": ["lev", "level", "layer", "sigma_level"],
        }
        for name, coord_options in check_coords.items():
            for coord in coord_options:
                if coord in em_data.coords:
                    break
            else:
                raise ValueError("Input data must contain one of '{coord_options}' co-ordinate")
            if name != coord:
                print("Renaming co-ordinate '{coord}' to '{name}'")
                em_data = em_data.rename({coord: name})

        attrs = em_data.attrs

        # author_name = "OpenGHG Cloud"
        # em_data.attrs["author"] = author_name

        metadata = {}
        metadata.update(attrs)

        metadata["model"] = model
        metadata["species"] = species
        metadata["processed"] = str(timestamp_now())

        if start_date is None:
            if len(em_data["time"]) > 1:
                start_date = str(timestamp_tzaware(em_data.time[0].values))
            else:
                try:
                    start_date = attrs["simulation_start_date_and_time"]
                except KeyError:
                    raise Exception("Unable to derive start_date from data, please provide as an input.")
                else:
                    start_date = timestamp_tzaware(start_date)
                    start_date = str(start_date)

        if end_date is None:
            if len(em_data["time"]) > 1:
                end_date = str(timestamp_tzaware(em_data.time[-1].values))
            else:
                try:
                    end_date = attrs["simulation_end_date_and_time"]
                except KeyError:
                    raise Exception("Unable to derive `end_date` from data, please provide as an input.")
                else:
                    end_date = timestamp_tzaware(end_date)
                    end_date = str(end_date)

        date = str(pd_Timestamp(start_date).date())

        metadata["date"] = date
        metadata["start_date"] = start_date
        metadata["end_date"] = end_date

        metadata["max_longitude"] = round(float(em_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(em_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(em_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(em_data["lat"].min()), 5)

        history = metadata.get("history")
        if history is None:
            history = ""
        metadata["history"] = history + f" {str(timestamp_now())} Processed onto OpenGHG cloud"

        key = "_".join((model, species, date))

        model_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
        model_data[key]["data"] = em_data
        model_data[key]["metadata"] = metadata

        keyed_metadata = {key: metadata}

        lookup_results = em_store.datasource_lookup(metadata=keyed_metadata)

        data_type = "eulerian_model"
        datasource_uuids = assign_data(
            data_dict=model_data,
            lookup_results=lookup_results,
            overwrite=overwrite,
            data_type=data_type,
        )

        em_store.add_datasources(datasource_uuids=datasource_uuids, metadata=keyed_metadata)

        # Record the file hash in case we see this file again
        em_store._file_hashes[file_hash] = filepath.name

        em_store.save()

        return datasource_uuids

    def lookup_uuid(self, model: str, species: str, date: str) -> Union[str, bool]:
        """Perform a lookup for the UUID of a Datasource

        Args:
            model: Eulerian model name
            species: Species name
            date: Start date associated with model run
        Returns:
            str or bool: UUID or False if no entry
        """
        uuid = self._datasource_table[model][species][date]

        return uuid if uuid else False

    def set_uuid(self, model: str, species: str, date: str, uuid: str) -> None:
        """Record a UUID of a Datasource in the datasource table

        Args:
            model: Eulerian model name
            species: Species name
            date: Start date associated with model run
            uuid: UUID of Datasource
        Returns:
            None
        """
        self._datasource_table[model][species][date] = uuid

    def datasource_lookup(self, metadata: Dict) -> Dict[str, Union[str, bool]]:
        """Find the Datasource we should assign the data to

        Args:
            metadata: Dictionary of metadata
        Returns:
            dict: Dictionary of datasource information
        """
        # TODO - I'll leave this as a function for now as the way we read footprints may
        # change in the near future
        # GJ - 2021-04-20 (added by RT on 2021-06-11)
        lookup_results = {}

        for key, data in metadata.items():
            model = data["model"]
            species = data["species"]
            date = data["date"]

            lookup_results[key] = self.lookup_uuid(model=model, species=species, date=date)

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
            model = md["model"]
            species = md["species"]
            date = md["date"]

            result = self.lookup_uuid(model=model, species=species, date=date)

            if result and result != uid:
                raise ValueError("Mismatch between assigned uuid and stored Datasource uuid.")
            else:
                self.set_uuid(model=model, species=species, date=date, uuid=uid)
                self._datasource_uuids[uid] = key
