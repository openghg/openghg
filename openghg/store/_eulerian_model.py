from __future__ import annotations
from pathlib import Path
from typing import DefaultDict, Dict, Optional, Union
import logging
from openghg.store.base import BaseStore
from xarray import Dataset
from types import TracebackType

from openghg.store.base._base import add_attr_to_data_REFACTOR  # TODO refactor this...
from openghg.store._connection import get_object_store_connection, get_file_hash_tracker


logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

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
    _metakey = f"{_root}/uuid/{_uuid}/metastore"


    def read_file(
        self,
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
        from openghg.util import clean_string, hash_file, timestamp_now, timestamp_tzaware
        from pandas import Timestamp as pd_Timestamp
        from xarray import open_dataset

        model = clean_string(model)
        species = clean_string(species)
        start_date = clean_string(start_date)
        end_date = clean_string(end_date)
        setup = clean_string(setup)

        filepath = Path(filepath)

        datasource_uuids = {}
        file_tracker = get_file_hash_tracker("eulerian_model", self._bucket)
        file_hash = hash_file(filepath=filepath)
        if not overwrite:
            try:
                file_tracker.check_file_hash(file_hash)
            except ValueError as e:
                logger.warning((str(e) + " Skipping."))
                return datasource_uuids

        with get_object_store_connection("eulerian_model", self._bucket) as conn:

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
                    raise ValueError(f"Input data must contain one of '{coord_options}' co-ordinate")
                if name != coord:
                    logger.info(f"Renaming co-ordinate '{coord}' to '{name}'")
                    em_data = em_data.rename({coord: name})

            attrs = em_data.attrs

            # author_name = "OpenGHG Cloud"
            # em_data.attrs["author"] = author_name

            metadata = {}
            metadata.update(attrs)

            metadata["model"] = model
            metadata["species"] = species
            metadata["processed"] = str(timestamp_now())
            metadata["data_type"] = "eulerian_model"

            if start_date is None:
                if len(em_data["time"]) > 1:
                    start_date = str(timestamp_tzaware(em_data.time[0].values))
                else:
                    try:
                        start_date = attrs["simulation_start_date_and_time"]
                        print(start_date)
                    except KeyError:
                        raise Exception("Unable to derive start_date from data, please provide as an input.")
                    else:
                        start_date = timestamp_tzaware(start_date)
                        print(start_date)
                        start_date = str(start_date)

            print(start_date)

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

            for key, parsed_data in model_data.items():
                metadata_data_pair = (parsed_data["metadata"], parsed_data["data"])
                add_attr_to_data_REFACTOR(*metadata_data_pair)
                ds_uuid = conn.add(*metadata_data_pair)
                datasource_uuids[key] = ds_uuid

            # Record the file hash in case we see this file again
            file_tracker.save_file_hash(file_hash, filepath)  # TODO: do we want filepath.name? this caused an error...

        return datasource_uuids
