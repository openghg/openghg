from __future__ import annotations
from pathlib import Path
from typing import DefaultDict, Dict, Optional, Union
import logging
from openghg.store.base import BaseStore
from xarray import Dataset
from types import TracebackType

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

    def __enter__(self) -> EulerianModel:
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_type is not None:
            logger.error(msg=f"{exc_type}, {exc_tb}")
        else:
            self.save()

    def read_file(
        self,
        filepath: Union[str, Path],
        model: str,
        species: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        setup: Optional[str] = None,
        if_exists: str = "default",
        save_current: Optional[bool] = None,
        overwrite: bool = False,
        force: bool = False,
    ) -> Dict:
        """Read Eulerian model output

        Args:
            filepath: Path of Eulerian model species output
            model: Eulerian model name
            species: Species name
            start_date: Start date (inclusive) associated with model run
            end_date: End date (exclusive) associated with model run
            setup: Additional setup details for run
            if_exists: What to do if existing data is present.
                - "default" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - just include new data and ignore previous
                - "replace" - replace and insert new data into current timeseries
            save_current: Whether to save data in current form and create a new version.
                If None, this will depend on if_exists input ("default" -> True), (other -> False)
            overwrite: Deprecated. This will use options for if_exists="new" and save_current=True.
            force: Force adding of data even if this is identical to data stored.
        """
        # TODO: As written, this currently includes some light assumptions that we're dealing with GEOSChem SpeciesConc format.
        # May need to split out into multiple modules (like with ObsSurface) or into separate retrieve functions as needed.

        from collections import defaultdict
        from openghg.util import (
            clean_string,
            hash_file,
            timestamp_now,
            timestamp_tzaware,
            check_if_need_new_version,
        )
        from pandas import Timestamp as pd_Timestamp
        from xarray import open_dataset

        model = clean_string(model)
        species = clean_string(species)
        start_date = clean_string(start_date)
        end_date = clean_string(end_date)
        setup = clean_string(setup)

        if overwrite and if_exists == "default":
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                "See documentation for details of these inputs and options."
            )
            if_exists = "new"

        # Making sure data can be force overwritten if force keyword is included.
        if force and if_exists == "default":
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

        filepath = Path(filepath)

        file_hash = hash_file(filepath=filepath)
        if file_hash in self._file_hashes and not force:
            raise ValueError(
                f"This file has been uploaded previously with the filename : {self._file_hashes[file_hash]}.\n"
                "If necessary, use force=True to bypass this to add this data."
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

        required = ("model", "species", "date")

        data_type = "eulerian_model"
        datasource_uuids = self.assign_data(
            data=model_data,
            if_exists=if_exists,
            new_version=new_version,
            data_type=data_type,
            required_keys=required
        )

        ## TODO: MAY NEED TO ADD BACK IN OR CAN DELETE
        # update_keys = ["start_date", "end_date", "latest_version"]
        # model_data = update_metadata(
        #     data_dict=model_data, uuid_dict=datasource_uuids, update_keys=update_keys
        # )

        # em_store.add_datasources(
        #     uuids=datasource_uuids, data=model_data, metastore=metastore, update_keys=update_keys
        # )

        # Record the file hash in case we see this file again
        self._file_hashes[file_hash] = filepath.name

        return datasource_uuids
