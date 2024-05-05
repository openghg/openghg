from __future__ import annotations
from pathlib import Path
from typing import Any, DefaultDict, Dict, Optional, Union
import logging
from openghg.store.base import BaseStore
from xarray import Dataset

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

    _data_type = "eulerian_model"
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
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
        chunks: Optional[Dict] = None,
        optional_metadata: Optional[Dict] = None,
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
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - just include new data and ignore previous
                - "combine" - replace and insert new data into current timeseries
            save_current: Whether to save data in current form and create a new version.
                - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
                - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
                - "n" / "no" - Allow current data to updated / deleted
            overwrite: Deprecated. This will use options for if_exists="new".
            force: Force adding of data even if this is identical to data stored.
            compressor: A custom compressor to use. If None, this will default to
                `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
                See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
            filters: Filters to apply to the data on storage, this defaults to no filtering. See
                https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
            chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
                for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
                See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
                To disable chunking pass in an empty dictionary.
            optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
        """
        # TODO: As written, this currently includes some light assumptions that we're dealing with GEOSChem SpeciesConc format.
        # May need to split out into multiple modules (like with ObsSurface) or into separate retrieve functions as needed.

        from collections import defaultdict
        from openghg.util import (
            clean_string,
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

        if overwrite and if_exists == "auto":
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                "See documentation for details of these inputs and options."
            )
            if_exists = "new"

        # Making sure new version will be created by default if force keyword is included.
        if force and if_exists == "auto":
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

        filepath = Path(filepath)

        _, unseen_hashes = self.check_hashes(filepaths=filepath, force=force)

        if not unseen_hashes:
            return {}

        filepath = next(iter(unseen_hashes.values()))

        if chunks is None:
            chunks = {}

        with open_dataset(filepath).chunk(chunks) as em_data:
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

            lookup_keys = self.get_lookup_keys(optional_metadata)

            if optional_metadata is not None:
                for parsed_data in model_data.values():
                    parsed_data["metadata"].update(optional_metadata)

            data_type = "eulerian_model"
            datasource_uuids = self.assign_data(
                data=model_data,
                if_exists=if_exists,
                new_version=new_version,
                data_type=data_type,
                required_keys=lookup_keys,
                compressor=compressor,
                filters=filters,
            )

            # TODO: MAY NEED TO ADD BACK IN OR CAN DELETE
            # update_keys = ["start_date", "end_date", "latest_version"]
            # model_data = update_metadata(
            #     data_dict=model_data, uuid_dict=datasource_uuids, update_keys=update_keys
            # )

            # em_store.add_datasources(
            #     uuids=datasource_uuids, data=model_data, metastore=metastore, update_keys=update_keys
            # )

            # Record the file hash in case we see this file again
            self.store_hashes(unseen_hashes)

            return datasource_uuids
