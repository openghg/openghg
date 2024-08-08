from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, TYPE_CHECKING, DefaultDict, Dict, Optional, Tuple, Union
import numpy as np
from xarray import Dataset
from openghg.util import synonyms

if TYPE_CHECKING:
    from openghg.store import DataSchema

from openghg.store.base import BaseStore

__all__ = ["BoundaryConditions"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class BoundaryConditions(BaseStore):
    """This class is used to process boundary condition data"""

    _data_type = "boundary_conditions"
    _root = "BoundaryConditions"
    _uuid = "4e787366-be91-4fc5-ad1b-4adcb213d478"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def read_data(self, binary_data: bytes, metadata: Dict, file_metadata: Dict) -> Optional[Dict]:
        """Ready a footprint from binary data

        Args:
            binary_data: Footprint data
            metadata: Dictionary of metadata
            file_metadat: File metadata
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            try:
                filename = file_metadata["filename"]
            except KeyError:
                raise KeyError("We require a filename key for metadata read.")

            filepath = tmpdir_path.joinpath(filename)
            filepath.write_bytes(binary_data)

            return self.read_file(filepath=filepath, **metadata)

    def read_file(
        self,
        filepath: Union[str, Path],
        species: str,
        bc_input: str,
        domain: str,
        period: Optional[Union[str, tuple]] = None,
        continuous: bool = True,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
        chunks: Optional[Dict] = None,
        optional_metadata: Optional[Dict] = None,
    ) -> dict:
        """Read boundary conditions file

        Args:
            filepath: Path of boundary conditions file
            species: Species name
            bc_input: Input used to create boundary conditions. For example:
              - a model name such as "MOZART" or "CAMS"
              - a description such as "UniformAGAGE" (uniform values based on AGAGE average)
            domain: Region for boundary conditions
            period: Period of measurements. Only needed if this can not be inferred from the time coords
                    If specified, should be one of:
                     - "yearly", "monthly"
                     - suitable pandas Offset Alias
                     - tuple of (value, unit) as would be passed to pandas.Timedelta function
            continuous: Whether time stamps have to be continuous.
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
        Returns:
            dict: Dictionary of datasource UUIDs data assigned to
        """
        from collections import defaultdict

        from openghg.store import (
            infer_date_range,
            update_zero_dim,
        )
        from openghg.util import (
            clean_string,
            timestamp_now,
            check_if_need_new_version,
        )

        from xarray import open_dataset

        species = clean_string(species)
        species = synonyms(species)
        bc_input = clean_string(bc_input)
        domain = clean_string(domain)

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

        with open_dataset(filepath).chunk(chunks) as bc_data:
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
            metadata["bc_input"] = bc_input
            metadata["author"] = author_name
            metadata["processed"] = str(timestamp_now())

            # Check if time has 0-dimensions and, if so, expand this so time is 1D
            if "time" in bc_data.coords:
                bc_data = update_zero_dim(bc_data, dim="time")

            # Currently ACRG boundary conditions are split by month or year
            bc_time = bc_data["time"]

            start_date, end_date, period_str = infer_date_range(
                bc_time, filepath=filepath, period=period, continuous=continuous
            )

            # Checking against expected format for boundary conditions
            BoundaryConditions.validate_data(bc_data)
            data_type = "boundary_conditions"

            metadata["start_date"] = str(start_date)
            metadata["end_date"] = str(end_date)
            metadata["data_type"] = data_type

            metadata["max_longitude"] = round(float(bc_data["lon"].max()), 5)
            metadata["min_longitude"] = round(float(bc_data["lon"].min()), 5)
            metadata["max_latitude"] = round(float(bc_data["lat"].max()), 5)
            metadata["min_latitude"] = round(float(bc_data["lat"].min()), 5)
            metadata["min_height"] = round(float(bc_data["height"].min()), 5)
            metadata["max_height"] = round(float(bc_data["height"].max()), 5)

            metadata["input_filename"] = filepath.name

            metadata["time_period"] = period_str

            key = "_".join((species, bc_input, domain))

            boundary_conditions_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
            boundary_conditions_data[key]["data"] = bc_data
            boundary_conditions_data[key]["metadata"] = metadata

            lookup_keys = self.get_lookup_keys(optional_metadata)

            if optional_metadata is not None:
                for parsed_data in boundary_conditions_data.values():
                    parsed_data["metadata"].update(optional_metadata)

            # This performs the lookup and assignment of data to new or
            # existing Datasources
            datasource_uuids = self.assign_data(
                data=boundary_conditions_data,
                if_exists=if_exists,
                new_version=new_version,
                data_type=data_type,
                required_keys=lookup_keys,
                compressor=compressor,
                filters=filters,
            )

            # TODO: MAY NEED TO ADD BACK IN OR CAN DELETE
            # update_keys = ["start_date", "end_date", "latest_version"]
            # boundary_conditions_data = update_metadata(
            #     data_dict=boundary_conditions_data, uuid_dict=datasource_uuids, update_keys=update_keys
            # )

            # bc_store.add_datasources(
            #     uuids=datasource_uuids,
            #     data=boundary_conditions_data,
            #     metastore=metastore,
            #     update_keys=update_keys,
            # )

            # Record the file hash in case we see this file again
            self.store_hashes(unseen_hashes)

            return datasource_uuids

    @staticmethod
    def schema() -> DataSchema:
        """
        Define schema for boundary conditions Dataset.

        Includes volume mole fractions for each time and ordinal, vertical boundary at the edge of the defined domain:
            - "vmr_n", "vmr_s"
                - expected dimensions: ("time", "height", "lon")
            - "vmr_e", "vmr_w"
                - expected dimensions: ("time", "height", "lat")

        Expected data types for all variables and coordinates also included.

        Returns:
            DataSchema : Contains schema for BoundaryConditions.
        """
        from openghg.store import DataSchema

        data_vars: Dict[str, Tuple[str, ...]] = {
            "vmr_n": ("time", "height", "lon"),
            "vmr_e": ("time", "height", "lat"),
            "vmr_s": ("time", "height", "lon"),
            "vmr_w": ("time", "height", "lat"),
        }
        dtypes = {
            "lat": np.floating,
            "lon": np.floating,
            "height": np.floating,
            "time": np.datetime64,
            "vmr_n": np.floating,
            "vmr_e": np.floating,
            "vmr_s": np.floating,
            "vmr_w": np.floating,
        }

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format

    @staticmethod
    def validate_data(data: Dataset) -> None:
        """
        Validate input data against BoundaryConditions schema - definition from
        BoundaryConditions.schema() method.

        Args:
            data : xarray Dataset in expected format

        Returns:
            None

            Raises a ValueError with details if the input data does not adhere
            to the BoundaryConditions schema.
        """
        data_schema = BoundaryConditions.schema()
        data_schema.validate_data(data)
