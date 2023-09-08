from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, DefaultDict, Dict, Optional, Tuple, Union
from types import TracebackType
import numpy as np
from xarray import Dataset

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

    def __enter__(self) -> BoundaryConditions:
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
        overwrite: bool = False,
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
            overwrite: Should this data overwrite currently stored data.
        Returns:
            dict: Dictionary of datasource UUIDs data assigned to
        """
        from collections import defaultdict

        from openghg.store import (
            infer_date_range,
            update_zero_dim,
        )
        from openghg.util import clean_string, hash_file, timestamp_now
        from xarray import open_dataset

        species = clean_string(species)
        bc_input = clean_string(bc_input)
        domain = clean_string(domain)

        filepath = Path(filepath)

        file_hash = hash_file(filepath=filepath)
        if file_hash in self._file_hashes and not overwrite:
            logger.warning(
                "This file has been uploaded previously with the filename : "
                f"{self._file_hashes[file_hash]} - skipping."
            )
            return {}

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

        required_keys = ("species", "bc_input", "domain")

        # This performs the lookup and assignment of data to new or
        # exisiting Datasources
        datasource_uuids = self.assign_data(
            data=boundary_conditions_data,
            overwrite=overwrite,
            data_type=data_type,
            required_keys=required_keys,
        )

        # Record the file hash in case we see this file again
        self._file_hashes[file_hash] = filepath.name

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
