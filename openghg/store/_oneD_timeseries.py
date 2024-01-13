from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np
from xarray import Dataset
from typing import TYPE_CHECKING, DefaultDict, Dict, Optional, Tuple, Union

if TYPE_CHECKING:
    from openghg.store import DataSchema

from openghg.store.base import BaseStore

__all__ = ["OneDTtimeseries"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class OneDTtimeseries(BaseStore):
    """This class is used to process ond dimension timeseries data"""

    _data_type = "1d_timeseries"

    # Identify if below section of code is relevant,
    # If so What changes do we want here?
    """ _root = "BoundaryConditions"
    _uuid = "4e787366-be91-4fc5-ad1b-4adcb213d478"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"""

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
        domain: str,
        period: Optional[Union[str, tuple]] = None,
        continuous: bool = True,
        overwrite: bool = False,
    ) -> dict:
        """Read one dimension timeseries file

        Args:
            filepath: Path of boundary conditions file
            species: Species name
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
        domain = clean_string(domain)

        filepath = Path(filepath)

        file_hash = hash_file(filepath=filepath)
        if file_hash in self._file_hashes and not overwrite:
            logger.warning(
                "This file has been uploaded previously with the filename : "
                f"{self._file_hashes[file_hash]} - skipping."
            )
            return {}

        oneD_data = open_dataset(filepath)

        # Some attributes are numpy types we can't serialise to JSON so convert them
        # to their native types here
        attrs = {}
        for key, value in oneD_data.attrs.items():
            try:
                attrs[key] = value.item()
            except AttributeError:
                attrs[key] = value

        author_name = "OpenGHG Cloud"
        oneD_data.attrs["author"] = author_name

        metadata = {}
        metadata.update(attrs)

        metadata["species"] = species
        metadata["domain"] = domain
        metadata["author"] = author_name
        metadata["processed"] = str(timestamp_now())

        # Check if time has 0-dimensions and, if so, expand this so time is 1D
        if "time" in oneD_data.coords:
            oneD_data = update_zero_dim(oneD_data, dim="time")

        # Currently ACRG boundary conditions are split by month or year
        oneD_time = oneD_data["time"]

        start_date, end_date, period_str = infer_date_range(
            oneD_time, filepath=filepath, period=period, continuous=continuous
        )

        # Checking against expected format for boundary conditions
        OneDTtimeseries.validate_data(oneD_data)
        data_type = "1d_timeseries"

        metadata["start_date"] = str(start_date)
        metadata["end_date"] = str(end_date)
        metadata["data_type"] = data_type

        metadata["input_filename"] = filepath.name

        metadata["time_period"] = period_str

        key = "_".join((species, domain))

        oneD_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
        oneD_data[key]["data"] = oneD_data
        oneD_data[key]["metadata"] = metadata

        required_keys = ("species", "domain")

        # This performs the lookup and assignment of data to new or
        # exisiting Datasources
        datasource_uuids = self.assign_data(
            data=oneD_data,
            overwrite=overwrite,
            data_type=data_type,
            required_keys=required_keys,
        )

        # Record the file hash in case we see this file again
        self._file_hashes[file_hash] = filepath.name

        return datasource_uuids

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
        data_schema = OneDTtimeseries.schema()
        data_schema.validate_data(data)

    @staticmethod
    def schema() -> DataSchema:
        """
        Define schema for one dimensional timeseries(OneDTtimeseries) Dataset.

        Includes observed_mole_fraction for each time of the defined domain:
            - "Yobs"
                - expected dimensions: ("time")

        Expected data types for all variables and coordinates also included.

        Returns:
            DataSchema : Contains schema for OneDTtimeseries.
        """
        from openghg.store import DataSchema

        data_vars: Dict[str, Tuple[str, ...]] = {
            "Yobs": ("time"),
        }
        dtypes = {
            "time": np.datetime64,
            "Yobs": np.floating,
        }

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format
