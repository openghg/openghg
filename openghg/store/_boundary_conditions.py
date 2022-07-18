from pathlib import Path
from typing import DefaultDict, Dict, Optional, Union, Tuple
from xarray import Dataset
import numpy as np
from tempfile import TemporaryDirectory

from openghg.store import DataSchema
from openghg.store.base import BaseStore

__all__ = ["BoundaryConditions"]


class BoundaryConditions(BaseStore):
    """This class is used to process boundary condition data"""

    _root = "BoundaryConditions"
    _uuid = "4e787366-be91-4fc5-ad1b-4adcb213d478"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

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
    def read_data(binary_data: bytes, metadata: Dict, file_metadata: Dict) -> Dict:
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

            return BoundaryConditions.read_file(filepath=filepath, **metadata)

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        species: str,
        bc_input: str,
        domain: str,
        period: Optional[Union[str, tuple]] = None,
        continuous: bool = True,
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
        from xarray import open_dataset
        from openghg.store import assign_data, datasource_lookup, infer_date_range, load_metastore
        from openghg.util import (
            clean_string,
            hash_file,
            timestamp_now,
        )

        species = clean_string(species)
        bc_input = clean_string(bc_input)
        domain = clean_string(domain)

        filepath = Path(filepath)

        bc_store = BoundaryConditions.load()

        # Load in the metadata store
        metastore = load_metastore(key=bc_store._metakey)

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
        metadata["bc_input"] = bc_input
        metadata["author"] = author_name
        metadata["processed"] = str(timestamp_now())

        # Currently ACRG boundary conditions are split by month or year
        bc_time = bc_data.time

        start_date, end_date, period_str = infer_date_range(
            bc_time, filepath=filepath, period=period, continuous=continuous
        )

        if "year" in period_str:
            date = f"{start_date.year}"
        elif "month" in period_str:
            date = f"{start_date.year}{start_date.month:02}"
        else:
            date = start_date.astype("datetime64[s]").astype(str)

        # Checking against expected format for boundary conditions
        BoundaryConditions.validate_data(bc_data)

        metadata["start_date"] = str(start_date)
        metadata["end_date"] = str(end_date)

        metadata["max_longitude"] = round(float(bc_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(bc_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(bc_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(bc_data["lat"].min()), 5)
        metadata["min_height"] = round(float(bc_data["height"].min()), 5)
        metadata["max_height"] = round(float(bc_data["height"].max()), 5)

        metadata["input_filename"] = filepath.name

        metadata["time_period"] = period_str
        metadata["date"] = date

        key = "_".join((species, bc_input, domain, date))

        boundary_conditions_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
        boundary_conditions_data[key]["data"] = bc_data
        boundary_conditions_data[key]["metadata"] = metadata

        required_keys = ("species", "bc_input", "domain", "date")
        lookup_results = datasource_lookup(
            metastore=metastore, data=boundary_conditions_data, required_keys=required_keys
        )

        data_type = "boundary_conditions"
        datasource_uuids = assign_data(
            data_dict=boundary_conditions_data,
            lookup_results=lookup_results,
            overwrite=overwrite,
            data_type=data_type,
        )

        bc_store.add_datasources(uuids=datasource_uuids, data=boundary_conditions_data, metastore=metastore)

        # Record the file hash in case we see this file again
        bc_store._file_hashes[file_hash] = filepath.name

        bc_store.save()

        metastore.close()

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
