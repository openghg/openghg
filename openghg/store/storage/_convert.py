# This is a simple way to convert an old style NetCDF based object store
# to a new Zarr based object store

# First we want to get the path to the old style object store
import logging
import inspect
from pathlib import Path
import re
from typing import Dict, List, Union
import tinydb
import xarray as xr

# import openghg.standardise
from openghg.store.base import Datasource
from openghg.standardise import standardise
from openghg.objectstore.metastore import open_metastore
from openghg.objectstore.metastore._classic_metastore import BucketKeyStorage
from openghg.objectstore.metastore._metastore import TinyDBMetaStore
from openghg.store import storage_class_info
from openghg.types import ObjectStoreError, DataOverlapError

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)


def convert_store(path_in: Union[str, Path], store_out: str) -> None:
    """Convert object store to new style Zarr based object store.

    Args:
        path_in: Path to current old style object store
        store_out: Name of new object store. The store must exist in your OpenGHG config file.
    """
    # First we need to read the data in the object store
    # Iterating over each data type and then each data file
    # to convert to Zarr
    old_store_path = Path(path_in)

    # Let's now check if it's a valid object store
    if not old_store_path.exists():
        raise ObjectStoreError(f"Path {old_store_path} does not exist")

    dirs = [x.name for x in old_store_path.iterdir()]
    if "datasource" not in dirs:
        raise ObjectStoreError("Cannot find datasource folder, please check this is a valid object store")

    # Let's first get each of the storage classes
    storage_classes = storage_class_info()
    # These are the functions we can use to standardise the data
    standardise_args = _get_standardise_args()

    for data_type in storage_classes:
        logger.info(f"Converting {data_type} data...")
        # Let's load the metadata store
        # As we renamed Emissions -> Flux in 0.8.0 we need to check for this
        if data_type.lower() == "flux":
            key = "Emissions/uuid/c5c88168-0498-40ac-9ad3-949e91a30872/metastore"
            with tinydb.TinyDB(old_store_path, key, "r", storage=BucketKeyStorage) as db:
                records = db.all()
        else:
            with open_metastore(bucket=str(old_store_path), data_type=data_type, mode="r") as db:
                records = db._db.all()

        if not records:
            logger.info(f"No metadata records found for {data_type}, skipping...")
            continue
        
        valid_args = standardise_args[data_type]
        # Now let's iterate over the records
        for record in records:
            # Open the dataset for this record
            # First get the paths
            uuid = record["uuid"]            
            data_folderpath = Path(old_store_path).joinpath("data", uuid)
        
            if "latest_version" in record:
                version = record["latest_version"]
            else:
                try:
                    version = max([v.name for v in data_folderpath.iterdir()], key=lambda x: int(x[1:]))
                except (ValueError, AttributeError):
                    # .iterdir gives empty sequence or some version name doesn't match r"v\d+" (e.g. "v10", etc.)
                    raise ValueError("No data versions found for this search result.")   

            # Now get the files to restandardise
            version_folderpath = data_folderpath.joinpath(version)
            data_filepaths = sorted(list(version_folderpath.glob("*._data")))

            # Now we handle the metadata for the args we want to pass into the standardise functions
            standardise_kwargs = {k: v for k, v in record.items() if k in valid_args}
            # Parse some values to the format required for standardising
            tf_none = {"true": True, "false": False, "none": None}
            for k, v in standardise_kwargs.items():
                if v.lower() in tf_none:
                    standardise_kwargs[k] = tf_none[v.lower()]
                if k == "sampling_period":
                    if m := re.search(r"(\d+\.?\d*)", v):
                        standardise_kwargs[k] = str(int(float(m.group(1))) // 60) + "m"
                    else:
                        standardise_kwargs[k] = None

            chunks = {}
            if data_type == "surface":
                try:
                    standardise(
                        data_type=data_type,
                        store=store_out,
                        filepath=data_filepaths,
                        source_format="openghg",
                        chunks=chunks,
                        **standardise_kwargs,
                    )
                except DataOverlapError:
                    logger.warning(f"Data overlap error for record {uuid}")

            elif data_type == "footprints":
                if standardise_kwargs["high_time_resolution"]:
                    chunks = {"time": 24}
                else:
                    chunks = {"time": 480}

                try:
                    standardise(
                        data_type=data_type,
                        store=store_out,
                        filepath=data_filepaths,
                        chunks=chunks,
                        **standardise_kwargs,
                    )
                except DataOverlapError:
                    logger.warning(f"Data overlap error for record {uuid}")
            else:
                for data_file in data_filepaths:
                    try:
                        standardise(
                            data_type=data_type,
                            store=store_out,
                            filepath=data_file,
                            chunks=chunks,
                            **standardise_kwargs,
                        )
                    except DataOverlapError:
                        logger.warning(f"Data overlap error for record {uuid}")

        logger.info(f"Finished converting {data_type} data.")



        # TODO - remember to copy the file hashes and data storage class members over!

def convert():
    # could we just iterate over the data for each Datasource, read the data in and then save it to a Datasource
    # in the new object store?
    # That would get around needing to call the standardise functions


def _get_standardise_args() -> Dict[str, List[str]]:
    """Get dictionary mapping OpenGHG storage classes to a list of the arguments for the standardise
    function of that data class.

    Returns:
        dict:
    """
    standardise_members = inspect.getmembers(openghg.standardise)

    # Get function objects for standardise_* functions
    func_dict = {mem[0]: mem[1] for mem in standardise_members if mem[0].startswith("standardise_")}

    # Make dict with args
    arg_dict = {
        k.split("_")[1]: list(inspect.signature(v).parameters.keys())
        for k, v in func_dict.items()
        if "binary" not in k
    }

    # rename some types
    arg_dict["boundary_conditions"] = arg_dict["bc"]
    del arg_dict["bc"]

    arg_dict["footprints"] = arg_dict["footprint"]
    del arg_dict["footprint"]

    return arg_dict
