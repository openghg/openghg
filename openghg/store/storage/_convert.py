# This is a simple way to convert an old style NetCDF based object store
# to a new Zarr based object store

# First we want to get the path to the old style object store
import logging
import inspect
import json
from pathlib import Path
import re
import tinydb
import dask

import openghg.standardise
from openghg.standardise import standardise
from openghg.objectstore import get_writable_bucket
from openghg.objectstore.metastore import open_metastore
from openghg.objectstore.metastore._classic_metastore import BucketKeyStorage

from openghg.types import ObjectStoreError, DataOverlapError

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)


def convert_store(
    path_in: str | Path,
    store_out: str,
    chunks: dict | None = None,
    to_convert: list | None = None,
) -> None:
    """Convert object store to new style Zarr based object store.

    Args:
        path_in: Path to current old style object store
        store_out: Name of new object store. The store must exist in your OpenGHG config file.
        chunks: Chunking to use for the Zarr store, optional, pass only if you get warning about chunks being too big
        for the compressor
        to_convert: List of storage classes to convert, if None will convert all storage classes
    """
    from openghg.store import data_class_info, get_data_class

    # Tell dask to use the sync scheduler as I was getting Dask queue/thread hanging issues
    # when running this locally
    dask.config.set(scheduler="sync")

    logger.warning(
        "This function is provided as is and although it has been tested, may not duplicate the "
        + "object store perfectly."
        + "\nWe recommend repopulating object stores with the original data if possible."
    )

    # First we need to read the data in the object store
    # Iterating over each data type and then each data file
    # to convert to Zarr
    old_store_path = Path(path_in).expanduser().resolve().absolute()

    # Let's now check if it's a valid object store
    if not old_store_path.exists():
        raise ObjectStoreError(f"Path {old_store_path} does not exist")

    dirs = [x.name for x in old_store_path.iterdir()]
    if "datasource" not in dirs:
        raise ObjectStoreError("Cannot find datasource folder, please check this is a valid object store")

    # Let's first get each of the storage classes
    storage_classes_all = data_class_info()
    if to_convert is None:
        storage_classes = storage_classes_all
    else:
        storage_classes = {k: v for k, v in storage_classes_all.items() if k in to_convert}
        if not storage_classes:
            raise ValueError(
                f"No valid storage classes to convert, select from {list(storage_classes_all.keys())}"
            )

    if chunks is not None and len(storage_classes) > 1:
        raise ValueError(
            "Chunks should only be set for when a single storage class is chosen for conversion."
            + "\nYou should only set chunks if you get a warning about chunks being too big for the compressor."
        )

    logger.info(f"We'll convert data from the following classes {list(storage_classes.keys())}")

    # These are the functions we can use to standardise the data
    standardise_args = _get_standardise_args()

    for data_type in storage_classes:
        logger.info(f"Converting {data_type} data...")
        # Let's load the metadata store
        # As we renamed Emissions -> Flux in 0.8.0 we need to check for this
        if data_type.lower() == "flux":
            key = "Emissions/uuid/c5c88168-0498-40ac-9ad3-949e91a30872/metastore"
            with tinydb.TinyDB(str(old_store_path), key, "r", storage=BucketKeyStorage) as db:
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
            data_folderpath = Path(old_store_path).joinpath("data", "uuid", uuid)

            if "latest_version" in record:
                version = record["latest_version"]
            else:
                try:
                    version = max([v.name for v in data_folderpath.iterdir()], key=lambda x: int(x[1:]))
                except (ValueError, AttributeError):
                    # .iterdir gives empty sequence or some version name doesn't match r"v\d+" (e.g. "v10", etc.)
                    logger.warning(
                        f"Unable to find a data version for {data_type} data with UUID {uuid}, skipping..."
                    )
                    continue

            # Now get the files to restandardise
            version_folderpath = data_folderpath.joinpath(version)
            data_filepaths = sorted(list(version_folderpath.glob("*._data")))

            # Now we handle the metadata for the args we want to pass into the standardise functions
            standardise_kwargs = {k: v for k, v in record.items() if k in valid_args}

            # Parse some values to the format required for standardising
            tf_none = {"true": True, "false": False, "none": None}
            for k, v in standardise_kwargs.items():
                if isinstance(v, str) and v.lower() in tf_none:
                    standardise_kwargs[k] = tf_none[v.lower()]
                if k == "sampling_period":
                    if m := re.search(r"(\d+\.?\d*)", v):
                        val = str(int(float(m.group(1))) // 60) + "m"
                        standardise_kwargs[k] = val
                    else:
                        standardise_kwargs[k] = None

            # The number of Datasources converted
            n_converted = 0
            # An error will be raised if we get daterange overlaps, we'll just warn the user
            overlap_msg = f"Data overlap error for {data_type} data with UUID {uuid}"

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
                    logger.warning(overlap_msg)
                else:
                    n_converted += 1

            elif data_type == "footprints":
                if standardise_kwargs["time_resolved"]:
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
                    logger.warning(overlap_msg)
                else:
                    n_converted += 1
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
                        logger.warning(overlap_msg)
                    except ValueError as e:
                        logger.warning(f"Error standardising {data_type} data with UUID {uuid}: {e}")
                    else:
                        n_converted += 1

        # We only want to update the file hashes if we've converted some data
        if n_converted > 0:
            # TODO - remember to copy the file hashes and data storage class members over!
            # First we need to get the path of the object
            key_root = storage_classes[data_type]["_root"]
            key_uuid = storage_classes[data_type]["_uuid"]

            if data_type == "flux":
                key_root = "Emissions"

            dlcass_datapath = old_store_path.joinpath(key_root, "uuid", f"{key_uuid}._data")

            old_dclass_data = json.loads(dlcass_datapath.read_text())
            old_file_hashes = old_dclass_data["_file_hashes"]

            current_dclass = get_data_class(data_type=data_type)
            bucket = get_writable_bucket(name=store_out)

            with current_dclass(bucket=bucket) as dclass:
                dclass._file_hashes.update(old_file_hashes)

        logger.info(f"Finished converting {data_type} data.")


def _get_standardise_args() -> dict[str, list[str]]:
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
