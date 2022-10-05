""" Helper functions to provide datapaths etc used in the tutorial notebooks

"""
import contextlib
import json
import os
import shutil
import tarfile
import tempfile
import warnings
from pathlib import Path
from typing import List, Union

__all__ = ["bilsdale_datapaths"]


# def _suppress_output(func):
#     def wrapper(*a, **ka):
#         with warnings.catch_warnings():
#             warnings.simplefilter("ignore")
#             with open(os.devnull, "w") as devnull:
#                 with contextlib.redirect_stdout(devnull):
#                     return func(*a, **ka)

#     return wrapper


def populate_surface_data() -> None:
    """Populates the example object store with data from the
    example data repository.

    Returns:
        None
    """
    from openghg.standardise import standardise_surface

    use_tutorial_store()

    bsd_data = "https://github.com/openghg/example_data/raw/main/timeseries/bsd_example.tar.gz"
    tac_data = "https://github.com/openghg/example_data/raw/main/timeseries/tac_example.tar.gz"
    capegrim_data = "https://github.com/openghg/example_data/raw/main/timeseries/capegrim_example.tar.gz"

    print("Retrieving example data...\n")
    bsd_paths = retrieve_example_data(url=bsd_data)
    tac_paths = retrieve_example_data(url=tac_data)
    capegrim_paths = sorted(retrieve_example_data(url=capegrim_data))

    # Create the tuple required
    capegrim_tuple = (capegrim_paths[0], capegrim_paths[1])

    print("Standardising data...\n")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with open(os.devnull, "w") as devnull:
            with contextlib.redirect_stdout(devnull):
                standardise_surface(filepaths=bsd_paths, source_format="crds", site="bsd", network="decc")
                standardise_surface(filepaths=tac_paths, source_format="crds", site="tac", network="decc")
                standardise_surface(
                    filepaths=capegrim_tuple,
                    instrument="medusa",
                    source_format="gcwerks",
                    site="cgo",
                    network="agage",
                )

    print("Finished.\n")


def bilsdale_datapaths() -> List:
    """Return a list of paths to the Tacolneston data for use in the ranking
    tutorial

    Returns:
        list: List of paths
    """
    crds_path = Path(__file__).resolve().parent.parent.parent.joinpath("tests/data/proc_test_data/CRDS")

    return list(crds_path.glob("bsd.picarro.1minute.*.min.*"))


def tutorial_store_path() -> Path:
    """Returns the path to the tutorial object store
    at Path(tempfile.gettempdir(), "openghg_temp_store")

    Returns:
        Path: Path to tutorial store
    """
    return Path(tempfile.gettempdir(), "openghg_temp_store")


def use_tutorial_store() -> None:
    """Sets an environment variable telling OpenGHG to use a
    temporary object store. This sets the store to be
    the result of tempfile.gettempdir() / openghg_temp_store.
    To tidy up this store use the clean_tutorial_store function.

    Returns:
        None
    """
    os.environ["OPENGHG_TMP_STORE"] = "1"


def clear_tutorial_store() -> None:
    """Cleans up the tutorial store

    Returns:
        None
    """
    temp_path = tutorial_store_path()

    if temp_path.exists():
        shutil.rmtree(temp_path, ignore_errors=True)


def example_extract_path() -> Path:
    """Return the path to folder containing the extracted example files

    Returns:
        None
    """
    return Path(tutorial_store_path(), "extracted_files")


def clear_example_cache() -> None:
    """Removes the file cache created when running the tutorials.

    Returns:
        None
    """
    from openghg.objectstore import get_local_objectstore_path

    example_cache_path = get_local_objectstore_path() / "example_cache"
    extracted_examples = example_extract_path()

    if example_cache_path.exists():
        shutil.rmtree(example_cache_path, ignore_errors=True)
        shutil.rmtree(extracted_examples, ignore_errors=True)


def unpack_archive(archive_path: Path, extract_dir: Union[str, Path, None] = None) -> List[Path]:
    """Unpacks an tar file to a temporary folder, or extract_dir if given.
    Returns the filepath(s) of the objects.

    Returns:
        list: List of filepaths
    """
    if extract_dir is None:
        extract_dir = example_extract_path()

    with tarfile.open(archive_path) as tar:
        filenames = [f.name for f in tar.getmembers()]

    shutil.unpack_archive(filename=archive_path, extract_dir=extract_dir)

    extracted_filepaths = [Path(extract_dir, str(fname)) for fname in filenames]

    return extracted_filepaths


def retrieve_example_data(url: str, extract_dir: Union[str, Path, None] = None) -> List[Path]:
    """Retrieve data from the OpenGHG example data repository, cache the downloaded data,
    extract the data and return the filepaths of the extracted files.

    Args:
        url: URL to retrieve.
        extract_dir: Folder to extract example tarballs to
    Returns:
        list: List of filepaths
    """
    from openghg.objectstore import get_local_objectstore_path
    from openghg.util import download_data, parse_url_filename

    # Check we're getting a tar
    output_filename = parse_url_filename(url=url)

    suffixes = Path(output_filename).suffixes
    if ".tar" not in suffixes:
        raise ValueError("This function can only use tar files.")

    example_cache_path = get_local_objectstore_path() / "example_cache"

    if not example_cache_path.exists():
        example_cache_path.mkdir(parents=True)

    cache_record = example_cache_path / "cache_record.json"
    download_path = Path(example_cache_path).joinpath(output_filename)

    cache_exists = cache_record.exists()

    if cache_exists:
        cache_data = json.loads(cache_record.read_text())

        try:
            cached_datapath = Path(cache_data[output_filename])
        except KeyError:
            cache_data[output_filename] = str(download_path)
        else:
            return unpack_archive(archive_path=cached_datapath, extract_dir=extract_dir)
    else:
        cache_data = {}
        cache_data[output_filename] = str(download_path)

    cache_record.write_text(json.dumps(cache_data))

    download_data(url=url, filepath=download_path)

    return unpack_archive(archive_path=download_path, extract_dir=extract_dir)
