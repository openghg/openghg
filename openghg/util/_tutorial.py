""" Helper functions to provide datapaths etc used in the tutorial notebooks

"""
import contextlib
import json
import os
import shutil
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
def populate_footprint_data() -> None:
    """Populates the tutorial object store with footprints data from the
    example data repository.

    Returns:
        None
    """
    from openghg.standardise import standardise_footprint

    use_tutorial_store()

    tac_fp_co2 = "https://github.com/openghg/example_data/raw/main/footprint/tac_footprint_co2_201707.tar.gz"
    tac_fp_inert = (
        "https://github.com/openghg/example_data/raw/main/footprint/tac_footprint_inert_201607.tar.gz"
    )

    print("Retrieving example data...")
    tac_co2_path = retrieve_example_data(url=tac_fp_co2)[0]
    tac_inert_path = retrieve_example_data(url=tac_fp_inert)[0]

    print("Standardising footprint data...")
    # TODO - GJ - 2022-10-05 - This feels messy, how can we do this in a neater way?
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with open(os.devnull, "w") as devnull:
            with contextlib.redirect_stdout(devnull):
                site = "TAC"
                domain = "EUROPE"
                species = "co2"
                height = "185m"
                model = "NAME"
                metmodel = "UKV"

                standardise_footprint(
                    filepath=tac_co2_path,
                    site=site,
                    height=height,
                    domain=domain,
                    model=model,
                    metmodel=metmodel,
                    species=species,
                )

                site = "TAC"
                height = "100m"
                domain = "EUROPE"
                model = "NAME"

                standardise_footprint(
                    filepath=tac_inert_path, site=site, height=height, domain=domain, model=model
                )

    print("Done.")


def populate_flux_data() -> None:
    """Populates the tutorial object store with flux data from the
    example data repository.

    Returns:
        None
    """
    from openghg.standardise import standardise_flux

    use_tutorial_store()

    print("Retrieving data...")
    eur_2012_flux = "https://github.com/openghg/example_data/raw/main/flux/ch4-ukghg-all_EUROPE_2012.tar.gz"
    flux_data = retrieve_example_data(url=eur_2012_flux)

    co2_flux_eur = "https://github.com/openghg/example_data/raw/main/flux/co2-flux_EUROPE_2017.tar.gz"
    co2_flux_paths = retrieve_example_data(url=co2_flux_eur)

    domain = "EUROPE"
    date = "2012"
    species = "ch4"

    flux_data_waste = [filename for filename in flux_data if "waste" in str(filename)][0]
    flux_data_energyprod = [filename for filename in flux_data if "energyprod" in str(filename)][0]

    print("Standardising flux...")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with open(os.devnull, "w") as devnull:
            with contextlib.redirect_stdout(devnull):
                standardise_flux(
                    filepath=flux_data_waste, species=species, source="waste", domain=domain, date=date
                )
                standardise_flux(
                    filepath=flux_data_energyprod,
                    species=species,
                    source="energyprod",
                    domain=domain,
                    date=date,
                )

                domain = "EUROPE"
                species = "co2"
                date = "2017"

                source_natural = "natural"
                source_fossil = "ff-edgar-bp"

                flux_file_natural = [
                    filename for filename in co2_flux_paths if source_natural in str(filename)
                ][0]
                flux_file_ff = [filename for filename in co2_flux_paths if source_fossil in str(filename)][0]

                standardise_flux(
                    filepath=flux_file_natural,
                    species=species,
                    source=source_natural,
                    domain=domain,
                    date=date,
                    high_time_resolution=True,
                )
                standardise_flux(
                    filepath=flux_file_ff, species=species, source=source_fossil, domain=domain, date=date
                )

    print("Done.")


def populate_surface_data() -> None:
    """Populates the tutorial object store with surface measurement data from the
    example data repository.

    Returns:
        None
    """
    from openghg.standardise import standardise_surface

    use_tutorial_store()

    bsd_data = "https://github.com/openghg/example_data/raw/main/timeseries/bsd_example.tar.gz"
    tac_data = "https://github.com/openghg/example_data/raw/main/timeseries/tac_example.tar.gz"
    capegrim_data = "https://github.com/openghg/example_data/raw/main/timeseries/capegrim_example.tar.gz"

    print("Retrieving example data...")
    bsd_paths = retrieve_example_data(url=bsd_data)
    tac_paths = retrieve_example_data(url=tac_data)
    capegrim_paths = sorted(retrieve_example_data(url=capegrim_data))

    # Create the tuple required
    capegrim_tuple = (capegrim_paths[0], capegrim_paths[1])

    print("Standardising data...")

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

    print("Done.")


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
    from openghg.util import download_data, parse_url_filename, unpack_archive

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
