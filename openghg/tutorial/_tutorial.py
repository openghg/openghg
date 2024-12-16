""" Helper functions to provide datapaths etc used in the tutorial notebooks

"""

import contextlib
import os
import shutil
import tarfile
import warnings
from pathlib import Path
import logging
from openghg.standardise import standardise_footprint, standardise_flux, standardise_bc

logger = logging.getLogger("openghg.tutorial")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def populate_footprint_data() -> None:
    """Adds all footprint data to the tutorial object store

    Returns:
        None
    """
    populate_footprint_inert()
    populate_footprint_co2()


def populate_footprint_inert() -> None:
    """Populates the tutorial object store with inert footprint data

    Returns:
        None
    """
    use_tutorial_store()

    tac_fp_inert = (
        "https://github.com/openghg/example_data/raw/main/footprint/tac_footprint_inert_201607.tar.gz"
    )

    tac_inert_path = retrieve_example_data(url=tac_fp_inert)[0]

    logger.info("Standardising footprint data...")
    # TODO - GJ - 2022-10-05 - This feels messy, how can we do this in a neater way?
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with open(os.devnull, "w") as devnull:
            with contextlib.redirect_stdout(devnull):
                site = "TAC"
                # height = "100m"
                inlet = "100m"
                domain = "EUROPE"
                model = "NAME"

                standardise_footprint(
                    filepath=tac_inert_path, site=site, inlet=inlet, domain=domain, model=model
                )


def populate_footprint_co2() -> None:
    """Populates the tutorial object store with footprints data from the
    example data repository.

    Returns:
        None
    """
    tac_fp_co2 = "https://github.com/openghg/example_data/raw/main/footprint/tac_footprint_co2_201707.tar.gz"

    logger.info("Retrieving example data...")
    tac_co2_path = retrieve_example_data(url=tac_fp_co2)[0]

    logger.info("Standardising footprint data...")
    # TODO - GJ - 2022-10-05 - This feels messy, how can we do this in a neater way?
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with open(os.devnull, "w") as devnull:
            with contextlib.redirect_stdout(devnull):
                site = "TAC"
                domain = "EUROPE"
                species = "co2"
                # height = "185m"
                inlet = "185m"
                model = "NAME"
                met_model = "UKV"

                standardise_footprint(
                    filepath=tac_co2_path,
                    site=site,
                    inlet=inlet,
                    domain=domain,
                    model=model,
                    met_model=met_model,
                    species=species,
                )

    logger.info("Done.")


def populate_flux_data() -> None:
    """Populate the tutorial store with flux data

    Returns:
        None
    """
    populate_flux_ch4()
    populate_flux_co2()


def populate_flux_co2() -> None:
    """Populate the tutorial object store with CO2 flux data

    Returns:
        None
    """
    co2_flux_eur = "https://github.com/openghg/example_data/raw/main/flux/co2-flux_EUROPE_2017.tar.gz"
    co2_flux_paths = retrieve_example_data(url=co2_flux_eur)

    source_natural = "natural"
    source_fossil = "ff-edgar-bp"

    flux_file_natural = [filename for filename in co2_flux_paths if source_natural in str(filename)][0]
    flux_file_ff = [filename for filename in co2_flux_paths if source_fossil in str(filename)][0]

    domain = "EUROPE"
    species = "co2"

    source_natural = "natural"
    source_fossil = "ff-edgar-bp"

    standardise_flux(
        filepath=flux_file_natural,
        species=species,
        source=source_natural,
        domain=domain,
        time_resolved=True,
    )
    standardise_flux(filepath=flux_file_ff, species=species, source=source_fossil, domain=domain)


def populate_flux_ch4() -> None:
    """Populates the tutorial object store with flux data from the
    example data repository.

    Returns:
        None
    """
    use_tutorial_store()

    logger.info("Retrieving data...")
    eur_2016_flux = "https://github.com/openghg/example_data/raw/main/flux/ch4-ukghg-all_EUROPE_2016.tar.gz"
    flux_data = retrieve_example_data(url=eur_2016_flux)

    source_waste = "waste"
    source_energyprod = "energyprod"

    flux_data_waste = [filename for filename in flux_data if source_waste in str(filename)][0]
    flux_data_energyprod = [filename for filename in flux_data if source_energyprod in str(filename)][0]

    logger.info("Standardising flux...")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with open(os.devnull, "w") as devnull:
            with contextlib.redirect_stdout(devnull):
                domain = "EUROPE"
                species = "ch4"

                standardise_flux(
                    filepath=flux_data_waste, species=species, source=source_waste, domain=domain
                )
                standardise_flux(
                    filepath=flux_data_energyprod,
                    species=species,
                    source=source_energyprod,
                    domain=domain,
                )

    logger.info("Done.")


def populate_bc() -> None:
    """ """
    populate_bc_ch4()


def populate_bc_ch4() -> None:
    """Populates the tutorial object store with boundary conditions data from the
    example data repository.

    Returns:
        None
    """
    use_tutorial_store()

    logger.info("Retrieving data...")
    eur_2016_bc = (
        "https://github.com/openghg/example_data/raw/main/boundary_conditions/ch4_EUROPE_201607.tar.gz"
    )
    bc_data_path = retrieve_example_data(url=eur_2016_bc)[0]

    bc_input = "CAMS"
    domain = "EUROPE"
    species = "ch4"

    logger.info("Standardising boundary conditions...")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with open(os.devnull, "w") as devnull:
            with contextlib.redirect_stdout(devnull):
                standardise_bc(filepath=bc_data_path, bc_input=bc_input, species=species, domain=domain)

    logger.info("Done.")


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

    logger.info("Retrieving example data...")
    bsd_paths = retrieve_example_data(url=bsd_data)
    tac_paths = retrieve_example_data(url=tac_data)
    capegrim_paths = sorted(retrieve_example_data(url=capegrim_data))

    # Create the tuple required
    capegrim_tuple = (capegrim_paths[0], capegrim_paths[1])

    logger.info("Standardising data...")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with open(os.devnull, "w") as devnull:
            with contextlib.redirect_stdout(devnull):
                standardise_surface(filepath=bsd_paths, source_format="crds", site="bsd", network="decc")
                standardise_surface(filepath=tac_paths, source_format="crds", site="tac", network="decc")
                standardise_surface(
                    filepath=capegrim_tuple,
                    instrument="medusa",
                    source_format="gcwerks",
                    site="cgo",
                    network="agage",
                )

    logger.info("Done.")


def download_edgar_data() -> Path:
    """
    Download edgar data to tutorial store to be used within parse_edgar transform
    tutorial.

    This is currently a limited subset of v6.0 CH4 data (2014-2015)

    TODO: Upgrade to use v7.0 data when this has been checked and added into workflow.
    """

    use_tutorial_store()

    edgar_v60_database = "https://github.com/openghg/example_data/raw/main/databases/TOTALS_nc.tar.gz"

    logger.info("Retrieving example database...")
    edgar_database_path = retrieve_example_data(url=edgar_v60_database)[0]
    logger.info("Done.")

    return edgar_database_path


def use_tutorial_store() -> None:
    """Sets an environment variable telling OpenGHG to use a
    temporary object store. This sets the store to be
    the result of tempfile.gettempdir() / openghg_temp_store.
    To tidy up this store use the clean_tutorial_store function.

    Returns:
        None
    """
    os.environ["OPENGHG_TUT_STORE"] = "1"


def example_extract_path() -> Path:
    """Return the path to folder containing the extracted example files

    Returns:
        None
    """
    from openghg.objectstore import get_tutorial_store_path

    return Path(get_tutorial_store_path(), "extracted_files")


def clear_example_cache() -> None:
    """Removes the file cache created when running the tutorials.

    Returns:
        None
    """
    from openghg.objectstore import get_tutorial_store_path

    example_cache_path = get_tutorial_store_path() / "example_cache"
    extracted_examples = example_extract_path()

    if example_cache_path.exists():
        shutil.rmtree(example_cache_path, ignore_errors=True)
        shutil.rmtree(extracted_examples, ignore_errors=True)


def retrieve_example_obspack(extract_dir: str | Path | None = None) -> Path:
    """Retrieves our example ObsPack dataset, extracts it and returns the path to the folder.

    Args:
        url: URL to retrieve.
        extract_dir: Folder to extract example tarballs to
    Returns:
        Path: Path to directory
    """
    url = "https://github.com/openghg/example_data/raw/main/obspack/obspack_ch4_example.tar.gz"
    files = retrieve_example_data(url=url, extract_dir=extract_dir)
    return files[0].parent


def retrieve_example_data(url: str, extract_dir: str | Path | None = None) -> list[Path]:
    """Retrieve data from the OpenGHG example data repository, cache the downloaded data,
    extract the data and return the filepaths of the extracted files.

    Args:
        url: URL to retrieve.
        extract_dir: Folder to extract example tarballs to
    Returns:
        list: List of filepaths
    """
    from openghg.objectstore import get_tutorial_store_path
    from openghg.util import download_data, parse_url_filename

    use_tutorial_store()

    # Check we're getting a tar
    output_filename = parse_url_filename(url=url)

    suffixes = Path(output_filename).suffixes
    if ".tar" not in suffixes:
        raise ValueError("This function can only currently works with tar files.")

    example_cache_path = get_tutorial_store_path() / "example_cache"

    if not example_cache_path.exists():
        example_cache_path.mkdir(parents=True)

    # cache_record = example_cache_path / "cache_record.json"
    download_path = Path(example_cache_path).joinpath(output_filename)

    # cache_exists = cache_record.is_file()

    # if cache_exists:
    #     cache_data = json.loads(cache_record.read_text())

    #     try:
    #         cached_datapath = Path(cache_data[output_filename])
    #     except KeyError:
    #         cache_data[output_filename] = str(download_path)
    #     else:
    #         return unpack_example_archive(archive_path=cached_datapath, extract_dir=extract_dir)
    # else:
    #     cache_data = {}
    #     cache_data[output_filename] = str(download_path)

    download_data(url=url, filepath=download_path)

    if not download_path.exists():
        raise ValueError("Unable to download file. Please check the URL is correct.")

    # # Make sure we still have all the files in the cache we expect to
    # checked_cache = {}
    # for filename, path in cache_data.items():
    #     if Path(path).exists():
    #         checked_cache[filename] = path

    # cache_record.write_text(json.dumps(checked_cache))

    return unpack_example_archive(archive_path=download_path, extract_dir=extract_dir)


def unpack_example_archive(archive_path: Path, extract_dir: str | Path | None = None) -> list[Path]:
    """Unpacks an tar file to a temporary folder, or extract_dir if given.
    Returns the filepath(s) of the objects.

    Returns:
        list: List of filepaths
    """
    from openghg.tutorial import example_extract_path

    if extract_dir is None:
        extract_dir = example_extract_path()

    with tarfile.open(archive_path) as tar:
        filenames = [f.name for f in tar.getmembers()]

    shutil.unpack_archive(filename=archive_path, extract_dir=extract_dir)

    extracted_filepaths = [Path(extract_dir, str(fname)) for fname in filenames]

    return extracted_filepaths


def clear_tutorial_store() -> None:
    """Delete the contents of the tutorial object store

    Returns:
        None
    """
    from openghg.objectstore import get_tutorial_store_path

    path = get_tutorial_store_path()

    shutil.rmtree(path=path, ignore_errors=True)

    logger.info(f"Tutorial store at {path} cleared.")
