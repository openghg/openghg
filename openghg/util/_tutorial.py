""" Helper functions to provide datapaths etc used in the tutorial notebooks

"""
from pathlib import Path
from typing import List, Union

__all__ = ["bilsdale_datapaths"]


def bilsdale_datapaths() -> List:
    """Return a list of paths to the Tacolneston data for use in the ranking
    tutorial

    Returns:
        list: List of paths
    """
    crds_path = Path(__file__).resolve().parent.parent.parent.joinpath("tests/data/proc_test_data/CRDS")

    return list(crds_path.glob("bsd.picarro.1minute.*.min.*"))


def retrieve_example_data(
    path: str, output_filename: str = None, download_dir: str = None
) -> Union[List, Path]:
    """Retrieve data from the OpenGHG example data repository and write it to a temporary file
    for reading.
    """
    import tempfile
    import requests
    import shutil

    url = f"https://github.com/openghg/example_data/raw/main/{path}"

    if download_dir is None:
        download_dir = tempfile.mkdtemp()

    if output_filename is None:
        output_filename = url.split("/")[-1]

    download_path = Path(download_dir).joinpath(output_filename)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        with open(download_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)

    shutil.unpack_archive(filename=download_path, extract_dir=download_dir)

    extracted_filepaths = [f for f in Path(download_dir).glob("*") if not f.name.endswith("tar.gz")]

    if len(extracted_filepaths) == 1:
        return extracted_filepaths[0]
    else:
        return extracted_filepaths
