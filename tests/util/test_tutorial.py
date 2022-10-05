from pathlib import Path

from helpers import get_retrieval_datapath, get_surface_datapath
from openghg.util import bilsdale_datapaths, retrieve_example_data


def test_bilsdale_data():
    paths = bilsdale_datapaths()

    names = [p.name for p in paths]
    names.sort()

    assert names == [
        "bsd.picarro.1minute.108m.min.dat",
        "bsd.picarro.1minute.248m.min.dat",
        "bsd.picarro.1minute.42m.min.dat",
    ]


def test_retrieve_example_data(requests_mock, mocker):
    from openghg.util import download_data

    download_mock = mocker.patch("openghg.util.download_data", side_effect=download_data, autospec=True)

    tar_data = Path(get_surface_datapath(filename="test.tar.gz", source_format="crds")).read_bytes()

    url = "https://github.com/openghg/example_data/raw/main/timeseries/bsd_example.tar.gz"
    requests_mock.get(url, content=tar_data, status_code=200)

    retrieve_example_data(url=url, extract_dir="/tmp")

    assert download_mock.call_count == 1

    retrieve_example_data(url=url, extract_dir="/tmp")

    assert download_mock.call_count == 1
