from pathlib import Path

from helpers import get_datapath
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


def test_retrieve_example_data(requests_mock, tmpdir):
    tar_data = Path(get_datapath(filename="test.tar.gz", data_type="crds")).read_bytes()

    url = "https://github.com/openghg/example_data/raw/main/timeseries/bsd_example.tar.gz"
    requests_mock.get(url, content=tar_data, status_code=200)

    filename = retrieve_example_data(path="timeseries/bsd_example.tar.gz", download_dir=str(tmpdir))

    with open(filename) as f:
        d = f.readline()
        assert d.strip() == "Created: 28 Sep 20 07:34 GMT"
