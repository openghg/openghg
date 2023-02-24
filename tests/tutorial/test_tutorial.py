from pathlib import Path

from helpers import get_surface_datapath
from openghg.tutorial import retrieve_example_data


def test_retrieve_example_data(requests_mock, mocker):
    from openghg.util import download_data

    download_mock = mocker.patch("openghg.util.download_data", side_effect=download_data, autospec=True)

    tar_data = Path(get_surface_datapath(filename="test.tar.gz", source_format="crds")).read_bytes()

    url = "https://github.com/openghg/example_data/raw/main/timeseries/bsd_888_example.tar.gz"
    requests_mock.get(url, content=tar_data, status_code=200)

    retrieve_example_data(url=url)

    assert download_mock.call_count == 1

    retrieve_example_data(url=url)

    assert download_mock.call_count == 2
