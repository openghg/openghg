from openghg.util import parse_url_filename, download_data


def test_url_parser():
    url = "https://example.com/test/download.nc"

    assert parse_url_filename(url) == "download.nc"

    url = "http://example.xyz/%20/that/this/some_file.tar.bz2?some_query_params=true&some_more=true#and-an-anchor"

    assert parse_url_filename(url) == "some_file.tar.bz2"

    url = "http://example.com/no_file_extension"

    assert parse_url_filename(url) == "no_file_extension"


def test_download_data(requests_mock, capfd):
    content = "some_excellent_bytes".encode("utf-8")
    url = "http://example.com/data/test.nc"
    requests_mock.get(url, content=content)

    data = download_data(url=url)

    assert data == content

    requests_mock.get(url, status_code=404)

    data = download_data(url=url)

    out, _ = capfd.readouterr()

    assert "Unable to download http://example.com/data/test.nc, please check URL." in out
