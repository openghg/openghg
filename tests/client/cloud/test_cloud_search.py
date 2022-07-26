from openghg.client import search_surface
from openghg.dataobjects import SearchResults
from openghg.util import compress
from helpers import call_function_packager


def test_cloud_search_with_results(mocker):
    sr = SearchResults(results={"bsd_co2": {"keys": {"1": "1", "2": "2", "3": "3"}}})
    compressed_sr = compress(sr.to_json().encode("utf-8"))

    content = {"found": True, "result": compressed_sr}

    to_return = call_function_packager(status=200, headers={}, content=content)

    mocker.patch("openghg.cloud.call_function", return_value=to_return)

    result = search_surface(species="co2", site="tac")

    assert result.results == {"bsd_co2": {"keys": {"1": "1", "2": "2", "3": "3"}}}


def test_cloud_search_no_results(mocker):
    content = {"found": False, "result": False}

    to_return = call_function_packager(status=200, headers={}, content=content)

    mocker.patch("openghg.cloud.call_function", return_value=to_return)

    result = search_surface(species="co2", site="tac")

    assert not result.results
