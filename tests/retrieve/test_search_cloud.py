import pytest
from helpers import call_function_packager
from openghg.dataobjects import SearchResults
from openghg.retrieve import search_surface
from openghg.util import compress


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("OPENGHG_HUB", "1")


# I'm not sure if these tests really do anything

@pytest.mark.xfail(reason="Cloud tests marked for removal. Cloud code needs rewrite.")
def test_cloud_search_with_results(mocker):
    metadata = {"site": "london"}
    sr = SearchResults(keys={"data": [1, 2, 3]}, metadata=metadata)

    compressed_sr = compress(sr.to_json().encode("utf-8"))

    content = {"found": True, "result": compressed_sr}

    to_return = call_function_packager(status=200, headers={}, content=content)

    mocker.patch("openghg.cloud.call_function", return_value=to_return)

    result = search_surface(species="co2", site="tac")

    assert result.metadata == metadata


def test_cloud_search_no_results(mocker):
    content = {"found": False, "result": False}

    to_return = call_function_packager(status=200, headers={}, content=content)

    mocker.patch("openghg.cloud.call_function", return_value=to_return)

    result = search_surface(species="co2", site="tac")

    assert not result.results
