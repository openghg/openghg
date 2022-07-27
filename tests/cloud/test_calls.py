import msgpack
import pytest
from openghg.cloud import call_function
from openghg.types import FunctionError

# TODO - these need to be expanded to allow proper checks


def test_call_function_raises_both(monkeypatch):
    monkeypatch.setenv("OPENGHG_CLOUD", "1")
    monkeypatch.setenv("OPENGHG_HUB", "1")

    with pytest.raises(ValueError):
        call_function(data={"function": "sweet_function"})


def test_call_cloud(monkeypatch):
    monkeypatch.setenv("OPENGHG_CLOUD", "1")

    with pytest.raises(FunctionError):
        call_function(data={"function": "sweet_function"})


def test_call_hub(monkeypatch, requests_mock):
    monkeypatch.setenv("OPENGHG_HUB", "1")

    to_send = {"data": b"1234"}
    resp = msgpack.packb(to_send)
    requests_mock.post("https://fn.openghg.org/t/openghg", content=resp)

    result = call_function(data={"function": "sweet_function"})

    assert result["content"] == to_send
    assert result["status"] == 200
