import msgpack
import pytest
from openghg.cloud import call_function
from openghg.types import FunctionError

# TODO - these need to be expanded to allow proper checks


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("OPENGHG_FN_URL", "https://localhost/t/openghg")
    monkeypatch.setenv("AUTH_KEY", "test-key-123")


def test_call_hub(monkeypatch, requests_mock):
    to_send = {"data": b"1234"}
    resp = msgpack.packb(to_send)
    requests_mock.post("https://localhost/t/openghg", content=resp)

    result = call_function(data={"function": "sweet_function"})

    assert result["content"] == to_send
    assert result["status"] == 200


def test_incorrect_call_raises_functionerror(requests_mock):
    requests_mock.post("https://localhost/t/openghg", status_code=400, content=b"Error")

    with pytest.raises(FunctionError):
        call_function(data={"function": "sweet_function"})
