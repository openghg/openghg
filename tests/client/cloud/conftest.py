import pytest
import json


@pytest.fixture()
def set_envs(monkeypatch):
    urls = json.dumps({"standardise": "http://localhost"})
    monkeypatch.setenv("FN_URLS", urls)
    monkeypatch.setenv("AUTH_KEY", "test-key-123")
    monkeypatch.setenv("OPENGHG_CLOUD", "1")
