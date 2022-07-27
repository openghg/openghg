import pytest


@pytest.fixture()
def set_envs(monkeypatch):
    monkeypatch.setenv("AUTH_KEY", "test-key-123")
    monkeypatch.setenv("OPENGHG_CLOUD", "1")
