"""Configuration selects backends without imposing the local filesystem layout."""

import sys
from types import ModuleType

import pytest

from openghg.objectstore import locking_object_store, open_object_store
from openghg.objectstore._factory import configured_object_store, get_object_store_config
from openghg.types import ConfigFileError, ObjectStoreError


@pytest.fixture
def custom_backend(monkeypatch):
    calls = []

    class Store:
        entered = 0
        closed = 0

        def __enter__(self):
            self.entered += 1
            return self

        def __exit__(self, *args):
            self.closed += 1

    store = Store()
    module = ModuleType("test_objectstore_backend")

    def factory(**kwargs):
        calls.append(kwargs)
        return store

    module.factory = factory
    monkeypatch.setitem(sys.modules, module.__name__, module)
    config = {
        "path": "custom://server/catalog",
        "permissions": "rw",
        "factory": "test_objectstore_backend:factory",
        "options": {"profile": "science"},
        "credentials_env": {"token": "OPENGHG_TEST_BACKEND_TOKEN"},
    }
    monkeypatch.setenv("OPENGHG_TEST_BACKEND_TOKEN", "first-token")
    monkeypatch.setattr(
        "openghg.objectstore._local_store.read_local_config",
        lambda: {"object_store": {"custom": config}},
    )
    return config, store, calls


def test_both_entry_points_select_factory_and_resolve_credentials_at_open(custom_backend, monkeypatch):
    config, store, calls = custom_backend
    with open_object_store(config["path"], "surface", mode="r") as opened:
        assert opened is store
    assert store.entered == store.closed == 1
    assert calls[0] == {
        "bucket": config["path"],
        "data_type": "surface",
        "mode": "r",
        "skip_keys": None,
        "extend_keys": None,
        "profile": "science",
        "token": "first-token",
    }

    monkeypatch.setenv("OPENGHG_TEST_BACKEND_TOKEN", "second-token")
    locking_store = locking_object_store(config["path"], "surface", skip_keys=["path"], extend_keys=["tag"])
    assert locking_store is store
    assert store.entered == 1  # Creation does not acquire the write context.
    with locking_store:
        pass
    with locking_store:
        pass
    assert store.entered == store.closed == 3
    assert calls[1]["token"] == "second-token"
    assert calls[1]["skip_keys"] == ["path"]
    assert calls[1]["extend_keys"] == ["tag"]
    assert "token" not in config["options"]


def test_document_operations_allow_empty_data_type(custom_backend):
    config, store, calls = custom_backend
    assert configured_object_store(config["path"], "") is store
    assert calls[0]["data_type"] == ""


@pytest.mark.parametrize(
    ("setting", "value", "message"),
    [
        ("factory", "invalid", "module:callable"),
        ("factory", "test_objectstore_backend:missing", "Unable to import"),
        ("factory", "test_objectstore_backend:__name__", "not callable"),
        ("options", [], "TOML tables"),
        ("options", {"mode": "rw"}, "cannot override"),
        ("options", {"token": "do-not-log-this"}, "distinct argument names"),
        ("credentials_env", {"mode": "TOKEN"}, "cannot override"),
        ("credentials_env", {"token": 3}, "environment variable"),
        ("credentials_env", {"token": "OPENGHG_TEST_MISSING_TOKEN"}, "is unset"),
    ],
)
def test_invalid_factory_configuration_fails_before_open(
    custom_backend, setting, value, message, monkeypatch
):
    config, _, calls = custom_backend
    monkeypatch.delenv("OPENGHG_TEST_MISSING_TOKEN", raising=False)
    config[setting] = value
    with pytest.raises(ConfigFileError, match=message) as exc:
        configured_object_store(config["path"], "surface")
    assert "do-not-log-this" not in str(exc.value)
    assert not calls


def test_permissions_checked_before_custom_factory(custom_backend):
    config, store, calls = custom_backend
    config["permissions"] = "r"
    with pytest.raises(ObjectStoreError, match="does not allow writes"):
        configured_object_store(config["path"], "surface")
    assert not calls
    assert configured_object_store(config["path"], "surface", mode="r") is store


def test_ambiguous_custom_locations_are_rejected(custom_backend, monkeypatch):
    config, _, _ = custom_backend
    monkeypatch.setattr(
        "openghg.objectstore._local_store.read_local_config",
        lambda: {"object_store": {"a": config, "b": dict(config, permissions="r")}},
    )
    with pytest.raises(ConfigFileError, match="distinct configured locations"):
        get_object_store_config(config["path"])


def test_unconfigured_locations_keep_local_paths_and_reject_uris(custom_backend, tmp_path):
    assert configured_object_store(str(tmp_path), "surface") is None
    with pytest.raises(ObjectStoreError, match="requires a configured factory"):
        configured_object_store("custom://unconfigured/catalog", "surface")


def test_direct_local_path_without_config(monkeypatch, tmp_path):
    def no_config():
        raise ConfigFileError("Configuration does not exist")

    monkeypatch.setattr("openghg.objectstore._local_store.read_local_config", no_config)
    monkeypatch.setattr("openghg.util._user.get_user_config_path", lambda: tmp_path / "missing.conf")
    assert configured_object_store(str(tmp_path), "surface") is None


def test_existing_invalid_config_is_not_silently_ignored(monkeypatch, tmp_path):
    def invalid_config():
        raise ConfigFileError("Invalid existing configuration")

    config_path = tmp_path / "openghg.conf"
    config_path.touch()
    monkeypatch.setattr("openghg.objectstore._local_store.read_local_config", invalid_config)
    monkeypatch.setattr("openghg.util._user.get_user_config_path", lambda: config_path)
    with pytest.raises(ConfigFileError, match="Invalid existing configuration"):
        configured_object_store(str(tmp_path), "surface")
