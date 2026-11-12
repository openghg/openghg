"""The configured factory is used throughout the normal public workflow."""

from copy import deepcopy
import sys
from types import ModuleType

from helpers import get_surface_datapath

from openghg.dataobjects import data_manager
from openghg.objectstore import locking_object_store
from openghg.retrieve import get_obs_surface, search_surface
from openghg.standardise import standardise_surface
from openghg.store import create_custom_config, get_metakeys, write_metakeys
from openghg.store._metakeys_config import get_metakey_defaults


def test_factory_used_for_standardisation_retrieval_management_and_documents(tmp_path, monkeypatch):
    """Map an opaque configured URI to a local backend without changing public callers."""
    location = "example://catalog/scientific-data"
    calls = []
    module = ModuleType("test_store_plugin")

    def factory(*, bucket, data_type, mode, skip_keys, extend_keys, token):
        assert bucket == location
        assert token == "from-environment"
        calls.append((data_type, mode))
        return locking_object_store(
            str(tmp_path), data_type, mode=mode, skip_keys=skip_keys, extend_keys=extend_keys
        )

    module.factory = factory
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setenv("TEST_STORE_TOKEN", "from-environment")
    configuration = {
        "object_store": {
            "user": {
                "path": location,
                "permissions": "rw",
                "factory": "test_store_plugin:factory",
                "credentials_env": {"token": "TEST_STORE_TOKEN"},
            }
        }
    }
    monkeypatch.setattr("openghg.objectstore._local_store.read_local_config", lambda: configuration)

    create_custom_config(location)
    metakeys = deepcopy(get_metakey_defaults())
    metakeys["surface"]["optional"]["backend_note"] = {"type": ["str"]}
    write_metakeys(location, metakeys)
    assert get_metakeys(location) == metakeys

    for filename, if_exists in (
        ("bsd.picarro.1minute.248m.min.dat", "auto"),
        ("bsd.picarro.1minute.248m.co2_mod.dat", "new"),
    ):
        standardise_surface(
            store="user",
            filepath=get_surface_datapath(filename, source_format="CRDS"),
            site="bsd",
            network="decc",
            source_format="CRDS",
            if_exists=if_exists,
        )
    results = search_surface(store="user", site="bsd", species="co2", inlet="248m")
    uuid = next(iter(results.metadata))
    assert results.metadata[uuid]["object_store"] == location
    old = get_obs_surface(store="user", site="bsd", species="co2", inlet="248m", version="v1")
    latest = get_obs_surface(store="user", site="bsd", species="co2", inlet="248m")
    assert old.data.mf.max() < 425
    assert latest.data.mf.min() > 9000

    manager = data_manager(data_type="surface", store="user", site="bsd", species="co2", inlet="248m")
    manager.update_metadata(uuid, to_update={"comment": "configured backend"})
    manager.update_attributes(uuid, to_update={"test_attribute": "written through datasource"})
    changed = get_obs_surface(store="user", site="bsd", species="co2", inlet="248m")
    assert changed.data.attrs["test_attribute"] == "written through datasource"
    manager.delete_datasource(uuid)
    assert not search_surface(store="user", site="bsd", species="co2", inlet="248m")
    assert ("", "rw") in calls
    assert ("surface", "rw") in calls
    assert ("surface", "r") in calls
