from openghg.store import data_manager, ObsSurface, Footprints
from openghg.store.base import Datasource
from openghg.retrieve import search_surface
from openghg.standardise import standardise_surface, standardise_footprint
from openghg.objectstore import get_writable_bucket
from openghg.dataobjects import DataManager

import pytest
from helpers import (
    get_surface_datapath,
    clear_test_stores,
    key_to_local_filepath,
    all_datasource_keys,
    get_footprint_datapath,
)


@pytest.fixture(autouse=True)
def add_data(mocker):
    clear_test_stores()
    mock_uuids = [f"test-uuid-{n}" for n in range(100, 150)]
    mocker.patch("uuid.uuid4", side_effect=mock_uuids)
    one_min = get_surface_datapath("tac.picarro.1minute.100m.test.dat", source_format="CRDS")

    standardise_surface(filepaths=one_min, site="tac", network="decc", source_format="CRDS", store="user")


@pytest.fixture()
def footprint_read(mocker):
    clear_test_stores()
    datapath = get_footprint_datapath("footprint_test.nc")

    mock_uuids = [f"test-uuid-{n}" for n in range(100, 188)]
    mocker.patch("uuid.uuid4", side_effect=mock_uuids)
    # model_params = {"simulation_params": "123"}

    site = "TMB"
    network = "LGHG"
    height = "10m"
    domain = "EUROPE"
    model = "test_model"

    standardise_footprint(
        filepath=datapath,
        site=site,
        model=model,
        network=network,
        height=height,
        domain=domain,
        period="monthly",
        high_spatial_res=True,
        store="user",
    )


def test_footprint_metadata_modification(footprint_read):
    search_res = data_manager(data_type="footprints", site="tmb", network="lghg", store="user")

    assert len(search_res.metadata) == 1
    uuid = next(iter(search_res.metadata))

    metadata = search_res.metadata[uuid]

    assert metadata["domain"] == "europe"
    assert metadata["model"] == "test_model"
    assert metadata["max_latitude_high"] == 52.01937

    to_update = {"domain": "antarctica", "model": "peugeot"}
    to_delete = ["max_latitude_high"]

    search_res.update_metadata(uuid=uuid, to_update=to_update, to_delete=to_delete)

    search_res = data_manager(data_type="footprints", site="tmb", network="lghg", store="user")

    metadata = search_res.metadata[uuid]

    assert metadata["domain"] == "antarctica"
    assert metadata["model"] == "peugeot"

    assert "max_latitude_high" not in metadata


def test_delete_footprint_data(footprint_read):
    res = data_manager(data_type="footprints", site="tmb", store="user")

    bucket = get_writable_bucket(name="user")
    with Footprints(bucket=bucket) as fps:
        uuid = fps.datasources()[0]

    ds = Datasource.load(bucket=bucket, uuid=uuid, shallow=True)
    key = ds.key()
    datasource_path = key_to_local_filepath(key=key)

    assert datasource_path[0].exists()

    all_keys = all_datasource_keys(keys=ds._data_keys)
    filepaths = key_to_local_filepath(key=all_keys)
    for k in filepaths:
        assert k.exists()

    assert uuid in fps._datasource_uuids

    res.delete_datasource(uuid=uuid)

    assert not datasource_path[0].exists()

    for k in filepaths:
        assert not k.exists()

    with Footprints(bucket=bucket) as fps:
        assert uuid not in fps._datasource_uuids


def test_object_store_not_in_metadata():
    # metadata = {"object_store" : ""}
    search_res = data_manager(data_type="surface", site="tac", species="co2", store="user")
    uuid = next(iter(search_res.metadata))

    assert "object_store" not in search_res.metadata[uuid]

    with_obj_store = search_res.metadata
    with_obj_store[uuid]["object_store"] = "/tmp/store"

    dm = DataManager(metadata=with_obj_store, store="user")

    assert "object_store" not in dm.metadata[uuid]


def test_find_modify_metadata():
    search_res = data_manager(data_type="surface", site="tac", species="co2", store="user")

    assert len(search_res.metadata) == 1
    uuid = next(iter(search_res.metadata))

    start_metadata = {
        "data_type": "surface",
        "site": "tac",
        "instrument": "picarro",
        "sampling_period": "60.0",
        "inlet": "100m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "calibration_scale": "wmo-x2019",
        "long_name": "tacolneston",
        "inlet_height_magl": "100",
        "data_owner": "simon o'doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "station_longitude": 1.13872,
        "station_latitude": 52.51775,
        "station_long_name": "tacolneston tower, uk",
        "station_height_masl": 50.0,
        "uuid": "test-uuid-105",
        "start_date": "2012-07-31 14:50:30+00:00",
        "end_date": "2019-06-26 15:54:29+00:00",
        "latest_version": "v1",
    }

    assert search_res.metadata[uuid].items() >= start_metadata.items()

    to_add = {"forgotten_key": "tis_but_a_scratch", "another_key": "swallow", "a_third": "parrot"}

    search_res.update_metadata(uuid=uuid, to_update=to_add)

    assert search_res.metadata[uuid].items() >= to_add.items()

    res = search_surface(site="tac", species="co2")

    for key, value in to_add.items():
        assert res.metadata[uuid][key] == value


def test_modify_multiple_uuids():
    res = data_manager(data_type="surface", site="tac", store="user")

    uuids = sorted(res.metadata.keys())

    assert not res.metadata[uuids[0]]["data_owner"] == "michael palin"
    assert not res.metadata[uuids[0]]["data_owner_email"] == "palin@python.com"
    assert not res.metadata[uuids[1]]["data_owner"] == "michael palin"
    assert not res.metadata[uuids[1]]["data_owner_email"] == "palin@python.com"

    uids = list(res.metadata.keys())
    to_add = {"data_owner": "michael palin", "data_owner_email": "palin@python.com"}

    res.update_metadata(uuid=uids, to_update=to_add)

    search_res = search_surface(site="tac", inlet="100m")

    for metadata in search_res.metadata.values():
        assert metadata["data_owner"] == "michael palin"
        assert metadata["data_owner_email"] == "palin@python.com"


def test_invalid_uuid_raises():
    res = data_manager(data_type="surface", site="tac", store="user")

    with pytest.raises(ValueError):
        res.update_metadata(uuid="123-567", to_update={})


def test_delete_metadata_keys():
    res = data_manager(data_type="surface", site="tac", species="ch4", inlet="100m", store="user")

    expected = {
        "site": "tac",
        "instrument": "picarro",
        "sampling_period": "60.0",
        "inlet": "100m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "ch4",
        "calibration_scale": "wmo-x2004a",
        "long_name": "tacolneston",
        "data_type": "surface",
        "inlet_height_magl": "100",
        "data_owner": "simon o'doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "station_longitude": 1.13872,
        "station_latitude": 52.51775,
        "station_long_name": "tacolneston tower, uk",
        "station_height_masl": 50.0,
        "start_date": "2012-07-31 14:50:30+00:00",
        "end_date": "2019-06-26 15:54:29+00:00",
        "latest_version": "v1",
        "uuid": "test-uuid-100",
    }

    assert res.metadata["test-uuid-100"].items() >= expected.items()

    # Delete a key giving it a string
    res.update_metadata(uuid="test-uuid-100", to_delete="species")

    res = data_manager(data_type="surface", site="tac", inlet="100m", store="user")

    assert "species" not in res.metadata["test-uuid-100"]

    res = data_manager(data_type="surface", site="tac", species="ch4", inlet="100m", store="user")

    assert not res

    res = data_manager(data_type="surface", site="tac", inlet="100m", store="user")

    # Delete keys passing in a list
    res.update_metadata(uuid="test-uuid-100", to_delete=["site", "inlet"])

    res.refresh()

    assert "site" not in res.metadata["test-uuid-100"]
    assert "inlet" not in res.metadata["test-uuid-100"]


def test_delete_and_modify_keys():
    res = data_manager(data_type="surface", site="tac", species="ch4", inlet="100m", store="user")

    to_delete = ["station_longitude", "station_latitude"]

    res.update_metadata(uuid="test-uuid-100", to_delete=to_delete)

    search_res = search_surface(site="tac", inlet="100m", species="ch4")

    fresh_metadata = search_res.metadata["test-uuid-100"]

    assert "station_longitude" not in fresh_metadata
    assert "station_latitide" not in fresh_metadata

    res = data_manager(data_type="surface", site="tac", species="ch4", store="user")

    to_update = {"sampling_period": "12H", "tasty_dish": "pasta"}

    # We've already deleted these keys
    with pytest.raises(KeyError):
        res.update_metadata(uuid="test-uuid-100", to_delete=to_delete, to_update=to_update)

    to_delete = ["long_name"]

    res.update_metadata(uuid="test-uuid-100", to_delete=to_delete, to_update=to_update)

    search_res = search_surface(site="tac", inlet="100m", species="ch4")

    freshest_metadata = search_res.metadata["test-uuid-100"]

    assert "long_name" not in freshest_metadata

    assert freshest_metadata["sampling_period"] == "12H"
    assert freshest_metadata["tasty_dish"] == "pasta"


def test_try_delete_none_modify_none_changes_nothing():
    res = data_manager(data_type="surface", site="tac", inlet="100m", species="ch4", store="user")

    res.update_metadata(uuid="test-uuid-100")
    res.update_metadata(uuid="test-uuid-100", to_update={}, to_delete=[])

    res2 = data_manager(data_type="surface", site="tac", inlet="100m", species="ch4", store="user")

    assert res.metadata == res2.metadata


def test_delete_data():
    res = data_manager(data_type="surface", site="tac", inlet="100m", species="ch4", store="user")

    uid = next(iter(res.metadata))

    bucket = get_writable_bucket(name="user")
    d = Datasource.load(bucket=bucket, uuid=uid)
    key = d.key()

    with ObsSurface(bucket) as obs:
        assert uid in obs._datasource_uuids

    datasource_path = key_to_local_filepath(key=key)[0]

    ds_keys = d._data_keys

    assert datasource_path.exists()

    all_keys = all_datasource_keys(keys=ds_keys)
    key_paths = key_to_local_filepath(key=all_keys)

    for k in key_paths:
        assert k.exists()

    res.delete_datasource(uuid=uid)

    assert not datasource_path.exists()

    for k in key_paths:
        assert not k.exists()

    with ObsSurface(bucket) as obs:
        assert uid not in obs._datasource_uuids


@pytest.mark.xfail(reason="Failing due to the Datasource save bug - issue 724", raises=AssertionError)
def test_metadata_backup_restore():
    res_one = data_manager(data_type="surface", site="tac", inlet="100m", species="ch4", store="user")

    uid = next(iter(res_one.metadata))

    version_one = res_one.metadata[uid].copy()

    to_update = {"owner": "john"}

    res_one.update_metadata(uuid=uid, to_update=to_update)

    assert res_one.metadata[uid]["owner"] == "john"

    # Force a refresh from the object store
    res_one.refresh()

    assert res_one.metadata[uid]["owner"] == "john"

    assert res_one.metadata[uid] != version_one

    res_one.restore(uuid=uid)

    assert res_one.metadata[uid] == version_one

    res_one.refresh()

    assert res_one.metadata[uid] == version_one


def test_delete_update_uuid_raises():
    res_one = data_manager(data_type="surface", site="tac", inlet="100m", species="ch4", store="user")
    uid = next(iter(res_one.metadata))

    with pytest.raises(ValueError):
        res_one.update_metadata(uuid=uid, to_delete=["uuid"])

    with pytest.raises(ValueError):
        res_one.update_metadata(uuid=uid, to_update={"uuid": 123})


def test_metadata_backup_restore_multiple_changes():
    res_one = data_manager(data_type="surface", site="tac", inlet="100m", species="ch4", store="user")

    uid = next(iter(res_one.metadata))

    to_update = {"owner": "john"}

    res_one.update_metadata(uuid=uid, to_update=to_update)

    assert res_one.metadata[uid]["owner"] == "john"

    res_one.refresh()

    assert res_one.metadata[uid]["owner"] == "john"

    to_update = {"species": "sparrow"}

    res_one.update_metadata(uuid=uid, to_update=to_update)

    first_backup = res_one._backup[uid]["1"]

    assert "owner" not in first_backup

    second_backup = res_one._backup[uid]["2"]

    assert second_backup["owner"] == "john"

    assert "sparrow" not in second_backup

    assert len(res_one._backup[uid]) == 2

    res_one.restore(uuid=uid, version=1)

    assert res_one.metadata[uid] == first_backup
