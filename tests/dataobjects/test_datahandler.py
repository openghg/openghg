from openghg.store import data_handler_lookup, ObsSurface, Footprints
from openghg.store.base import Datasource
from openghg.retrieve import search_surface
from openghg.standardise import standardise_surface
from openghg.objectstore import get_bucket

import pytest
from helpers import (
    get_surface_datapath,
    clear_test_store,
    key_to_local_filepath,
    all_datasource_keys,
    get_footprint_datapath,
)


@pytest.fixture(autouse=True)
def add_data(mocker):
    clear_test_store()
    mock_uuids = [f"test-uuid-{n}" for n in range(100, 150)]
    mocker.patch("uuid.uuid4", side_effect=mock_uuids)
    one_min = get_surface_datapath("tac.picarro.1minute.100m.test.dat", source_format="CRDS")

    standardise_surface(filepaths=one_min, site="tac", network="decc", source_format="CRDS")


@pytest.fixture()
def footprint_read(mocker):
    clear_test_store()
    datapath = get_footprint_datapath("footprint_test.nc")

    mock_uuids = [f"test-uuid-{n}" for n in range(100, 188)]
    mocker.patch("uuid.uuid4", side_effect=mock_uuids)
    # model_params = {"simulation_params": "123"}

    site = "TMB"
    network = "LGHG"
    height = "10m"
    domain = "EUROPE"
    model = "test_model"

    bucket = get_bucket()
    with Footprints(bucket=bucket) as fps:
        fps.read_file(
            filepath=datapath,
            site=site,
            model=model,
            network=network,
            height=height,
            domain=domain,
            period="monthly",
            high_spatial_res=True,
        )


def test_footprint_metadata_modification(footprint_read):
    search_res = data_handler_lookup(data_type="footprints", site="tmb", network="lghg")

    assert len(search_res.metadata) == 1
    uuid = next(iter(search_res.metadata))

    metadata = search_res.metadata[uuid]

    assert metadata["domain"] == "europe"
    assert metadata["model"] == "test_model"
    assert metadata["max_latitude_high"] == 52.01937

    to_update = {"domain": "antarctica", "model": "peugeot"}
    to_delete = ["max_latitude_high"]

    bucket = get_bucket()
    search_res.update_metadata(bucket=bucket, uuid=uuid, to_update=to_update, to_delete=to_delete)

    search_res = data_handler_lookup(data_type="footprints", site="tmb", network="lghg")

    metadata = search_res.metadata[uuid]

    assert metadata["domain"] == "antarctica"
    assert metadata["model"] == "peugeot"

    assert "max_latitude_high" not in metadata


def test_delete_footprint_data(footprint_read):
    res = data_handler_lookup(data_type="footprints", site="tmb")

    bucket = get_bucket()
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

    res.delete_datasource(bucket=bucket, uuid=uuid)

    assert not datasource_path[0].exists()

    for k in filepaths:
        assert not k.exists()

    with Footprints(bucket=bucket) as fps:
        assert uuid not in fps._datasource_uuids


def test_find_modify_metadata():
    search_res = data_handler_lookup(data_type="surface", site="tac", species="co2")

    assert len(search_res.metadata) == 1
    uuid = next(iter(search_res.metadata))

    to_add = {"forgotten_key": "tis_but_a_scratch", "another_key": "swallow", "a_third": "parrot"}

    start_metadata = {
        uuid: {
            "site": "tac",
            "instrument": "picarro",
            "sampling_period": "60.0",
            "inlet": "100m",
            "port": "9",
            "type": "air",
            "network": "decc",
            "species": "co2",
            "calibration_scale": "wmo-x2007",
            "long_name": "tacolneston",
            "data_type": "surface",
            "inlet_height_magl": "100m",
            "data_owner": "simon o'doherty",
            "data_owner_email": "s.odoherty@bristol.ac.uk",
            "station_longitude": 1.13872,
            "station_latitude": 52.51775,
            "station_long_name": "tacolneston tower, uk",
            "station_height_masl": 50.0,
            "uuid": "test-uuid-101",
        }
    }

    bucket = get_bucket()
    search_res.update_metadata(bucket=bucket, uuid=uuid, to_update=to_add)

    res = search_surface(site="tac", species="co2")

    diff_d = {k: v for k, v in res.metadata[uuid].items() if k not in start_metadata[uuid]}

    assert res.metadata[uuid].items() >= to_add.items()

    # assert diff_d == {"forgotten_key": "tis_but_a_scratch", "another_key": "swallow", "a_third": "parrot"}


def test_modify_multiple_uuids():
    res = data_handler_lookup(data_type="surface", site="tac")

    uuids = sorted(res.metadata.keys())

    assert not res.metadata[uuids[0]]["data_owner"] == "michael palin"
    assert not res.metadata[uuids[0]]["data_owner_email"] == "palin@python.com"
    assert not res.metadata[uuids[1]]["data_owner"] == "michael palin"
    assert not res.metadata[uuids[1]]["data_owner_email"] == "palin@python.com"

    uids = list(res.metadata.keys())
    to_add = {"data_owner": "michael palin", "data_owner_email": "palin@python.com"}

    bucket = get_bucket()
    res.update_metadata(bucket=bucket, uuid=uids, to_update=to_add)

    search_res = search_surface(site="tac", inlet="100m")

    for metadata in search_res.metadata.values():
        assert metadata["data_owner"] == "michael palin"
        assert metadata["data_owner_email"] == "palin@python.com"


def test_invalid_uuid_raises():
    res = data_handler_lookup(data_type="surface", site="tac")

    bucket = get_bucket()

    with pytest.raises(ValueError):
        res.update_metadata(bucket=bucket, uuid="123-567", to_update={})


def test_delete_metadata_keys():
    res = data_handler_lookup(data_type="surface", site="tac", species="ch4", inlet="100m")

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
        "uuid": "test-uuid-100",
    }

    assert res.metadata["test-uuid-100"].items() >= expected.items()

    bucket = get_bucket()
    res.update_metadata(bucket=bucket, uuid="test-uuid-100", to_delete=["species"])

    res = data_handler_lookup(data_type="surface", site="tac", inlet="100m")

    assert "species" not in res.metadata["test-uuid-100"]

    res = data_handler_lookup(data_type="surface", site="tac", species="ch4", inlet="100m")

    assert not res


def test_delete_and_modify_keys():
    res = data_handler_lookup(data_type="surface", site="tac", species="ch4", inlet="100m")

    bucket = get_bucket()

    to_delete = ["station_longitude", "station_latitude"]

    res.update_metadata(bucket=bucket, uuid="test-uuid-100", to_delete=to_delete)

    search_res = search_surface(site="tac", inlet="100m", species="ch4")

    fresh_metadata = search_res.metadata["test-uuid-100"]

    assert "station_longitude" not in fresh_metadata
    assert "station_latitide" not in fresh_metadata

    res = data_handler_lookup(data_type="surface", site="tac", species="ch4")

    to_update = {"sampling_period": "12H", "tasty_dish": "pasta"}

    # We've already deleted these keys
    with pytest.raises(KeyError):
        res.update_metadata(bucket=bucket, uuid="test-uuid-100", to_delete=to_delete, to_update=to_update)

    to_delete = ["long_name"]

    res.update_metadata(bucket=bucket, uuid="test-uuid-100", to_delete=to_delete, to_update=to_update)

    search_res = search_surface(site="tac", inlet="100m", species="ch4")

    freshest_metadata = search_res.metadata["test-uuid-100"]

    assert "long_name" not in freshest_metadata

    assert freshest_metadata["sampling_period"] == "12H"
    assert freshest_metadata["tasty_dish"] == "pasta"


def test_try_delete_none_modify_none_changes_nothing():
    res = data_handler_lookup(data_type="surface", site="tac", inlet="100m", species="ch4")

    bucket = get_bucket()

    res.update_metadata(bucket=bucket, uuid="test-uuid-100")

    res.update_metadata(bucket=bucket, uuid="test-uuid-100", to_update={}, to_delete=[])

    res2 = data_handler_lookup(data_type="surface", site="tac", inlet="100m", species="ch4")

    assert res.metadata == res2.metadata


def test_delete_data():
    res = data_handler_lookup(data_type="surface", site="tac", inlet="100m", species="ch4")

    uid = next(iter(res.metadata))

    bucket = get_bucket()
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

    res.delete_datasource(bucket=bucket, uuid=uid)

    assert not datasource_path.exists()

    for k in key_paths:
        assert not k.exists()

    with ObsSurface(bucket) as obs:
        assert uid not in obs._datasource_uuids


def test_metadata_backup_restore():
    res_one = data_handler_lookup(data_type="surface", site="tac", inlet="100m", species="ch4")

    uid = next(iter(res_one.metadata))

    version_one = res_one.metadata[uid].copy()

    to_update = {"owner": "john"}

    bucket = get_bucket()
    res_one.update_metadata(bucket=bucket, uuid=uid, to_update=to_update)

    assert res_one.metadata[uid]["owner"] == "john"

    # Force a refresh from the object store
    res_one.refresh()

    assert res_one.metadata[uid]["owner"] == "john"

    assert res_one.metadata[uid] != version_one

    res_one.restore(bucket=bucket, uuid=uid)

    assert res_one.metadata[uid] == version_one

    res_one.refresh()

    assert res_one.metadata[uid] == version_one


def test_delete_update_uuid_raises():
    res_one = data_handler_lookup(data_type="surface", site="tac", inlet="100m", species="ch4")
    uid = next(iter(res_one.metadata))

    bucket = get_bucket()

    with pytest.raises(ValueError):
        res_one.update_metadata(bucket=bucket, uuid=uid, to_delete=["uuid"])

    with pytest.raises(ValueError):
        res_one.update_metadata(bucket=bucket, uuid=uid, to_update={"uuid": 123})


def test_metadata_backup_restore_multiple_changes():
    res_one = data_handler_lookup(data_type="surface", site="tac", inlet="100m", species="ch4")

    uid = next(iter(res_one.metadata))

    to_update = {"owner": "john"}

    bucket = get_bucket()

    res_one.update_metadata(bucket=bucket, uuid=uid, to_update=to_update)

    assert res_one.metadata[uid]["owner"] == "john"

    res_one.refresh()

    assert res_one.metadata[uid]["owner"] == "john"

    to_update = {"species": "sparrow"}

    res_one.update_metadata(bucket=bucket, uuid=uid, to_update=to_update)

    first_backup = res_one._backup[uid]["1"]

    assert "owner" not in first_backup

    second_backup = res_one._backup[uid]["2"]

    assert second_backup["owner"] == "john"

    assert "sparrow" not in second_backup

    assert len(res_one._backup[uid]) == 2

    res_one.restore(bucket=bucket, uuid=uid, version=1)

    assert res_one.metadata[uid] == first_backup
