import pytest
from openghg.objectstore import integrity_check
from openghg.standardise import standardise_flux, standardise_footprint
from openghg.objectstore import get_writable_bucket, delete_object
from openghg.types import ObjectStoreError
from helpers import get_footprint_datapath, get_emissions_datapath, clear_test_stores
from openghg.store.base import Datasource
from openghg.objectstore.metastore import open_metastore
import tinydb


@pytest.fixture(autouse=True)
def populate_store():
    clear_test_stores()

    datapath = get_footprint_datapath("footprint_test.nc")

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
        high_spatial_resolution=True,
        store="user",
    )

    emissions_datapath = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2012.nc")
    standardise_flux(
        filepath=emissions_datapath,
        species="co2",
        source="gpp-cardamom",
        domain="europe",
        high_time_resolution=False,
        store="user",
    )


def test_integrity_check_delete_Datasource_keys():
    integrity_check()

    # Now delete some of the Datasources
    bucket = get_writable_bucket(name="user")
    with open_metastore(bucket=bucket, data_type="emissions") as metastore:
        uid = metastore.search()[0]['uuid']
        ds = Datasource.load(bucket=bucket, uuid=uid)
        keys = ds.data_keys()
        for key in keys:
            delete_object(bucket=bucket, key=key)

    with pytest.raises(ObjectStoreError):
        integrity_check()

@pytest.mark.xfail(reason="metastore.datasources() now returns metastore uuids")
def test_integrity_delete_uuids_metastore():
    integrity_check()

    bucket = get_writable_bucket(name="user")
    with open_metastore(bucket=bucket, data_type="footprints") as metastore:
        uids = [result['uuid'] for result in metastore.search()[:4]]
        for u in uids:
            metastore._metastore.remove(tinydb.where("uuid") == u)

    with pytest.raises(ObjectStoreError):
        integrity_check()
