import pytest
from helpers import clear_test_stores, get_emissions_datapath, get_footprint_datapath
from openghg.objectstore import delete_object, get_writable_bucket, integrity_check
from openghg.objectstore.metastore import open_metastore
from openghg.standardise import standardise_flux, standardise_footprint
from openghg.store.base import Datasource
from openghg.types import ObjectStoreError


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
        uid = metastore.select("uuid")[0]
        ds = Datasource.load(bucket=bucket, uuid=uid)
        keys = ds.data_keys()
        for key in keys:
            delete_object(bucket=bucket, key=key)

    with pytest.raises(ObjectStoreError):
        integrity_check()
