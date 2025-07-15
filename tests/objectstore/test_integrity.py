import pytest
from openghg.objectstore import integrity_check
from openghg.standardise import standardise_flux, standardise_footprint
from openghg.objectstore import get_writable_bucket
from openghg.objectstore._objectstore import open_object_store
from openghg.types import ObjectStoreError
from helpers import get_footprint_datapath, get_flux_datapath, clear_test_stores


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

    flux_datapath = get_flux_datapath("co2-gpp-cardamom_EUROPE_2012.nc")
    standardise_flux(
        filepath=flux_datapath,
        species="co2",
        source="gpp-cardamom",
        domain="europe",
        time_resolved=False,
        store="user",
    )


def test_integrity_check_delete_datasource_keys():
    integrity_check()

    # Now delete some of the Datasources
    bucket = get_writable_bucket(name="user")
    with open_object_store(bucket=bucket, data_type="flux") as objstore:
        uid = objstore.uuids[0]
        ds = objstore.get_datasource(uuid=uid)

        ds._store.delete_all()

    with pytest.raises(ObjectStoreError):
        integrity_check()


# TODO - expand these integrity tests
