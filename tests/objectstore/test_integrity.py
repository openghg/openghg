import pytest
import tinydb
from helpers import (
    clear_test_stores,
    get_column_datapath,
    get_emissions_datapath,
    get_footprint_datapath,
    get_surface_datapath,
)
from openghg.objectstore import delete_object, get_writable_bucket, integrity_check
from openghg.standardise import (
    standardise_column,
    standardise_flux,
    standardise_footprint,
    standardise_surface,
)
from openghg.store import Emissions, Footprints
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
        high_spatial_res=True,
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
    with Emissions(bucket=bucket) as em:
        uid = em.datasources()[0]
        ds = Datasource.load(bucket=bucket, uuid=uid)
        keys = ds.data_keys()
        for key in keys:
            delete_object(bucket=bucket, key=key)

    with pytest.raises(ObjectStoreError):
        integrity_check()


def test_integrity_delete_uuids_metastore():
    integrity_check()

    bucket = get_writable_bucket(name="user")
    with Footprints(bucket=bucket) as fp:
        uids = fp.datasources()[:4]
        for u in uids:
            fp._metastore.remove(tinydb.where("uuid") == u)

    with pytest.raises(ObjectStoreError):
        integrity_check()
