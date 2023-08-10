import pytest
from openghg.objectstore import integrity_check
from openghg.standardise import (
    standardise_surface,
    standardise_flux,
    standardise_column,
    standardise_footprint,
)
from openghg.objectstore import get_writable_bucket, delete_object
from openghg.types import ObjectStoreError
from helpers import (
    get_surface_datapath,
    get_column_datapath,
    get_footprint_datapath,
    get_emissions_datapath,
    clear_test_stores,
)
from openghg.store import Footprints, Emissions
from openghg.store.base import Datasource
from openghg.store._connection import get_object_store_connection
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
    with get_object_store_connection(data_type="emissions", bucket=bucket) as em:
        uid = em._datasources()[0]
        ds = Datasource.load(bucket=bucket, uuid=uid)
        keys = ds.data_keys()
        for key in keys:
            delete_object(bucket=bucket, key=key)

    with pytest.raises(ObjectStoreError):
        integrity_check()


def test_integrity_delete_uuids_metastore():
    from openghg.store._connection import get_object_store_connection

    integrity_check()

    bucket = get_writable_bucket(name="user")
    with get_object_store_connection("footprints", bucket=bucket) as fp:
        uids = list(fp._datasources())[:4]  # TODO should there be a property to expose this attribute?
        for u in uids:
            fp._metastore.remove(tinydb.where("uuid") == u)

    with pytest.raises(ObjectStoreError):
        integrity_check()
