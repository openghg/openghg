import numpy as np
import pytest
from helpers import get_column_datapath, clear_test_stores
from openghg.objectstore import get_bucket
from openghg.retrieve import search_column
from openghg.standardise import standardise_column
from openghg.store.base import Datasource
from pandas import Timestamp

@pytest.fixture
def clear_store():
    clear_test_stores()

def test_read_openghg_format():
    """
    Test that files already in OpenGHG format can be read. This file includes:
     - appropriate variable names and types
     - attributes which can be understood and intepreted
    """
    filename = "gosat-fts_gosat_20170318_ch4-column.nc"
    datafile = get_column_datapath(filename=filename)

    satellite = "GOSAT"
    domain = "BRAZIL"
    species = "methane"

    bucket = get_bucket()
    results = standardise_column(
        store="user",
        filepath=datafile,
        source_format="OPENGHG",
        satellite=satellite,
        domain=domain,
        species=species,
    )

    # Output style from ObsSurface - may want to use for ObsColumn as well
    # uuid = results["processed"][filename]["ch4"]["uuid"]

    # Output style for other object types
    assert "ch4" in results
    uuid = results["ch4"]["uuid"]

    bucket = get_bucket()

    d = Datasource(bucket=bucket, uuid=uuid)

    with d.get_data(version="latest") as ch4_data:
        assert ch4_data.time[0] == Timestamp("2017-03-18T15:32:54")
        assert np.isclose(ch4_data["xch4"][0], 1844.2019)


def test_optional_metadata_raise_error(clear_store):
    """
    Test to verify required keys present in optional metadata supplied as dictionary raise ValueError
    """
    filename = "gosat-fts_gosat_20170318_ch4-column.nc"
    datafile = get_column_datapath(filename=filename)

    satellite = "GOSAT"
    domain = "BRAZIL"
    species = "methane"

    with pytest.raises(ValueError):
        standardise_column(
            store="user",
            filepath=datafile,
            source_format="OPENGHG",
            satellite=satellite,
            domain=domain,
            species=species,
            optional_metadata={"domain":"openghg_test"}
     )


def test_optional_metadata():
    """
    Test to verify required keys present in optional metadata supplied as dictionary raise ValueError
    """
    filename = "gosat-fts_gosat_20170318_ch4-column.nc"
    datafile = get_column_datapath(filename=filename)

    satellite = "GOSAT"
    domain = "BRAZIL"
    species = "methane"

    standardise_column(
        store="user",
        filepath=datafile,
        source_format="OPENGHG",
        satellite=satellite,
        domain=domain,
        species=species,
        optional_metadata={"project":"openghg_test"}
    )
    col_data = search_column(species="ch4").retrieve_all()
    metadata = col_data.metadata

    assert "project" in metadata
