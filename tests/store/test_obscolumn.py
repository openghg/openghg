import numpy as np
import pytest
from helpers import get_column_datapath, clear_test_store, filt
from openghg.objectstore import get_bucket
from openghg.retrieve import search_column
from openghg.standardise import standardise_column
from openghg.objectstore import get_datasource
from pandas import Timestamp


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

    results = filt(results, species="ch4")
    assert results  # results with species ch4 exist
    uuid = results[0]["uuid"]

    bucket = get_bucket()

    d = get_datasource(bucket=bucket, uuid=uuid)

    with d.get_data(version="latest") as ch4_data:
        assert ch4_data.time[0] == Timestamp("2017-03-18T15:32:54")
        assert np.isclose(ch4_data["xch4"][0], 1844.2019)


def test_read_tccon_format():
    """
    Test that files in the TCCON format can be correctly parsed
    and are able to pass the internal format schema checks.
    """
    filename = "hw20230402_20230402.public.qc.nc"
    datafile = get_column_datapath(filename=filename)

    site = "THW"
    species = "ch4"
    pressure_weights_method = "pressure_weight"

    bucket = get_bucket()
    results = standardise_column(
        store="user",
        filepath=datafile,
        source_format="TCCON",
        site=site,
        species=species,
        pressure_weights_method=pressure_weights_method,
    )

    results = filt(results, species="ch4")
    assert results  # results with species ch4 exist
    uuid = results[0]["uuid"]

    bucket = get_bucket()

    d = get_datasource(bucket=bucket, uuid=uuid)

    with d.get_data(version="latest") as ch4_data:
        assert ch4_data.time[0] == Timestamp("2023-04-02T15:00:00")
        assert np.isclose(ch4_data["xch4"][0], 1888.025)


def test_info_metadata_raise_error():
    """
    Test to verify required keys present in optional metadata supplied as dictionary raise ValueError
    """

    clear_test_store("user")
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
            info_metadata={"species": "ch4"},
        )


def test_info_metadata():
    """
    Test to verify required keys present in optional metadata supplied as dictionary is
    added to metadata
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
        info_metadata={"project": "openghg_test"},
    )
    col_data = search_column(species="ch4").retrieve_all()
    metadata = col_data.metadata

    assert "project" in metadata
