import logging
from pathlib import Path
import pandas as pd
import pytest

from openghg.process.surface import GCWERKS
from helpers import get_datapath

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def cgo_path():
    return get_datapath(filename="capegrim-medusa.18.C", data_type="GC")


@pytest.fixture(scope="session")
def cgo_prec_path():
    return get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")


@pytest.fixture(scope="session")
def data_thd():
    return get_datapath(filename="trinidadhead.01.C", data_type="GC")


@pytest.fixture(scope="session")
def prec_thd():
    return get_datapath(filename="trinidadhead.01.precisions.C", data_type="GC")


def test_read_file_capegrim(cgo_path, cgo_prec_path):
    gc = GCWERKS()

    gas_data = gc.read_file(
        data_filepath=cgo_path,
        precision_filepath=cgo_prec_path,
        site="CGO",
        instrument="medusa",
        network="agage",
    )

    return False

    expected_eight = [
        "benzene_70m",
        "c4f10_70m",
        "c6f14_70m",
        "ccl4_70m",
        "cf4_70m",
        "cfc112_70m",
        "cfc113_70m",
        "cfc114_70m",
    ]

    sorted_keys = sorted(list(gas_data.keys()))

    assert sorted_keys[:8] == expected_eight

    assert len(sorted_keys) == 56


def test_read_file_thd(data_thd, prec_thd):
    gc = GCWERKS()

    gas_data = gc.read_file(
        data_filepath=data_thd,
        precision_filepath=prec_thd,
        site="thd",
        network="agage",
        instrument="gcmd"
    )

    expected_keys = ["ccl4_10m", "cfc113_10m", "cfc11_10m", "cfc12_10m", "ch3ccl3_10m", "ch4_10m", "chcl3_10m", "n2o_10m"]

    assert sorted(list(gas_data.keys())) == expected_keys

    expected_metadata = {
        "instrument": "gcmd",
        "site": "thd",
        "network": "agage",
        "species": "ch3ccl3",
        "units": "ppt",
        "scale": "SIO-05",
        "inlet": "10m",
        "sampling_period": "75"
    }

    assert gas_data["ch3ccl3_10m"]["metadata"] == expected_metadata

    meas_data = gas_data["ch3ccl3_10m"]["data"]

    assert meas_data.time[0] == pd.Timestamp("2001-01-01T01:05:22.5")
    assert meas_data.time[-1] == pd.Timestamp("2001-12-31T23:18:22.5")

    assert meas_data["ch3ccl3"][0] == 41.537
    assert meas_data["ch3ccl3"][-1] == 34.649


def test_read_invalid_instrument_raises(data_thd, prec_thd):
    gc = GCWERKS()

    with pytest.raises(ValueError):
        gc.read_file(data_filepath=data_thd, precision_filepath=prec_thd, site="CGO", instrument="fish", network="agage")


def test_read_data(cgo_path, cgo_prec_path):
    # Capegrim
    site = "CGO"
    instrument = "gcmd"

    gc = GCWERKS()
    data = gc.read_data(
        data_filepath=cgo_path, precision_filepath=cgo_prec_path, site=site, instrument=instrument, network="agage"
    )

    propane_data = data["propane_70m"]["data"]

    head_data = propane_data.head(1)
    tail_data = propane_data.tail(1)

    assert head_data.time[0] == pd.Timestamp("2018-01-01 02:33:22.500")
    assert head_data["propane"][0] == pytest.approx(5.458)
    assert head_data["propane repeatability"][0] == 0.22325

    assert tail_data.time[0] == pd.Timestamp("2018-01-31 23:42:22.500")
    assert tail_data["propane"][0] == 4.136
    assert tail_data["propane repeatability"][0] == 0.16027

    species = sorted(list(data.keys()))

    expected_species = ["benzene_70m", "c4f10_70m", "c6f14_70m", "ccl4_70m", "cf4_70m", "cfc112_70m", "cfc113_70m", "cfc114_70m"]

    assert species[:8] == expected_species

    attributes = {
        "data_owner": "Paul Krummel",
        "data_owner_email": "paul.krummel@csiro.au",
        "inlet_height_magl": "70m",
        "comment": "Gas chromatograph measurements. Output from GCWerks.",
    }

    assert data["nf3_70m"]["attributes"] == attributes


def test_read_precision(cgo_prec_path):
    gc = GCWERKS()

    precision, precision_series = gc.read_precision(cgo_prec_path)

    prec_test = ["NF3", "CF4", "PFC-116", "PFC-218", "PFC-318", "C4F10", "C6F14", "SF6"]
    end_prec_test = [
        "ethene",
        "ethane",
        "propane",
        "c-propane",
        "benzene",
        "toluene",
        "COS",
        "desflurane",
    ]

    assert precision_series[:8] == prec_test
    assert precision_series[-8:] == end_prec_test

    precision_head = precision.head(1)

    assert precision_head.iloc[0, 0] == 0.02531
    assert precision_head.iloc[0, 2] == 0.08338
    assert precision_head.iloc[0, 5] == 10
    assert precision_head.iloc[0, 7] == 10
    assert precision_head.iloc[0, 10] == 0.00565


def test_no_precisions_species_raises(cgo_path):
    missing_species_prec = get_datapath(filename="capegrim-medusa.18.precisions.broke.C", data_type="GC")

    gc = GCWERKS()

    with pytest.raises(ValueError):
        gc.read_file(data_filepath=cgo_path, precision_filepath=missing_species_prec, site="cgo", network="agage")


def test_read_ridgehill_window_inlet_all_NaNs():
    data_path = get_datapath(filename="ridgehill-md.11.C", data_type="GC")
    prec_path = get_datapath(filename="ridgehill-md.11.precisions.C", data_type="GC")

    gc = GCWERKS()
    res = gc.read_file(data_filepath=data_path, precision_filepath=prec_path, site="RGL", instrument="gcmd", network="agage")

    assert not res


def test_read_thd_window_inlet():
    data_path = get_datapath(filename="trinidadhead.01.window-inlet.C", data_type="GC")
    prec_path = get_datapath(filename="trinidadhead.01.precisions.C", data_type="GC")

    gc = GCWERKS()
    res = gc.read_file(data_filepath=data_path, precision_filepath=prec_path, site="thd", instrument="gcmd", network="agage")

    expected_metadata = {
        "instrument": "gcmd",
        "site": "thd",
        "network": "agage",
        "species": "ch4",
        "units": "ppb",
        "scale": "Tohoku",
        "inlet": "10m",
        "sampling_period": "75"
    }

    metadata = res["ch4_10m"]["metadata"]

    assert metadata == expected_metadata

    data = res["ch4_10m"]["data"]

    assert data.time[0] == pd.Timestamp("2001-01-01T01:05:22.5")
    assert data.time[-1] == pd.Timestamp("2001-01-01T10:25:22.5")
    assert data["ch4"][0] == pytest.approx(1818.62)
    assert data["ch4"][-1] == pytest.approx(1840.432)


def test_read_shangdianzi_ASM_inlet():
    data_path = get_datapath(filename="shangdianzi-medusa.18.C", data_type="GC")
    prec_path = get_datapath(filename="shangdianzi-medusa.18.precisions.C", data_type="GC")

    gc = GCWERKS()
    res = gc.read_file(data_filepath=data_path, precision_filepath=prec_path, site="sdz", instrument="medusa", network="agage")

    expected_metadata = {
        "instrument": "medusa",
        "site": "sdz",
        "network": "agage",
        "species": "nf3",
        "units": "ppt",
        "scale": "SIO-12",
        "inlet": "80m",
        "sampling_period": "1200",
    }

    metadata = res["nf3_80m"]["metadata"]

    assert metadata == expected_metadata

    data = res["nf3_80m"]["data"]

    data.time[0] == pd.Timestamp("2018-01-16T09:10:00")
    data.time[-1] == pd.Timestamp("2018-01-16T20:00:00")
    data["nf3"][0] == pytest.approx(2.172)
    data["nf3"][-1] == pytest.approx(2.061)

    # expected_metadata = {
    #     "instrument": "gcmd",
    #     "site": "thd",
    #     "network": "agage",
    #     "species": "ch4",
    #     "units": "ppb",
    #     "scale": "Tohoku",
    #     "inlet": "10m",
    # }

    # metadata = res["ch4_10m"]["metadata"]

    # assert metadata == expected_metadata

    # data = res["ch4_10m"]["data"]

    # assert data.time[0] == pd.Timestamp("2001-01-01T01:05:22.5")
    # assert data.time[-1] == pd.Timestamp("2001-01-01T10:25:22.5")
    # assert data["ch4"][0] == pytest.approx(1818.62)
    # assert data["ch4"][-1] == pytest.approx(1840.432)


def test_check_instrument():
    gc = GCWERKS()

    filename = Path("zeppelin-medusa.18.C")

    instrument = gc.check_instrument(filepath=filename)

    assert instrument == "medusa"

    filename = Path("macehead-gcms.98.C")

    instrument = gc.check_instrument(filepath=filename)

    assert instrument == "gcms"

    filename = Path("india-md.13.C")

    instrument = gc.check_instrument(filepath=filename)

    assert instrument == "gcmd"


def test_check_instrument_raises_correctly():
    gc = GCWERKS()

    filename = Path("zeppelin.18.C")

    instrument = gc.check_instrument(filepath=filename, should_raise=False)

    assert instrument is None

    with pytest.raises(KeyError):
        gc.check_instrument(filepath=filename, should_raise=True)


def test_check_site():
    gc = GCWERKS()

    filename = Path("zeppelin.18.C")

    site = gc.check_site(filepath=filename, site_code="zep")

    assert site == "zep"

    site = gc.check_site(filepath=filename, site_code="ZEP")

    assert site == "zep"

    with pytest.raises(ValueError):
        site = gc.check_site(filepath=filename, site_code="AAA")

    filename = Path("parrot.18.C")

    with pytest.raises(ValueError):
        site = gc.check_site(filepath=filename, site_code="ZEP")
