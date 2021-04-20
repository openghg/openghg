import logging
from pathlib import Path
import pandas as pd
import pytest

from openghg.modules import GCWERKS as GC

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def get_datapath(filename):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/GC/{filename}")


@pytest.fixture(scope="session")
def cgo_path():
    return get_datapath(filename="capegrim-medusa.18.C")


@pytest.fixture(scope="session")
def cgo_prec_path():
    return get_datapath(filename="capegrim-medusa.18.precisions.C")


@pytest.fixture(scope="session")
def data_thd():
    return get_datapath(filename="trinidadhead.01.C")


@pytest.fixture(scope="session")
def prec_thd():
    return get_datapath(filename="trinidadhead.01.precisions.C")


def test_read_file_capegrim(cgo_path, cgo_prec_path):
    gc = GC()

    gas_data = gc.read_file(
        data_filepath=cgo_path,
        precision_filepath=cgo_prec_path,
        site="CGO",
        instrument="medusa",
        network="AGAGE",
    )

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
    gc = GC()

    gas_data = gc.read_file(
        data_filepath=data_thd,
        precision_filepath=prec_thd,
        site="thd",
        network="AGAGE",
    )

    expected_keys = ["ccl4_10m", "cfc113_10m", "cfc11_10m", "cfc12_10m", "ch3ccl3_10m", "ch4_10m", "chcl3_10m", "n2o_10m"]

    assert sorted(list(gas_data.keys())) == expected_keys

    expected_metadata = {
        "instrument": "GCMD",
        "site": "thd",
        "network": "AGAGE",
        "species": "ch3ccl3",
        "units": "ppt",
        "scale": "SIO-05",
        "inlet": "10m",
    }

    assert gas_data["ch3ccl3_10m"]["metadata"] == expected_metadata

    meas_data = gas_data["ch3ccl3_10m"]["data"]

    assert meas_data.time[0] == pd.Timestamp("2001-01-01T01:05:22.5")
    assert meas_data.time[-1] == pd.Timestamp("2001-12-31T23:18:22.5")

    assert meas_data["ch3ccl3"][0] == 41.537
    assert meas_data["ch3ccl3"][-1] == 34.649


def test_read_file_incorrect_inlet_raises(cgo_prec_path):
    data_path = get_datapath("capegrim-incorrect-inlet.18.C")

    gc = GC()

    with pytest.raises(ValueError):
        gc.read_file(data_filepath=data_path, precision_filepath=cgo_prec_path, site="CGO", instrument="medusa", network="AGAGE")


def test_read_invalid_instrument_raises(data_thd, prec_thd):
    gc = GC()

    with pytest.raises(ValueError):
        gc.read_file(data_filepath=data_thd, precision_filepath=prec_thd, site="CGO", instrument="fish", network="AGAGE")


def test_instrument_translator_works():
    gc = GC()

    instrument_suffix = "md"

    instrument_name = gc.instrument_translator(instrument=instrument_suffix)

    assert instrument_name == "GCMD"

    instrument_suffix = "gcms"

    instrument_name = gc.instrument_translator(instrument=instrument_suffix)

    assert instrument_name == "GCMS"

    instrument_suffix = "medusa"

    instrument_name = gc.instrument_translator(instrument=instrument_suffix)

    assert instrument_name == "medusa"

    instrument_suffix = "medusa21"

    instrument_name = gc.instrument_translator(instrument=instrument_suffix)

    assert instrument_name == "medusa"


def test_instrument_translator_raises():
    with pytest.raises(KeyError):
        gc = GC()
        instrument_suffix = "spam"
        gc.instrument_translator(instrument=instrument_suffix)


def test_read_data(cgo_path, cgo_prec_path):
    # Capegrim
    site = "CGO"
    instrument = "GCMD"

    gc = GC()
    data = gc.read_data(
        data_filepath=cgo_path, precision_filepath=cgo_prec_path, site=site, instrument=instrument, network="AGAGE"
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
    gc = GC()

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
    missing_species_prec = get_datapath(filename="capegrim-medusa.18.precisions.broke.C")

    gc = GC()

    with pytest.raises(ValueError):
        gc.read_file(data_filepath=cgo_path, precision_filepath=missing_species_prec, site="cgo", network="AGAGE")
