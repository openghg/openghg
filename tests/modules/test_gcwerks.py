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
def data_path():
    return get_datapath(filename="capegrim-medusa.18.C")


@pytest.fixture(scope="session")
def precision_path():
    return get_datapath(filename="capegrim-medusa.18.precisions.C")


@pytest.fixture(scope="session")
def data_path_no_instrument():
    return get_datapath(filename="trinidadhead.01.C")


@pytest.fixture(scope="session")
def precision_path_no_instrument():
    return get_datapath(filename="trinidadhead.01.precisions.C")


def test_read_file(data_path, precision_path):
    gc = GC()

    gas_data = gc.read_file(
        data_filepath=data_path,
        precision_filepath=precision_path,
        site="CGO",
        instrument="medusa",
        network="AGAGE",
    )

    return False

    expected_eight = [
        "benzene_75m_4",
        "c4f10_75m_4",
        "c6f14_75m_4",
        "ccl4_75m_4",
        "cf4_75m_4",
        "cfc112_75m_4",
        "cfc113_75m_4",
        "cfc114_75m_4",
    ]

    sorted_keys = sorted(list(gas_data.keys()))

    assert sorted_keys[:8] == expected_eight

    assert len(sorted_keys) == 56


def test_read_file_incorrect_inlet_raises(precision_path):
    data_path = Path(__file__).resolve().parent.joinpath("../data/proc_test_data/GC/capegrim-incorrect-inlet.18.C")

    gc = GC()

    with pytest.raises(ValueError):
        gc.read_file(
            data_filepath=data_path,
            precision_filepath=precision_path,
            site="CGO",
            instrument="medusa",
        )


def test_read_invalid_instrument_raises(data_path_no_instrument, precision_path_no_instrument):
    gc = GC()

    with pytest.raises(ValueError):
        gc.read_file(
            data_filepath=data_path_no_instrument,
            precision_filepath=precision_path_no_instrument,
            site="CGO",
            instrument="fish",
        )


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


def test_read_data(data_path, precision_path):
    # Capegrim
    site = "CGO"
    instrument = "GCMD"

    gc = GC()
    data = gc.read_data(
        data_filepath=data_path, precision_filepath=precision_path, site=site, instrument=instrument, network="AGAGE"
    )

    return False

    propane_data = data["propane_75m_4"]["data"]

    head_data = propane_data.head(1)
    tail_data = propane_data.tail(1)

    assert head_data.time[0] == pd.Timestamp("2018-01-01 02:33:22.500")
    assert head_data["propane"][0] == pytest.approx(5.458)
    assert head_data["propane repeatability"][0] == 0.22325

    assert tail_data.time[0] == pd.Timestamp("2018-01-31 23:42:22.500")
    assert tail_data["propane"][0] == 4.136
    assert tail_data["propane repeatability"][0] == 0.16027

    species = list(data.keys())

    assert species[:8] == [
        "NF3",
        "CF4",
        "PFC-116",
        "PFC-218",
        "PFC-318",
        "C4F10",
        "C6F14",
        "SF6",
    ]

    attributes = {
        "data_owner": "Paul Krummel",
        "data_owner_email": "paul.krummel@csiro.au",
        "inlet_height_magl": "75m_4",
        "comment": "Gas chromatograph measurements. Output from GCWerks.",
    }

    assert data["NF3"]["attributes"] == attributes


def test_read_precision(precision_path):
    gc = GC()

    precision, precision_series = gc.read_precision(precision_path)

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


def test_no_precisions_species_raises(data_path):
    missing_species_prec = get_datapath(filename="capegrim-medusa.18.precisions.broke.C")

    gc = GC()

    with pytest.raises(ValueError):
        gc.read_file(data_filepath=data_path, precision_filepath=missing_species_prec)
