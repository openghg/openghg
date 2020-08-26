import datetime
import logging
from pathlib import Path
import pandas as pd
import pytest

from HUGS.Modules import GC

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
        source_name="capegrim_medusa",
        site="CGO",
        instrument_name="medusa",
    )

    expected_eight = ['C4F10', 'C6F14', 'CCl4', 'CF4', 'CFC-11', 'CFC-112', 'CFC-113', 'CFC-114']

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
            source_name="capegrim_medusa",
            site="CGO",
            instrument_name="medusa",
        )


def test_read_invalid_instrument_raises(
    data_path_no_instrument, precision_path_no_instrument
):
    gc = GC()

    with pytest.raises(ValueError):
        gc.read_file(
            data_filepath=data_path_no_instrument,
            precision_filepath=precision_path_no_instrument,
            source_name="capegrim_medusa",
            site="CGO",
            instrument_name="fish",
        )


def test_read_valid_instrument_passed(
    data_path_no_instrument, precision_path_no_instrument
):
    gc = GC()
    data = gc.read_file(
        data_filepath=data_path_no_instrument,
        precision_filepath=precision_path_no_instrument,
        source_name="capegrim_medusa",
        site="CGO",
        instrument_name="medusa",
    )

    assert sorted(list(data.keys())) == sorted(['CH4', 'CFC-12', 'N2O', 'CFC-11', 'CFC-113', 'CHCl3', 'CH3CCl3', 'CCl4'])


def test_read_unsure_instrument_type(
    data_path_no_instrument, precision_path_no_instrument
):
    gc = GC()

    with pytest.raises(ValueError):
        gc.read_file(
            data_filepath=data_path_no_instrument,
            precision_filepath=precision_path_no_instrument,
            source_name="capegrim_medusa",
            site="CGO",
        )


def test_read_data(data_path, precision_path):
    # Capegrim
    site = "CGO"
    instrument = "GCMD"

    gc = GC()
    data = gc.read_data(
        data_filepath=data_path,
        precision_filepath=precision_path,
        site=site,
        instrument=instrument,
    )

    propane_data = data["propane"]["data"]

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


# TODO - write a new test for this function
def test_split(data_path, precision_path):
    site = "CGO"
    instrument = "GCMD"

    data_path = (
        Path(__file__)
        .resolve()
        .parent.joinpath("../data/proc_test_data/GC/test_split_data.pkl")
    )

    # Load in the test data
    # TODO - investigate error here
    # df = pd.read_hdf(data_path)
    # Use pickle due to error in CI with hdf verion
    df = pd.read_pickle(data_path)

    species = [
        "NF3",
        "CF4",
        "PFC-116",
        "PFC-218",
        "PFC-318",
        "C4F10",
        "C6F14",
        "SF6",
        "SO2F2",
        "SF5CF3",
        "HFC-23",
        "HFC-32",
        "HFC-125",
        "HFC-134a",
        "HFC-143a",
        "HFC-152a",
        "HFC-227ea",
        "HFC-236fa",
        "HFC-245fa",
        "HFC-365mfc",
        "HFC-4310mee",
        "HCFC-22",
        "HCFC-123",
        "HCFC-124",
        "HCFC-132b",
        "HCFC-133a",
        "HCFC-141b",
        "HCFC-142b",
        "CFC-11",
        "CFC-12",
        "CFC-13",
        "CFC-112",
        "CFC-113",
        "CFC-114",
        "CFC-115",
        "H-1211",
        "H-1301",
        "H-2402",
        "CH3Cl",
        "CH3Br",
        "CH3I",
        "CH2Cl2",
        "CHCl3",
        "CCl4",
        "CH2Br2",
        "CHBr3",
        "CH3CCl3",
        "TCE",
        "PCE",
        "ethyne",
        "ethene",
        "ethane",
        "propane",
        "c-propane",
        "benzene",
        "toluene",
        "COS",
        "desflurane",
    ]

    metadata = {"foo": "bar"}

    gc = GC.load()

    units = {}
    scale = {}
    for s in species:
        units[s] = "test_units"
        scale[s] = "test_scale"

    data = gc.split_species(
        data=df,
        site=site,
        instrument=instrument,
        species=species,
        metadata=metadata,
        units=units,
        scale=scale,
    )

    sorted_species = [
        "C4F10",
        "C6F14",
        "CCl4",
        "CF4",
        "CFC-11",
        "CFC-112",
        "CFC-113",
        "CFC-114",
        "CFC-115",
        "CFC-12",
    ]

    assert len(data) == 56

    assert sorted(list(data.keys()))[:10] == sorted_species
