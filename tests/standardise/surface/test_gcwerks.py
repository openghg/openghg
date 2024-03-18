import logging

import pandas as pd
import pytest
from helpers import check_cf_compliance, get_surface_datapath, parsed_surface_metachecker
from openghg.standardise.surface import parse_gcwerks

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def thd_data():
    thd_path = get_surface_datapath(filename="trinidadhead.01.C", source_format="GC")
    thd_prec_path = get_surface_datapath(filename="trinidadhead.01.precisions.C", source_format="GC")

    gas_data = parse_gcwerks(
        data_filepath=thd_path,
        precision_filepath=thd_prec_path,
        site="THD",
        instrument="gcmd",
        network="agage",
    )

    return gas_data


@pytest.fixture(scope="session")
def cgo_data():
    cgo_data = get_surface_datapath(filename="capegrim-medusa.18.C", source_format="GC")
    cgo_prec = get_surface_datapath(filename="capegrim-medusa.18.precisions.C", source_format="GC")

    gas_data = parse_gcwerks(
        data_filepath=cgo_data,
        precision_filepath=cgo_prec,
        site="cgo",
        instrument="medusa",
        network="agage",
    )

    return gas_data


def test_read_file_capegrim(cgo_data):
    parsed_surface_metachecker(data=cgo_data)

    # 30/11/2021: Species labels were updated to be standardised in line with variable naming
    # This list of expected labels was updated.
    expected_eight = [
        "c2cl4_70m",
        "c2f6_70m",
        "c2h2_70m",
        "c2h6_70m",
        "c2hcl3_70m",
        "c3f8_70m",
        "c3h8_70m",
        "c4f10_70m",
    ]

    sorted_keys = sorted(list(cgo_data.keys()))

    assert sorted_keys[:8] == expected_eight

    assert len(sorted_keys) == 56


def test_read_file_thd():
    thd_path = get_surface_datapath(filename="trinidadhead.01.C", source_format="GC")
    thd_prec_path = get_surface_datapath(filename="trinidadhead.01.precisions.C", source_format="GC")

    gas_data = parse_gcwerks(
        data_filepath=thd_path,
        precision_filepath=thd_prec_path,
        site="thd",
        network="agage",
        instrument="gcmd",
        sampling_period="1",  # Checking this can be compared successfully
    )

    parsed_surface_metachecker(data=gas_data)

    expected_keys = [
        "ccl4_10m",
        "cfc113_10m",
        "cfc11_10m",
        "cfc12_10m",
        "ch3ccl3_10m",
        "ch4_10m",
        "chcl3_10m",
        "n2o_10m",
    ]

    assert sorted(list(gas_data.keys())) == expected_keys

    meas_data = gas_data["ch3ccl3_10m"]["data"]

    assert meas_data.time[0] == pd.Timestamp("2001-01-01T01:05:59.5")
    assert meas_data.time[-1] == pd.Timestamp("2001-12-31T23:18:59.5")

    assert meas_data["ch3ccl3"][0] == 41.537
    assert meas_data["ch3ccl3"][-1] == 34.649


@pytest.mark.skip_if_no_cfchecker
@pytest.mark.cfchecks
def test_gc_thd_cf_compliance(thd_data):
    meas_data = thd_data["ch3ccl3_10m"]["data"]
    assert check_cf_compliance(dataset=meas_data)


def test_read_invalid_instrument_raises():
    thd_path = get_surface_datapath(filename="trinidadhead.01.C", source_format="GC")
    thd_prec_path = get_surface_datapath(filename="trinidadhead.01.precisions.C", source_format="GC")

    with pytest.raises(ValueError):
        parse_gcwerks(
            data_filepath=thd_path,
            precision_filepath=thd_prec_path,
            site="CGO",
            instrument="fish",
            network="agage",
        )


def test_no_precisions_species_raises():
    cgo_path = get_surface_datapath(filename="capegrim-medusa.18.C", source_format="GC")
    missing_species_prec = get_surface_datapath(
        filename="capegrim-medusa.18.precisions.broke.C", source_format="GC"
    )

    with pytest.raises(ValueError):
        parse_gcwerks(
            data_filepath=cgo_path, precision_filepath=missing_species_prec, site="cgo", network="agage"
        )


def test_read_ridgehill_window_inlet_all_NaNs():
    data_path = get_surface_datapath(filename="ridgehill-md.11.C", source_format="GC")
    prec_path = get_surface_datapath(filename="ridgehill-md.11.precisions.C", source_format="GC")

    res = parse_gcwerks(
        data_filepath=data_path, precision_filepath=prec_path, site="RGL", instrument="gcmd", network="agage"
    )

    assert not res


def test_read_thd_window_inlet():
    data_path = get_surface_datapath(filename="trinidadhead.01.window-inlet.C", source_format="GC")
    prec_path = get_surface_datapath(filename="trinidadhead.01.precisions.C", source_format="GC")

    res = parse_gcwerks(
        data_filepath=data_path, precision_filepath=prec_path, site="thd", instrument="gcmd", network="agage"
    )

    parsed_surface_metachecker(data=res)

    data = res["ch4_10m"]["data"]

    assert data.time[0] == pd.Timestamp("2001-01-01T01:05:59.5")
    assert data.time[-1] == pd.Timestamp("2001-01-01T10:25:59.5")
    assert data["ch4"][0] == pytest.approx(1818.62)
    assert data["ch4"][-1] == pytest.approx(1840.432)


@pytest.mark.skip_if_no_cfchecker
@pytest.mark.cfchecks
def test_thd_cf_compliance(thd_data):
    meas_data = thd_data["ch4_10m"]["data"]
    assert check_cf_compliance(dataset=meas_data)


def test_read_shangdianzi_ASM_inlet():
    data_path = get_surface_datapath(filename="shangdianzi-medusa.18.C", source_format="GC")
    prec_path = get_surface_datapath(filename="shangdianzi-medusa.18.precisions.C", source_format="GC")

    res = parse_gcwerks(
        data_filepath=data_path,
        precision_filepath=prec_path,
        site="sdz",
        instrument="medusa",
        network="agage",
    )

    parsed_surface_metachecker(data=res)

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

    # assert data.time[0] == pd.Timestamp("2001-01-01T01:05:59.5")
    # assert data.time[-1] == pd.Timestamp("2001-01-01T10:25:59.5")
    # assert data["ch4"][0] == pytest.approx(1818.62)
    # assert data["ch4"][-1] == pytest.approx(1840.432)
