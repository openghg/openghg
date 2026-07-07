"""Regression tests for ICOS retrieval route compatibility."""

import numpy as np
import pandas as pd
import xarray as xr

from openghg.retrieve.icos import _retrieve


EXPECTED_CO2 = 410.0


def _minimal_attrs() -> dict:
    return {
        "measurement_unit": "ppm",
        "dataset_calibration_scale": "WMO X2019",
        "dataset_data_frequency": 1,
        "dataset_data_frequency_unit": "hour",
        "sampling_heights": "10 m",
        "altitude": "100 m",
        "station_name": "Regression station",
        "latitude": "1.2",
        "longitude": "2.3",
    }


def _minimal_data_info() -> dict:
    return {
        "dobj_uri": "https://meta.icos-cp.eu/objects/test-object",
        "species": "co2",
        "site": "AAA",
        "project_name": "icos",
    }


def test_create_icos_attributes_keeps_metadata_instrument_when_dataset_has_no_instrument(monkeypatch):
    """Text products should not need an instrument data variable to create metadata."""

    def fake_attributes_requiring_retrieval(*_args, **_kwargs):
        return {"instrument": "co2-ch4-co-h2o picarro analyzer"}

    monkeypatch.setattr(
        _retrieve,
        "attributes_requiring_retrieval",
        fake_attributes_requiring_retrieval,
    )
    dataset = xr.Dataset(
        data_vars={
            "co2": ("time", [EXPECTED_CO2]),
            "co2 variability": ("time", [0.1]),
            "co2 number_of_observations": ("time", [12]),
            "flag": ("time", ["O"]),
        },
        coords={"time": pd.date_range("2020-01-01", periods=1)},
    )

    attributes = _retrieve.create_icos_attributes(
        dataset=dataset,
        data_info=_minimal_data_info(),
        data_attributes=_minimal_attrs(),
        species="co2",
        dataset_source="icos",
    )

    assert attributes["instrument"] == "co2-ch4-co-h2o picarro analyzer"


def test_create_icos_attributes_sets_single_instrument_id(monkeypatch):
    """A single NetCDF instrument ID should still be copied into attributes."""

    def fake_attributes_requiring_retrieval(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(
        _retrieve, "attributes_requiring_retrieval", fake_attributes_requiring_retrieval
    )
    dataset = xr.Dataset(
        data_vars={
            "co2": ("time", [EXPECTED_CO2, 411.0]),
            "flag": ("time", ["O", "O"]),
            "instrument": ("time", [7, 7]),
        },
        coords={"time": pd.date_range("2020-01-01", periods=2)},
    )

    attributes = _retrieve.create_icos_attributes(
        dataset=dataset,
        data_info=_minimal_data_info(),
        data_attributes=_minimal_attrs(),
        species="co2",
        dataset_source="icos",
    )

    assert attributes["instrument"] == "icos_id_7"


def test_retrieve_and_parse_icos_data_handles_flask_sampling_start_end(monkeypatch):
    """Flask text files should parse SamplingStart/SamplingEnd timestamps."""

    def fake_retrieve_dobj_format(_data_info):
        return "asciiAtcFlaskTimeSer"

    def fake_get_icos_text_file(_dobj_uri):
        return flask_text

    flask_text = "\n".join(
        [
            "# HEADER_LINES: 4",
            "# COMMENT",
            "# co2: mole fraction",
            "# site;SamplingStart;SamplingEnd;co2;Stdev;NbPoints;Flag",
            (
                "AAA;2020-01-01T00:00:00Z;2020-01-01T01:00:00Z;"
                f"{EXPECTED_CO2};0.1;12;O"
            ),
        ]
    )
    monkeypatch.setattr(
        _retrieve._data_parsing, "_retrieve_dobj_format", fake_retrieve_dobj_format
    )
    monkeypatch.setattr(
        _retrieve._data_parsing, "get_icos_text_file", fake_get_icos_text_file
    )

    dataset, _ = _retrieve.retrieve_and_parse_icos_data(
        _minimal_data_info(), dataset_source="EYE-AVE-PAR"
    )

    assert "co2" in dataset
    assert dataset.sizes["time"] == 1
    assert dataset["time"].values[0] == np.datetime64("2020-01-01T00:00:00")


def test_parse_icos_text_file_drops_sentinel_missing_values(monkeypatch):
    """ICOS sentinel values should not be stored as real observations."""

    def fake_get_icos_text_file(_dobj_uri):
        return text

    text = "\n".join(
        [
            "# HEADER_LINES: 4",
            "# COMMENT",
            "# co2: mole fraction",
            "# site;Year;Month;Day;Hour;Minute;co2;Stdev;NbPoints;Flag",
            "AAA;2020;1;1;0;0;-999.99;0.1;12;O",
            f"AAA;2020;1;1;1;0;{EXPECTED_CO2};0.2;12;O",
        ]
    )
    monkeypatch.setattr(
        _retrieve._data_parsing, "get_icos_text_file", fake_get_icos_text_file
    )

    dataset, _ = _retrieve.parse_icos_text_file(_minimal_data_info())

    assert dataset.sizes["time"] == 1
    assert dataset["co2"].values[0] == EXPECTED_CO2
