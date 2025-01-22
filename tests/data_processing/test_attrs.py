import pytest

import numpy as np
import xarray as xr

from openghg.data_processing._attrs import (
    _make_rename_dict,
    map_dict,
    map_dict_multi,
    rename,
    update_attrs,
)


def test_map_dict():
    to_update = {"a": "A", "b": "B"}

    result = map_dict(to_update, lambda x: x + "_suffix")

    assert result == {"a": "A_suffix", "b": "B_suffix"}


def test_map_dict_multi():
    funcs = [lambda x: "prefix_" + x, lambda x: x + "_suffix"]

    to_update = {"a": "A", "b": "B"}

    result = map_dict_multi(to_update, funcs)

    assert result == {"a": "prefix_A_suffix", "b": "prefix_B_suffix"}


def test_map_dict_multi_with_keys():
    funcs = [(lambda x: "prefix_" + x, ["a"]), (lambda x: x + "_suffix", ["b"])]

    to_update = {"a": "A", "b": "B"}

    result = map_dict_multi(to_update, funcs)

    assert result == {"a": "prefix_A", "b": "B_suffix"}


def test_make_rename_dict():
    to_rename = xr.Dataset({"flux_prior": xr.DataArray(), "flux_posterior": xr.DataArray()})

    rename_dict = _make_rename_dict(to_rename, lambda x: "mean_" + x)

    assert rename_dict == {"flux_prior": "mean_flux_prior", "flux_posterior": "mean_flux_posterior"}

    rename_dict = _make_rename_dict(to_rename, lambda x: x.replace("flux", "country"), lambda x: "mean_" + x)

    assert rename_dict == {"flux_prior": "mean_country_prior", "flux_posterior": "mean_country_posterior"}


@pytest.fixture()
def dataset():
    ds = xr.Dataset(
        data_vars={
            "ch4": (["time"], np.arange(24)),
            "ch4_number_of_observations": (["time"], np.arange(24)),
            "ch4_variability": (["time"], np.arange(24)),
        },
        coords={"time": ("time", np.array([f"2020-01-01T{h:0>2}:00:00" for h in range(24)], dtype="datetime64"))},
        attrs={
            "Conventions": "CF-1.8",
            "comment": "Cavity ring-down measurements. Output from GCWerks",
            "conditions_of_use": "Ensure that you contact the data owner at the outset of your project.",
            "data_owner": "Simon O'Doherty",
            "data_owner_email": "s.odoherty@bristol.ac.uk",
            "data_source": "internal",
            "data_type": "surface",
            "file_created": "2024-11-13 14:45:42.460999+00:00",
            "inlet": "185m",
            "inlet_height_magl": "185",
            "instrument": "picarro",
            "long_name": "tacolneston",
            "network": "decc",
            "port": "10",
            "processed_by": "OpenGHG_Cloud",
            "sampling_period": "60.0",
            "sampling_period_unit": "s",
            "scale": "wmo-x2004a",
            "site": "tac",
            "source": "In situ measurements of air",
            "source_format": "OPENGHG",
            "species": "ch4",
            "station_height_masl": 64,
            "station_latitude": "52.51811",
            "station_long_name": "Tacolneston Tower, UK",
            "station_longitude": "1.13847",
            "type": "air",
        },
    )
    ds["ch4"].attrs = {'long_name': 'mole_fraction_of_methane_in_air', 'units': '1e-9'}
    ds["ch4_number_of_observations"].attrs = {'long_name': 'mole_fraction_of_methane_in_air_number_of_observations'}
    ds["ch4_variability"].attrs = {'long_name': 'mole_fraction_of_methane_in_air_variability', 'units': '1e-9'}

    return ds


def test_rename_dataset(dataset):
    renamed_ds = rename(dataset, lambda x: x.replace("ch4", "mf"))

    assert sorted(renamed_ds.data_vars) == ["mf", "mf_number_of_observations", "mf_variability"]

    assert renamed_ds.attrs == dataset.attrs

    for dv in renamed_ds.data_vars:
        assert renamed_ds[dv].attrs == dataset[str(dv).replace("mf", "ch4")].attrs


def test_update_attrs(dataset):
    ds = dataset[["ch4"]].resample(time="4h").std()

    suffix_spec = (lambda x: x + "_variability", ["long_name"])
    ds = update_attrs(ds, suffix_spec)

    assert ds.ch4.attrs["long_name"] == "mole_fraction_of_methane_in_air_variability"
    assert ds.ch4.attrs["units"] == "1e-9"
