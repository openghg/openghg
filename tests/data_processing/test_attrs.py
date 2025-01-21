import pytest

import numpy as np
import xarray as xr

from openghg.data_processing._attrs import (
    add_prefix,
    add_suffix,
    replace,
    str_method,
    UpdateSpec,
    _make_update_dict,
    _make_rename_dict,
    rename,
    _update_attrs,
)


def test_update_spec():
    prefix_spec = UpdateSpec(add_prefix, "my_prefix")

    assert prefix_spec("abc") == "my_prefix_abc"

    prefix_spec2 = UpdateSpec(add_prefix, "my prefix", sep=" ")

    assert prefix_spec2("abc") == "my prefix abc"

    # UpdateSpec with incomplete arguments
    with pytest.raises(ValueError):
        UpdateSpec(add_prefix)

    # function without positional only arg
    def bad_fn1(value: str, option: str):
        pass

    with pytest.raises(ValueError):
        UpdateSpec(bad_fn1, "option_value")

    # function that doesn't take strings
    def bad_fn2(value: int, /, option: str):
        pass

    with pytest.raises(ValueError):
        UpdateSpec(bad_fn2, "option_value")


def test_make_update_dict():
    prefix_spec = UpdateSpec(add_prefix, "my_prefix")
    suffix_spec = UpdateSpec(add_suffix, "my_suffix")

    specs = [prefix_spec, suffix_spec]

    to_update = {"a": "A", "b": "B"}

    update_dict = _make_update_dict(specs, to_update)

    assert update_dict == {"a": "my_prefix_A_my_suffix", "b": "my_prefix_B_my_suffix"}


def test_make_update_dict_with_keys():
    prefix_spec = UpdateSpec(add_prefix, "my_prefix", keys=["a"])
    suffix_spec = UpdateSpec(add_suffix, "my_suffix", keys=["b"])

    specs = [prefix_spec, suffix_spec]

    to_update = {"a": "A", "b": "B"}

    update_dict = _make_update_dict(specs, to_update)

    assert update_dict == {"a": "my_prefix_A", "b": "B_my_suffix"}

    # test that "c" will not be updated by this specification
    to_update = {"a": "A", "b": "B", "c": "C"}

    update_dict = _make_update_dict(specs, to_update)

    assert update_dict == {"a": "my_prefix_A", "b": "B_my_suffix"}


def test_make_rename_dict():
    to_rename = ["flux_prior", "flux_posterior"]

    prefix_spec = UpdateSpec(add_prefix, "mean")

    rename_dict = _make_rename_dict(prefix_spec, to_rename)

    assert rename_dict == {"flux_prior": "mean_flux_prior", "flux_posterior": "mean_flux_posterior"}

    replace_spec = UpdateSpec(replace, old="flux", new="country")

    rename_dict = _make_rename_dict([prefix_spec, replace_spec], to_rename)

    assert rename_dict == {"flux_prior": "mean_country_prior", "flux_posterior": "mean_country_posterior"}


def test_generic_str_method():
    """
    Test `str_method` function, which can be used to apply any Python string
    method in a `UpdateSpec` object.
    """
    assert str_method("abc", "upper") == "ABC"
    assert str_method("  asdf  ", "strip") == "asdf"
    assert str_method("==asdf==", "strip", "=") == "asdf"

    upper_spec = UpdateSpec(str_method, "upper")
    strip_spec = UpdateSpec(str_method, "strip")
    strip_equals_spec = UpdateSpec(str_method, "strip", "=")

    assert upper_spec("abc") == "ABC"
    assert strip_spec("  asdf  ") == "asdf"
    assert strip_equals_spec("==asdf==") == "asdf"


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
    replace_spec = UpdateSpec(replace, old="ch4", new="mf")

    renamed_ds = rename(dataset, replace_spec)

    assert sorted(renamed_ds.data_vars) == ["mf", "mf_number_of_observations", "mf_variability"]

    assert renamed_ds.attrs == dataset.attrs

    for dv in renamed_ds.data_vars:
        assert renamed_ds[dv].attrs == dataset[dv.replace("mf", "ch4")].attrs


def test_update_attrs(dataset):
    ds = dataset[["ch4"]].resample(time="4h").std()

    suffix_spec = UpdateSpec(add_suffix, "variability", keys=["long_name"])
    ds = _update_attrs(ds, suffix_spec)

    assert ds.ch4.attrs["long_name"] == "mole_fraction_of_methane_in_air_variability"
    assert ds.ch4.attrs["units"] == "1e-9"
