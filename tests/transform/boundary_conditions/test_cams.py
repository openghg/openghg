from helpers import get_bc_datapath
import logging
import numpy as np
import xarray as xr

from openghg.transform.boundary_conditions import parse_cams
from openghg.transform.boundary_conditions._cams import _check_and_set_params, cams_to_domain

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_parse_cams():
    """
    To test the parser for boundary condititons
    """
    bc_input = "cams_test"
    cams_version = "v22r1"
    domain = "europe"
    species = "n2o"
    period = "daily"

    data_path = get_bc_datapath(filename="cams73_v22r1_n2o_test_202201.nc")

    results = parse_cams(
        datapath=data_path,
        species=species,
        bc_input=bc_input,
        period=period,
        cams_version=cams_version,
        domain=domain,
    )

    # test metadata
    metadata = results[f"{species}_{bc_input}_{domain}"]["metadata"]
    expected_str_metadata = {
        "bc_input": bc_input,
        "CAMS_version": cams_version,
        "domain": domain,
        "species": species,
    }
    for k, v in expected_str_metadata.items():
        assert metadata[k].lower() == v.lower()
    expected_float_metadata = {
        "min_longitude": -97.9,
        "max_longitude": 39.38,
        "min_latitude": 10.729,
        "max_latitude": 79.057,
        "min_height": 500,
        "max_height": 19500,
    }
    for k, v in expected_float_metadata.items():
        assert float(metadata[k]) == float(v)

    # test data
    bc_data = results[f"{species}_{bc_input}_{domain}"]["data"].compute()
    assert np.isclose(bc_data["vmr_n"].values.mean(), 313.4994)
    assert np.isclose(bc_data["vmr_s"].values.mean(), 335.2557)
    assert np.isclose(bc_data["vmr_w"].values.mean(), 323.5323)
    assert np.isclose(bc_data["vmr_e"].values.mean(), 324.0306)

    assert bc_data.time.size == 3
    assert bc_data.time.values[0] == np.datetime64("2022-01-01T00:00")

    assert bc_data["vmr_n"].attrs["units"] == "1e-09"
    assert bc_data["vmr_s"].attrs["units"] == "1e-09"
    assert bc_data["vmr_e"].attrs["units"] == "1e-09"
    assert bc_data["vmr_w"].attrs["units"] == "1e-09"


def _make_cams_dataset(vertical_coord_name: str) -> xr.Dataset:
    time = np.array(["2022-01-01T00:00"], dtype="datetime64[ns]")
    lat = np.array([-1.0, 0.0, 1.0, 2.0], dtype=float)
    lon = np.array([-1.0, 0.0, 1.0, 2.0], dtype=float)
    level = np.array([0, 1], dtype=int)
    hlevel = np.array([0, 1, 2], dtype=int)

    level_heights = np.array([50.0, 150.0], dtype=np.float32).reshape(1, 2, 1, 1)
    lat_offsets = (10.0 * lat).astype(np.float32).reshape(1, 1, 4, 1)
    lon_offsets = lon.astype(np.float32).reshape(1, 1, 1, 4)
    species_data = level_heights + lat_offsets + lon_offsets

    interface_heights = np.array([0.0, 100.0, 200.0], dtype=np.float32).reshape(1, 3, 1, 1)
    vertical_data = np.broadcast_to(interface_heights, (1, 3, 4, 4)).copy()

    return xr.Dataset(
        {
            "species": (("time", "level", "lat", "lon"), species_data),
            vertical_coord_name: (("time", "hlevel", "lat", "lon"), vertical_data),
        },
        coords={"time": time, "level": level, "hlevel": hlevel, "lat": lat, "lon": lon},
    )


def _write_mock_co2_cams_file(tmp_path, filename: str) -> xr.Dataset:
    """Write a minimal mock CO2 CAMS file based on cams73_v23r1_co2_conc_surface_inst_202101.cdl."""
    time = np.array(
        ["2022-01-01T00:00", "2022-01-01T03:00", "2022-01-01T06:00"], dtype="datetime64[ns]"
    )
    lat = np.array([-1.0, 0.0, 1.0, 2.0], dtype=float)
    lon = np.array([-1.0, 0.0, 1.0, 2.0], dtype=float)
    level = np.array([0, 1], dtype=int)
    hlevel = np.array([0, 1, 2], dtype=int)

    level_heights = np.array([50.0, 150.0], dtype=np.float32).reshape(1, 2, 1, 1)
    lat_offsets = (10.0 * lat).astype(np.float32).reshape(1, 1, 4, 1)
    lon_offsets = lon.astype(np.float32).reshape(1, 1, 1, 4)
    species_data = np.broadcast_to(level_heights + lat_offsets + lon_offsets, (3, 2, 4, 4)).copy()

    interface_heights = np.array([0.0, 100.0, 200.0], dtype=np.float32).reshape(1, 3, 1, 1)
    vertical_data = np.broadcast_to(interface_heights, (3, 3, 4, 4)).copy()

    ds = xr.Dataset(
        {
            "CO2": (
                ("time", "level", "latitude", "longitude"),
                species_data,
                {
                    "long_name": "CO2 dry mole fraction",
                    "units": "mol mol-1",
                },
            ),
            "height_above_reference_ellipsoid": (
                ("time", "hlevel", "latitude", "longitude"),
                vertical_data,
                {
                    "long_name": "Altitude of layer interfaces above the reference ellipsoid",
                    "units": "m",
                },
            ),
        },
        coords={
            "time": time,
            "level": level,
            "hlevel": hlevel,
            "latitude": lat,
            "longitude": lon,
        },
    )

    data_path = tmp_path / filename
    ds.to_netcdf(data_path)
    return ds


def test_cams_to_domain_uses_altitude_for_ch4(monkeypatch):
    class DummyFootprint:
        def __init__(self):
            self.data = xr.Dataset(
                coords={
                    "lat": np.array([0.0, 1.0], dtype=float),
                    "lon": np.array([0.0, 1.0], dtype=float),
                    "height": np.array([50.0, 150.0], dtype=float),
                }
            )

    monkeypatch.setattr(
        "openghg.transform.boundary_conditions._cams.get_footprint", lambda **kwargs: DummyFootprint()
    )

    ds = _make_cams_dataset("altitude")
    result = cams_to_domain(ds, "TEST", get_footprint_kwargs={"domain": "TEST"})

    assert np.isclose(result["vmr_n"].mean().item(), 120.5)
    assert np.isclose(result["vmr_s"].mean().item(), 90.5)
    assert np.isclose(result["vmr_e"].mean().item(), 107.0)
    assert np.isclose(result["vmr_w"].mean().item(), 104.0)


def test_cams_to_domain_uses_ellipsoid_height_for_co2(monkeypatch):
    class DummyFootprint:
        def __init__(self):
            self.data = xr.Dataset(
                coords={
                    "lat": np.array([0.0, 1.0], dtype=float),
                    "lon": np.array([0.0, 1.0], dtype=float),
                    "height": np.array([50.0, 150.0], dtype=float),
                }
            )

    monkeypatch.setattr(
        "openghg.transform.boundary_conditions._cams.get_footprint", lambda **kwargs: DummyFootprint()
    )

    ds = _make_cams_dataset("height_above_reference_ellipsoid")
    result = cams_to_domain(ds, "TEST", get_footprint_kwargs={"domain": "TEST"})

    assert np.isclose(result["vmr_n"].mean().item(), 120.5)
    assert np.isclose(result["vmr_s"].mean().item(), 90.5)
    assert np.isclose(result["vmr_e"].mean().item(), 107.0)
    assert np.isclose(result["vmr_w"].mean().item(), 104.0)


def test_parse_cams_co2_from_mock_file(tmp_path, monkeypatch):
    filename = "cams73_v23r1_co2_conc_surface_inst_202101.nc"
    data_path = tmp_path / filename
    ds = _write_mock_co2_cams_file(tmp_path, filename=filename)

    called = {}

    def fake_find_domain(domain):
        called["domain"] = domain
        return np.array([0.0, 1.0], dtype=float), np.array([0.0, 1.0], dtype=float), 1.0, 1.0

    monkeypatch.setattr("openghg.transform.boundary_conditions._cams.find_domain", fake_find_domain)

    results = parse_cams(bc_input="cams_test", domain="TESTDOMAIN", datapath=data_path, species="co2")

    assert called["domain"] == "TESTDOMAIN"
    assert ds["CO2"].attrs == {"long_name": "CO2 dry mole fraction", "units": "mol mol-1"}

    metadata = results["co2_cams_test_TESTDOMAIN"]["metadata"]
    assert metadata["domain"] == "TESTDOMAIN"
    assert metadata["species"] == "co2"

    bc_data = results["co2_cams_test_TESTDOMAIN"]["data"].compute()
    assert np.isclose(bc_data["vmr_n"].mean().item(), 170.5)
    assert np.isclose(bc_data["vmr_s"].mean().item(), 140.5)
    assert np.isclose(bc_data["vmr_e"].mean().item(), 157.0)
    assert np.isclose(bc_data["vmr_w"].mean().item(), 154.0)
    assert bc_data["vmr_n"].attrs["units"] == "1.0"


def test_check_and_set_params_for_co2_surface_inst_filename():
    filepath = [
        get_bc_datapath(filename="cams73_v22r1_n2o_test_202201.nc").parent
        / "cams73_v23r1_co2_conc_surface_inst_202101.nc"
    ]

    cams_version, species, input_observations = _check_and_set_params(filepath)

    assert cams_version == "v23r1"
    assert species == "co2"
    assert input_observations == "surface_inst"
