from pathlib import Path

from helpers import get_emissions_datapath, get_footprint_datapath, get_surface_datapath
from openghg.standardise import standardise_flux, standardise_footprint, standardise_surface
from openghg.util import compress


# Test local functions
def test_local_obs():
    hfd_path = get_surface_datapath(filename="hfd.picarro.1minute.100m.min.dat", source_format="CRDS")

    results = standardise_surface(
        filepaths=hfd_path,
        site="hfd",
        instrument="picarro",
        network="DECC",
        source_format="CRDS",
        overwrite=True,
    )

    results = results["processed"]["hfd.picarro.1minute.100m.min.dat"]

    assert "error" not in results
    assert "ch4" in results
    assert "co2" in results

    mhd_path = get_surface_datapath(filename="mhd.co.hourly.g2401.15m.dat", source_format="ICOS")

    results = standardise_surface(
        filepaths=mhd_path,
        site="mhd",
        inlet="15m",
        instrument="g2401",
        network="ICOS",
        source_format="ICOS",
        overwrite=True,
    )

    assert "co" in results["processed"]["mhd.co.hourly.g2401.15m.dat"]


def test_standardise_footprint():
    datapath = get_footprint_datapath("footprint_test.nc")

    site = "TMB"
    network = "LGHG"
    height = "10m"
    domain = "EUROPE"
    model = "test_model"

    results = standardise_footprint(
        filepath=datapath,
        site=site,
        model=model,
        network=network,
        height=height,
        domain=domain,
        high_spatial_res=True,
        overwrite=True,
    )

    assert "error" not in results
    assert "tmb_europe_test_model_10m" in results


def test_standardise_emissions():
    test_datapath = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2012.nc")

    proc_results = standardise_flux(
        filepath=test_datapath,
        species="co2",
        source="gpp-cardamom",
        date="2012",
        domain="europe",
        high_time_resolution=False,
        overwrite=True,
    )

    assert "co2_gpp-cardamom_europe_2012" in proc_results


def test_standardise(monkeypatch, mocker, tmpdir):
    monkeypatch.setenv("OPENGHG_HUB", "1")
    call_fn_mock = mocker.patch("openghg.cloud.call_function", autospec=True)
    test_string = "some_text"
    tmppath = Path(tmpdir).joinpath("test_file.txt")
    tmppath.write_text(test_string)

    packed = compress((tmppath.read_bytes()))

    standardise_surface(
        filepaths=tmppath,
        site="bsd",
        inlet="248m",
        network="decc",
        source_format="crds",
        sampling_period="1m",
        instrument="picarro",
        overwrite=True,
    )

    assert call_fn_mock.call_args == mocker.call(
        data={
            "function": "standardise",
            "data": packed,
            "metadata": {
                "site": "bsd",
                "source_format": "crds",
                "network": "decc",
                "inlet": "248m",
                "instrument": "picarro",
                "sampling_period": "1m",
                "data_type": "surface",
            },
            "file_metadata": {
                "compressed": True,
                "sha1_hash": "56ba5dd8ea2fd49024b91792e173c70e08a4ddd1",
                "filename": "test_file.txt",
                "obs_type": "surface",
            },
        }
    )
