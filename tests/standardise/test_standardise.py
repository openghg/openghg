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


def test_local_obs_openghg():
    """
    Based on reported Issue #477 where ValueError is raised when synchronising the metadata and attributes.
     - "inlet" and "inlet_height_magl" attribute within netcdf file was a float; "inlet" within metadata is converted to a string with "m" ("185m")
     - "inlet_height_magl" in metadata was just being set to "inlet" from metadata ("185m")
     - sync_surface_metadata was trying to compare the two values of 185.0 and "185m" but "185m" could not be converted to a float - ValueError
    """
    filepath = get_surface_datapath(filename="DECC-picarro_TAC_20130131_co2-185m-20220929_cut.nc", source_format="OPENGHG")

    results = standardise_surface(
        filepaths=filepath,
        site="TAC",
        network="DECC",
        inlet=185,
        instrument="picarro",
        source_format="openghg",
        sampling_period="1H",
        overwrite=True,
    )

    results = results["processed"]["DECC-picarro_TAC_20130131_co2-185m-20220929_cut.nc"]

    assert "error" not in results
    assert "co2" in results


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


def test_standardise_flux():
    test_datapath = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2012.nc")

    proc_results = standardise_flux(
        filepath=test_datapath,
        species="co2",
        source="gpp-cardamom",
        domain="europe",
        high_time_resolution=False,
        overwrite=True,
    )

    assert "co2_gpp-cardamom_europe" in proc_results


def test_standardise_flux_additional_keywords():

    test_datapath = get_emissions_datapath("ch4-anthro_globaledgar_v5-0_2014.nc")

    proc_results = standardise_flux(
        filepath=test_datapath,
        species="ch4",
        source="anthro",
        domain="globaledgar",
        database="EDGAR",
        database_version="v50",
    )

    assert "ch4_anthro_globaledgar" in proc_results


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
