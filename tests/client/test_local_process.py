from openghg.client import process_flux, process_footprint, process_obs
from helpers import get_datapath, get_footprint_datapath, get_emissions_datapath


def test_process_obs():
    hfd_path = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")

    results = process_obs(
        files=hfd_path, site="hfd", instrument="picarro", network="DECC", data_type="CRDS", overwrite=True
    )

    results = results["processed"]["hfd.picarro.1minute.100m.min.dat"]

    assert "error" not in results
    assert "ch4" in results
    assert "co2" in results


def test_process_footprint():
    datapath = get_footprint_datapath("footprint_test.nc")

    site = "TMB"
    network = "LGHG"
    height = "10m"
    domain = "EUROPE"
    model = "test_model"

    results = process_footprint(
        files=datapath, site=site, model=model, network=network, height=height, domain=domain
    )

    assert "error" not in results
    assert "tmb_europe_test_model_10m" in results


def test_process_emissions():
    test_datapath = get_emissions_datapath("co2-gpp-cardamom-mth_EUROPE_2012.nc")

    proc_results = process_flux(
        files=test_datapath,
        species="co2",
        source="gpp-cardamom",
        date="2012",
        domain="europe",
        high_time_resolution=False,
    )

    assert "co2_gppcardamom_europe_2012" in proc_results
