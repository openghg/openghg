from pathlib import Path
from openghg.objectstore import get_local_bucket
from openghg.store import ObsSurface, Emissions, Footprints
from openghg.analyse import footprints_data_merge


def get_datapath(filename, data_type):
    return (
        Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")
    )


def get_fp_datapath(filename):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/footprints/{filename}")


def get_flux_datapath(filename):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/emissions/{filename}")


# def test_single_site_footprint():
#     # First read in some data
#     tmb_path = get_datapath()

#     res = ObsSurface.read_file()

# @pytest.fixture()
def co2_setup():
    data_type = "CRDS"

    tac_file = get_datapath(filename="tac.picarro.hourly.100m.test.dat", data_type=data_type)
    tac_footprint = get_fp_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    co2_emissions = get_flux_datapath("co2-rtot-cardamom-2hr_TEST_2014.nc")

    site = "tac"
    species = "co2"
    network = "DECC"
    height = "100m"

    domain = "TEST"
    model = "NAME"
    metmodel = "UKV"

    source = "rtot-cardamom"
    date = "2014"

    ObsSurface.read_file(filepath=tac_file, data_type=data_type, site=site, network=network, inlet=height)

    Footprints.read_file(
        filepath=tac_footprint,
        site=site,
        height=height,
        domain=domain,
        model=model,
        metmodel=metmodel,
        species=species,
    )

    Emissions.read_file(
        filepath=co2_emissions,
        species=species,
        source=source,
        domain=domain,
        date=date,
        high_time_resolution=True,
    )


def test_co2_footprint_data_merge():

    co2_setup()

    site = "tac"
    species = "co2"
    network = "DECC"
    height = "100m"

    domain = "TEST"
    source = "rtot-cardamom"

    start_date = "2014-07-01"
    end_date = "2014-07-02"

    CombinedData_HiTRes = footprints_data_merge(
        site=site,
        height=height,
        domain=domain,
        network=network,
        start_date=start_date,
        end_date=end_date,
        flux_sources=source,
        species=species,
        load_flux=True,
        calc_timeseries=True,
        time_resolution="high",
    )

    data = CombinedData_HiTRes.data

    assert "mf_mod_high_res" in data
