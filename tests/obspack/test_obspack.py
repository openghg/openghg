from pathlib import Path
from openghg.standardise import standardise_surface
from openghg.obspack import read_input_file, create_obspack
from helpers import clear_test_stores, get_obspack_datapath, get_surface_datapath


def populate_object_store():

    clear_test_stores()

    openghg_path = get_surface_datapath(
        filename="DECC-picarro_TAC_20130131_co2-185m-20220929_cut.nc", source_format="OPENGHG"
    )
    standardise_surface(
        store="user",
        filepath=openghg_path,
        source_format="OPENGHG",
        site="tac",
        network="DECC",
        instrument="picarro",
        sampling_period="1h",
    )

    # DECC network sites
    network = "DECC"
    bsd_248_path = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")
    bsd_108_path = get_surface_datapath(filename="bsd.picarro.1minute.108m.min.dat", source_format="CRDS")
    bsd_42_path = get_surface_datapath(filename="bsd.picarro.1minute.42m.min.dat", source_format="CRDS")

    bsd_paths = [bsd_248_path, bsd_108_path, bsd_42_path]

    standardise_surface(store="user", filepath=bsd_paths, source_format="CRDS", site="bsd", network=network)


def test_read_input_file():

    populate_object_store()
    filename = get_obspack_datapath("example_search_input.csv")

    data, obs_types = read_input_file(filename)
    print("data", data)
    print("data 1 data", data[0].data)
    print("data 2 data", data[1].data)


def test_create_obspack():

    populate_object_store()
    filename = get_obspack_datapath("example_search_input.csv")

    # TODO: May want to mock the output creation and/or create, check then and delete this within tests
    output_path = Path("~/test_GEMMA_ObsPack").expanduser()
    output_path.mkdir(exist_ok=True)

    create_obspack(filename, output_path, "test_gemma_v1")
