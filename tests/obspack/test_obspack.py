import pytest
from pathlib import Path
from openghg.standardise import standardise_surface
from openghg.obspack import retrieve_data, create_obspack, define_obspack_filename, define_obspack_name
from helpers import clear_test_stores, get_obspack_datapath, get_surface_datapath


#%% Test define_obspack_filename functions

@pytest.mark.parametrize(
        "metadata, obs_type, obspack_path, out_filename",
        [
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m"},
                "surface-insitu", "",
                "./surface-insitu/ch4_WAO_10m_surface-insitu_v1.nc"
            ),
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m"},
                "surface-insitu", "/path/to/obspack/",
                "/path/to/obspack/surface-insitu/ch4_WAO_10m_surface-insitu_v1.nc"
            ),
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m"},
                "surface-flask", "",
                "./surface-flask/ch4_WAO_10m_surface-flask_v1.nc"
            ),
            (
                {"platform": "site", "species": "ch4", "site": "WAO"},
                "column", "",
                "./column/ch4_WAO_site_column_v1.nc"
            ),
            (
                {"platform": "satellite", "species": "ch4", "site": "GOSAT-BRAZIL"},
                "column", "",
                "./column/ch4_GOSAT-BRAZIL_satellite_column_v1.nc"
            ),
            (
                {"platform": "satellite", "species": "ch4", "satellite": "GOSAT", "selection": "BRAZIL"},
                "column", "",
                "./column/ch4_GOSAT-BRAZIL_satellite_column_v1.nc"
            ),
            (
                {"platform": "satellite", "species": "ch4", "satellite": "GOSAT", "domain": "SOUTHAMERICA"},
                "column", "",
                "./column/ch4_GOSAT-SOUTHAMERICA_satellite_column_v1.nc"
            ),
        ]
)
def test_define_obspack_filename(metadata, obs_type, obspack_path, out_filename):
    """
    Test creation of filename matches to naming scheme
    1. surface-insitu data
    2. surface-insitu data, specified output_path
    3. surface-flask data
    4. column, site data
    5. column, satellite data, site name specified
    6. column, satellite data, satellite name and selection specified
    7. column, satellite data, satellite name and domain specified
    """
    out_filename = Path(out_filename)
    filename = define_obspack_filename(metadata, obs_type=obs_type, obspack_path=obspack_path)

    assert filename == out_filename


@pytest.mark.parametrize(
        "version,current_obspacks,expected_output",
        [
            ("v1", [], "gemma_obspack_v1"),
            (None, ["gemma_obspack"], "gemma_obspack_v1"),
            (None, ["gemma_obspack_v1"], "gemma_obspack_v2"),
            (None, ["gemma_obspack_v1.1"], "gemma_obspack_v1.2"),
            (None, ["gemma_obspack_v0.23"], "gemma_obspack_v0.24"),
            (None, ["gemma_obspack_v1.1", "gemma_obspack_v2"], "gemma_obspack_v3"),
            (None, ["gemma_obspack_v0.23", "gemma_obspack_v1.1"], "gemma_obspack_v1.2"),
            (None, ["gemma_obspack_v2.0", "gemma_obspack_v1.1"], "gemma_obspack_v2.1"),
        ]
)
def test_define_obspack_name(version, current_obspacks, expected_output):
    obspack_stub = "gemma_obspack"
    output, version = define_obspack_name(obspack_stub=obspack_stub,
                                 version=version,
                                 current_obspacks=current_obspacks)

    assert output == expected_output


@pytest.mark.parametrize(
        "current_obspacks,expected_output",
        [
            ([], "gemma_obspack_v1.0"),
            (["gemma_obspack"], "gemma_obspack_v1.0"),
            (["gemma_obspack_v1"], "gemma_obspack_v1.1"),
            (["gemma_obspack_v1.1"], "gemma_obspack_v1.2"),
            (["gemma_obspack_v0.23"], "gemma_obspack_v0.24"),
            (["gemma_obspack_v1.1", "gemma_obspack_v2"], "gemma_obspack_v2.1"),
            (["gemma_obspack_v0.23", "gemma_obspack_v1.1"], "gemma_obspack_v1.2"),
            (["gemma_obspack_v2.0", "gemma_obspack_v1.1"], "gemma_obspack_v2.1"),
        ]
)
def test_define_obspack_name_minor(current_obspacks, expected_output):
    obspack_stub = "gemma_obspack"
    minor_version_only = True
    output, version = define_obspack_name(obspack_stub=obspack_stub,
                                          minor_version_only=minor_version_only,
                                          current_obspacks=current_obspacks)

    assert output == expected_output


@pytest.mark.parametrize(
        "current_obspacks,expected_output",
        [
            ([], "gemma_obspack_v1"),
            (["gemma_obspack"], "gemma_obspack_v1"),
            (["gemma_obspack_v1"], "gemma_obspack_v2"),
            (["gemma_obspack_v1.1"], "gemma_obspack_v2"),
            (["gemma_obspack_v0.23"], "gemma_obspack_v1"),
            (["gemma_obspack_v1.1", "gemma_obspack_v2"], "gemma_obspack_v3"),
            (["gemma_obspack_v0.23", "gemma_obspack_v1.1"], "gemma_obspack_v2"),
            (["gemma_obspack_v2.0", "gemma_obspack_v1.1"], "gemma_obspack_v3"),
        ]
)
def test_define_obspack_name_major(current_obspacks, expected_output):
    obspack_stub = "gemma_obspack"
    major_version_only = True
    output, version = define_obspack_name(obspack_stub=obspack_stub,
                                          major_version_only=major_version_only,
                                          current_obspacks=current_obspacks)

    assert output == expected_output


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


def test_retrieve_data():

    populate_object_store()
    filename = get_obspack_datapath("example_search_input.csv")

    data = retrieve_data(filename=filename)
    print("data", data)
    print("data 1 data", data[0].data)
    print("data 2 data", data[1].data)


def test_create_obspack():

    populate_object_store()
    filename = get_obspack_datapath("example_search_input.csv")

    store="user"

    # TODO: May want to mock the output creation and/or create, check then and delete this within tests
    output_path = Path("~/test_GEMMA_ObsPack").expanduser()
    output_path.mkdir(exist_ok=True)

    create_obspack(filename, output_path, "test_gemma_v1",store=store)
