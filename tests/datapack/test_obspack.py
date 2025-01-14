import xarray as xr
from pandas import DataFrame
import pytest
from pathlib import Path
from openghg.standardise import standardise_surface
from openghg.datapack import retrieve_data, create_obspack, define_obspack_filename, define_obspack_name
from helpers import clear_test_stores, get_obspack_datapath, get_surface_datapath


#%% Test define_obspack_filename functions

@pytest.mark.parametrize(
        "metadata, obs_type, obspack_path, out_filename",
        [
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m"},
                "surface-insitu", "",
                "./surface-insitu/ch4_WAO_10m_surface-insitu.nc"
            ),
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m"},
                "surface-insitu", "/path/to/obspack/",
                "/path/to/obspack/surface-insitu/ch4_WAO_10m_surface-insitu.nc"
            ),
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m", "latest_version": "v1"},
                "surface-insitu", "/path/to/obspack/",
                "/path/to/obspack/surface-insitu/ch4_WAO_10m_surface-insitu_v1.nc"
            ),
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m"},
                "surface-flask", "",
                "./surface-flask/ch4_WAO_10m_surface-flask.nc"
            ),
            (
                {"platform": "site", "species": "ch4", "site": "WAO"},
                "column", "",
                "./column/ch4_WAO_site_column.nc"
            ),
            (
                {"platform": "satellite", "species": "ch4", "site": "GOSAT-BRAZIL"},
                "column", "",
                "./column/ch4_GOSAT-BRAZIL_satellite_column.nc"
            ),
            (
                {"platform": "satellite", "species": "ch4", "satellite": "GOSAT", "selection": "BRAZIL"},
                "column", "",
                "./column/ch4_GOSAT-BRAZIL_satellite_column.nc"
            ),
            (
                {"platform": "satellite", "species": "ch4", "satellite": "GOSAT", "domain": "SOUTHAMERICA"},
                "column", "",
                "./column/ch4_GOSAT-SOUTHAMERICA_satellite_column.nc"
            ),
        ]
)
def test_define_obspack_filename(metadata, obs_type, obspack_path, out_filename):
    """
    Test creation of filename matches to naming scheme
    1. surface-insitu data
    2. surface-insitu data, specified output_path
    3. surface-insitu data, version in the metadata
    4. surface-flask data
    5. column, site data
    6. column, satellite data, site name specified
    7. column, satellite data, satellite name and selection specified
    8. column, satellite data, satellite name and domain specified
    """
    out_filename = Path(out_filename)
    filename = define_obspack_filename(metadata, obs_type=obs_type, obspack_path=obspack_path)

    assert filename == out_filename


@pytest.mark.parametrize(
        "latest_version, data_version, include_version, out_filename",
        [
            ("v52", None, True, "/path/to/obspack/surface-insitu/ch4_WAO_10m_surface-insitu_v52.nc"),
            (None, "v34", True, "/path/to/obspack/surface-insitu/ch4_WAO_10m_surface-insitu_v34.nc"),
            ("v52", "v34", True, "/path/to/obspack/surface-insitu/ch4_WAO_10m_surface-insitu_v34.nc"),
            (None, None, True, "/path/to/obspack/surface-insitu/ch4_WAO_10m_surface-insitu.nc"),
            ("v52", "v34", False, "/path/to/obspack/surface-insitu/ch4_WAO_10m_surface-insitu.nc"),
        ]
)
def test_define_obspack_filename_version(latest_version, data_version, include_version, out_filename):
    """
    Check internal version definitions when creating filenames.
    1. Check data version can be inferred from metadata
    2. Check data version can be defined directly
    3. Check data version defined directly is used in preference to metadata
    4. Check data version is not included if this cannot be found
    5. Check data version is not included if include_version is False
    """
    metadata = {"site": "WAO", "species": "ch4", "inlet": "10m"}
    if latest_version:
        metadata["latest_version"] = latest_version

    obs_type = "surface-insitu"
    obspack_path = "/path/to/obspack/"
    out_filename = Path(out_filename)
    filename = define_obspack_filename(metadata,
                                       obs_type=obs_type,
                                       obspack_path=obspack_path,
                                       include_version=include_version,
                                       data_version=data_version)

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
    """
    Check search file can be used to find data within the object store.
    """

    populate_object_store()
    filename = get_obspack_datapath("example_search_input_full.csv")

    full_data = retrieve_data(filename=filename)
    
    assert len(full_data) == 3

    expected_details_found = [
        {"site": "tac", "species": "co2", "inlet": "185m"},
        {"site": "bsd", "species": "ch4", "inlet": "multiple"},
        {"site": "bsd", "species": "ch4", "inlet": "42m"}
        ]

    for data, details in zip(full_data, expected_details_found):
        data_attrs = data.data.attrs
        assert data_attrs.items() >= details.items()


def test_create_obspack_structure(tmp_path):
    """
    Test full pipeline and that an obspack can be created based on the input
    search details.

    Current search file is: "example_search_input_full.csv"
    This contains three entries:
    1. TAC, 185m, co2, labelled as surface-insitu data - direct search
    2. BSD, 100m-250m, ch4, labelled as surface-insitu data - search across range of inlets
    3. BSD, 42m, ch4, labelled as surface-flask data - create in a different subfolder

    Expected obspack structure:
    test_gemma_v1/
        obspack_README.md
        site_index_details*.txt
        site_insitu/
            ch4_bsd_multiple_surface-insitu_v1.nc
            co2_tac_185m_surface-insitu_v1.nc
        site-flask/
            ch4_bsd_42m_surface-flask_v1.nc
    """

    populate_object_store()
    filename = get_obspack_datapath("example_search_input_full.csv")

    store="user"
    obspack_path = create_obspack(search_filename=filename,
                                  output_folder=tmp_path,
                                  obspack_name="test_gemma_v1",
                                  store=store)

    # Check obspack structure
    release_file = "obspack_README.md"
    site_file_search = "site_index_details*.txt"
    subfolder_file_num = {"surface-insitu": 2,
                          "surface-flask": 1}

    assert obspack_path.exists()
    assert (obspack_path / release_file).exists()
    
    site_index_files = list(obspack_path.glob(site_file_search))
    assert len(site_index_files) == 1

    for subfolder, num_files in subfolder_file_num.items():
        full_path = obspack_path / subfolder
        assert full_path.exists()

        files_in_folder = list(full_path.glob("*"))
        assert len(files_in_folder) == num_files


def test_create_obspack_file_insitu(tmp_path):
    """
    Check data file within obspack folder contains expected details.

    Current search file is: "example_search_input_1.csv"

    Expect: site_insitu/co2_tac_185m_surface-insitu_v1.nc
     - Main data variable should be species name (not "mf")
    """

    populate_object_store()
    filename = get_obspack_datapath("example_search_input_1.csv")

    store="user"
    obspack_path = create_obspack(search_filename=filename,
                                  output_folder=tmp_path,
                                  obspack_name="test_gemma_v1",
                                  store=store)

    species = "co2"
    site = "tac"
    inlet = "185m"
    obs_type = "surface-insitu"

    # Check TAC file within surface-insitu folder in obspack
    # Check expected data variables
    # Note: not checking values at the moment.
    subfolder_insitu = obspack_path / obs_type
    file_search = f"{species}_{site}_{inlet}_{obs_type}_*.nc"
    filename = list(subfolder_insitu.glob(file_search))[0]

    ds = xr.open_dataset(filename)
    assert species in ds.data_vars  # Make sure species name is still used
    assert "time" in ds.coords

    # Check TAC file contains (at least) the expected attributes
    expected_attrs = {'Conditions of use': 'Ensure that you contact the data owner at the outset of your project.',
                      'Source': 'In situ measurements of air',
                      'conditions_of_use': 'Ensure that you contact the data owner at the outset of your project.',
                      'data_owner': "Simon O'Doherty",
                      'data_owner_email': 's.odoherty@bristol.ac.uk',
                      'inlet': '185m',
                      'inlet_height_magl': 185.0,
                      'instrument': 'picarro',
                      'network': 'decc',
                      'sampling_period': '3600.0',
                      'sampling_period_unit': 's',
                      'site': 'tac',
                      'species': 'co2',
                      'station_height_masl': 50.0,
                      'station_latitude': 52.51775,
                      'station_long_name': 'Tacolneston Tower, UK',
                      'station_longitude': 1.13872,
                      'scale': 'WMO-X2019',
    }

    assert ds.attrs.items() > expected_attrs.items()


def test_create_obspack_file_multi_inlet(tmp_path):
    """
    Check data file within obspack folder contains expected details when
    range of inlets is specified.

    Current search file is: "example_search_input_2.csv"

    Expect: site_insitu/ch4_bsd_multiple_surface-insitu_v1.nc
     - Main data variable should be species name (not "mf")
     - Should contain "inlet" data variable
    """

    populate_object_store()
    filename = get_obspack_datapath("example_search_input_2.csv")

    store="user"
    obspack_path = create_obspack(search_filename=filename,
                                  output_folder=tmp_path,
                                  obspack_name="test_gemma_v1",
                                  store=store)

    species = "ch4"
    site = "bsd"
    inlet = "multiple"
    obs_type = "surface-insitu"

    # Check BSD file within surface-insitu folder in obspack
    # Check expected data variables
    # Note: not checking values at the moment.
    subfolder_insitu = obspack_path / obs_type
    file_search = f"{species}_{site}_{inlet}_{obs_type}_*.nc"
    filename = list(subfolder_insitu.glob(file_search))[0]

    ds = xr.open_dataset(filename)
    assert species in ds.data_vars  # Make sure species name is still used
    assert "inlet" in ds.data_vars  # Check inlet is included for multiple inlet file


def test_create_obspack_search(tmp_path):
    """
    Check obspack can be created when specifying a search DataFrame directly.
    """

    populate_object_store()
    store="user"

    search_df = DataFrame({"site": ["tac", "bsd"],
                           "species": ["co2", "ch4"],
                           "inlet": ["185m", "108m"]})

    obspack_path = create_obspack(search_df=search_df,
                                  output_folder=tmp_path,
                                  obspack_name="test_gemma_v1",
                                  store=store)

    species = "co2"
    site = "tac"
    inlet = "185m"
    obs_type = "surface-insitu"

    # Check TAC file within surface-insitu folder in obspack when created from search DataFrame directly
    # Check expected data variables
    # Note: not checking values at the moment.
    subfolder_insitu = obspack_path / obs_type
    file_search = f"{species}_{site}_{inlet}_{obs_type}_*.nc"
    filename = list(subfolder_insitu.glob(file_search))[0]

    ds = xr.open_dataset(filename)
    assert species in ds.data_vars  # Make sure species name is still used
