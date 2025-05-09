from pathlib import Path
import pytest
from openghg.datapack import (
    define_stored_data_filename,
    define_full_obspack_filename,
    define_obspack_name,
)
from openghg.datapack._file_structure import _find_additional_metakeys, _construct_name

# %% Test filename creation functions


@pytest.mark.parametrize(
    "keys, separators, expected",
    [
        (["a", "c", "b"], ("_",), "start_middle_end"),
        (["a", ["c", "b"]], ("_", "-"), "start_middle-end"),
        (["a", ["c", "d"], "b"], ("_", "-"), "start_middle-extra_end"),
        ([["a", ["c", "d"]], "b"], ("_", "-", "+"), "start-middle+extra_end"),
        (["a"], ("_",), "start"),
    ],
)
def test_construct_name(keys, separators, expected):
    """
    Check for the recurive _construct_name() function. This takes a nested list of keys,
    a dictionary and a set of seperators and constructs an output name based on this.
    1. Check simple list - all values separated by "_"
    2. Check nested list (using different separators)
    3. Check nested list can be intermixed
    4. Check 3-level nesting
    """
    dictionary = {"a": "start", "b": "end", "c": "middle", "d": "extra"}
    name = _construct_name(keys, dictionary, separators)
    assert name == expected


def test_construct_name_fails_sep_depth():
    keys = [["a", ["c", "d"]], "b"]
    separators = ("_", "-")
    dictionary = {"a": "start", "b": "end", "c": "middle", "d": "extra"}

    with pytest.raises(ValueError) as excinfo:
        _construct_name(keys, dictionary, separators)

    assert "separators must be >= depth of keys" in str(excinfo.value)


def test_construct_name_fails_missing_key():
    keys = ["a", "c", "b"]
    separators = ("_", "-")
    dictionary = {"a": "start", "b": "end"}

    with pytest.raises(ValueError) as excinfo:
        _construct_name(keys, dictionary, separators)

    assert "unable to find key: 'c'" in str(excinfo.value)


@pytest.mark.parametrize(
        "metadata, obs_type, out_filename",
        [
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m"},
                "surface-insitu", "ch4_WAO_10m_surface-insitu.nc"
            ),
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m"},
                "surface-insitu", "ch4_WAO_10m_surface-insitu.nc"
            ),
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m", "latest_version": "v1"},
                "surface-insitu", "ch4_WAO_10m_surface-insitu_v1.nc"
            ),
            (
                {"site": "WAO", "species": "ch4", "inlet": "10m"},
                "surface-flask", "ch4_WAO_10m_surface-flask.nc"
            ),
            (
                {"platform": "site", "species": "ch4", "site": "WAO"},
                "column", "ch4_WAO_site_column.nc"
            ),
            (
                {"platform": "satellite", "species": "ch4", "site": "GOSAT-BRAZIL"},
                "column", "ch4_GOSAT-BRAZIL_satellite_column.nc"
            ),
            (
                {"platform": "satellite", "species": "ch4", "satellite": "GOSAT", "selection": "BRAZIL"},
                "column", "ch4_GOSAT-BRAZIL_satellite_column.nc"
            ),
            (
                {"platform": "satellite", "species": "ch4", "satellite": "GOSAT", "domain": "SOUTHAMERICA"},
                "column", "ch4_GOSAT-SOUTHAMERICA_satellite_column.nc"
            ),
        ]
)
def test_define_stored_data_filename(metadata, obs_type, out_filename):
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
    filename = define_stored_data_filename(metadata, obs_type=obs_type)

    assert filename == out_filename

@pytest.mark.parametrize(
        "latest_version, data_version, include_version, out_filename",
        [
            ("v52", None, True, "ch4_WAO_10m_surface-insitu_v52.nc"),
            (None, "v34", True, "ch4_WAO_10m_surface-insitu_v34.nc"),
            ("v52", "v34", True, "ch4_WAO_10m_surface-insitu_v34.nc"),
            (None, None, True, "ch4_WAO_10m_surface-insitu.nc"),
            ("v52", "v34", False, "ch4_WAO_10m_surface-insitu.nc"),
        ]
)
def test_define_stored_data_filename_version(latest_version, data_version, include_version, out_filename):
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
    out_filename = Path(out_filename)
    filename = define_stored_data_filename(metadata,
                                       obs_type=obs_type,
                                       include_version=include_version,
                                       data_version=data_version)

    assert filename == out_filename


@pytest.mark.parametrize(
        "name_components, out_filename",
        [
            (["species", "site", "inlet"], "ch4_WAO_10m_surface-insitu_v1.nc"),
            (["site", "inlet", "species"], "WAO_10m_ch4_surface-insitu_v1.nc"),
            (["site", "data_source", "data_level"], "WAO_icos_1_surface-insitu_v1.nc"),
        ]
)
def test_define_stored_data_filename_name_components(name_components, out_filename):
    """
    Check name components for the file name can be used correctly.
    1. Check the usual value create the expected output - ["species", "site", "inlet"]
    2. Check the order of the specified values is used - ["site", "inlet", "species"]
    4. Check different values for the metadata can be selected - ["site", "data_source", "data_level"]
    """
    metadata = {
        "site": "WAO",
        "species": "ch4",
        "inlet": "10m",
        "data_level": 1,
        "data_source": "icos",
        "latest_version": "v1",
    }

    obs_type = "surface-insitu"
    out_filename = Path(out_filename)
    filename = define_stored_data_filename(metadata,
                                       obs_type=obs_type,
                                       name_components=name_components)

    assert filename == out_filename


@pytest.mark.parametrize(
        "name_suffixes, out_filename",
        [
            ({"obs_type": "surface-insitu", "data_version": "v1"}, "ch4_WAO_10m_surface-insitu_v1.nc"),
            ({"latest_version": "v51"}, "ch4_WAO_10m_v51.nc"),
            ({"project": "gemma", "source": "noaa"}, "ch4_WAO_10m_gemma_noaa.nc"),
        ]
)
def test_define_stored_data_filename_name_suffixes(name_suffixes, out_filename):
    """
    Check name components for the file name can be used correctly.
    1. Check the standard suffix values create the expected output
    2. If same key exists in metadata, check values from name_suffixes are used
    3. Check new suffix values can be used to create output name
    """
    metadata = {"site": "WAO", "species": "ch4", "inlet": "10m", "latest_version": "v1"}

    obs_type = "surface-insitu"
    out_filename = Path(out_filename)
    filename = define_stored_data_filename(metadata,
                                       obs_type=obs_type,
                                       name_suffixes=name_suffixes)

    assert filename == out_filename

@pytest.mark.parametrize(
        "output_folder, subfolder",
        [
            (None, None),
            ("", ""),
            ("path/to/folder", None),
            (None, "site-insitu"),
            ("path/to/folder", "site-insitu"),
        ]
)
def test_define_full_obspack_filename(output_folder, subfolder):
    """
    Fairly basic tests to just check the full path to the output file is being constructed
    as expected.
    """

    filename = "ch4_WAO_10m_surface-insitu_v1.nc"
    obspack_name = "gemma_obspack_v2"

    full_filename = define_full_obspack_filename(filename,
                                                 obspack_name=obspack_name,
                                                 output_folder=output_folder,
                                                 subfolder=subfolder)

    if subfolder is None:
        subfolder = ""
    if output_folder is None:
        output_folder = ""

    expected_full_filename = Path(output_folder) / obspack_name / Path(subfolder) / filename

    assert full_filename == expected_full_filename
    

def test_find_additional_metakeys_insitu():
    """
    Check additional metakeys can be found for surface-insitu data.
    Assumptions:
    - The following keys are defaults when defining surface data:
      - "site", "species", "inlet", "data_level"
    This test will need updating is this stops being the case.
    """

    obs_type = "surface-insitu"
    name_components = ["site", "species", "inlet"]

    metakeys = _find_additional_metakeys(obs_type=obs_type, name_components=name_components)

    # Define a metakey we would expect for surface data
    # Note: Will need to update if the surface definition changes to remove this
    one_expected_metakey = "data_level"

    # Check name_components are not in metakeys
    assert not set(name_components) <= set(metakeys)
    assert one_expected_metakey in metakeys


#%% Test creation of obspack_name
    
@pytest.mark.parametrize(
"version,current_obspacks,expected_output",
[
    ("v1", [], "gemma_obspack_v1"),
    (
        None,
        ["gemma_obspack"],
        "gemma_obspack_v2",
    ),  # If no version is found, assume v1 and use next version
    (None, ["gemma_obspack_v1"], "gemma_obspack_v2"),
    (None, ["gemma_obspack_v1.1"], "gemma_obspack_v1.2"),
    (None, ["gemma_obspack_v0.23"], "gemma_obspack_v0.24"),
    (None, ["gemma_obspack_v1.1", "gemma_obspack_v2"], "gemma_obspack_v3"),
    (None, ["gemma_obspack_v0.23", "gemma_obspack_v1.1"], "gemma_obspack_v1.2"),
    (None, ["gemma_obspack_v2.0", "gemma_obspack_v1.1"], "gemma_obspack_v2.1"),
],
)
def test_define_obspack_name(version, current_obspacks, expected_output):
    obspack_stub = "gemma_obspack"
    output, version = define_obspack_name(
        obspack_stub=obspack_stub, version=version, current_obspacks=current_obspacks
    )

    assert output == expected_output


@pytest.mark.parametrize(
    "current_obspacks,expected_output",
    [
        ([], "gemma_obspack_v1.0"),
        (
            ["gemma_obspack"],
            "gemma_obspack_v1.1",
        ),  # If no version is found, assume v1 and use next minor version
        (["gemma_obspack_v1"], "gemma_obspack_v1.1"),
        (["gemma_obspack_v1.1"], "gemma_obspack_v1.2"),
        (["gemma_obspack_v0.23"], "gemma_obspack_v0.24"),
        (["gemma_obspack_v1.1", "gemma_obspack_v2"], "gemma_obspack_v2.1"),
        (["gemma_obspack_v0.23", "gemma_obspack_v1.1"], "gemma_obspack_v1.2"),
        (["gemma_obspack_v2.0", "gemma_obspack_v1.1"], "gemma_obspack_v2.1"),
    ],
)
def test_define_obspack_name_minor(current_obspacks, expected_output):
    obspack_stub = "gemma_obspack"
    minor_version_only = True
    output, version = define_obspack_name(
        obspack_stub=obspack_stub, minor_version_only=minor_version_only, current_obspacks=current_obspacks
    )

    assert output == expected_output


@pytest.mark.parametrize(
    "current_obspacks,expected_output",
    [
        ([], "gemma_obspack_v1"),
        (
            ["gemma_obspack"],
            "gemma_obspack_v2",
        ),  # If no version is found, assume v1 and use next major version
        (["gemma_obspack_v1"], "gemma_obspack_v2"),
        (["gemma_obspack_v1.1"], "gemma_obspack_v2"),
        (["gemma_obspack_v0.23"], "gemma_obspack_v1"),
        (["gemma_obspack_v1.1", "gemma_obspack_v2"], "gemma_obspack_v3"),
        (["gemma_obspack_v0.23", "gemma_obspack_v1.1"], "gemma_obspack_v2"),
        (["gemma_obspack_v2.0", "gemma_obspack_v1.1"], "gemma_obspack_v3"),
    ],
)
def test_define_obspack_name_major(current_obspacks, expected_output):
    obspack_stub = "gemma_obspack"
    major_version_only = True
    output, version = define_obspack_name(
        obspack_stub=obspack_stub, major_version_only=major_version_only, current_obspacks=current_obspacks
    )

    assert output == expected_output