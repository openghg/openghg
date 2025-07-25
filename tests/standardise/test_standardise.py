import pytest
from helpers import (
    get_flux_datapath,
    get_footprint_datapath,
    get_surface_datapath,
    get_column_datapath,
    get_flux_timeseries_datapath,
    clear_test_stores,
    clear_test_store,
    filt,
)
from openghg.retrieve import get_obs_surface, search, search_footprints, get_footprint, get_obs_column
from openghg.standardise import (
    standardise_column,
    standardise_flux,
    standardise_footprint,
    standardise_surface,
    standardise_flux_timeseries,
)
from openghg.dataobjects import FootprintData
from openghg.types import AttrMismatchError, ObjectStoreError
from openghg.util import compress, find_domain
import numpy as np


def test_standardise_to_read_only_store():
    hfd_path = get_surface_datapath(filename="hfd.picarro.1minute.100m.min.dat", source_format="CRDS")

    with pytest.raises(ObjectStoreError):
        standardise_surface(
            filepath=hfd_path,
            site="hfd",
            instrument="picarro",
            network="DECC",
            source_format="CRDS",
            overwrite=True,
            store="shared",
        )


def test_standardise_obs_two_writable_stores(reset_mock_user_config):

    clear_test_stores()
    hfd_path = get_surface_datapath(filename="hfd.picarro.1minute.100m.min.dat", source_format="CRDS")

    results = standardise_surface(
        filepath=hfd_path,
        site="hfd",
        instrument="picarro",
        network="DECC",
        source_format="CRDS",
        force=True,
        store="user",
    )

    results = filt(results, file="hfd.picarro.1minute.100m.min.dat")

    processed_species = [res.get("species") for res in results]

    assert "ch4" in processed_species
    assert "co2" in processed_species

    results = search(site="hfd", inlet="100m", store="user")
    assert results
    results = search(site="hfd", inlet="100m", store="group")
    assert not results

    rgl_path = get_surface_datapath(filename="ICOS_ATC_L2_L2-2024.1_RGL_90.0_CTS.CH4", source_format="ICOS")

    results = standardise_surface(
        filepath=rgl_path,
        site="rgl",
        inlet="90m",
        instrument="g2301",
        sampling_period="1H",
        network="ICOS",
        source_format="ICOS",
        store="group",
    )

    assert "ch4" == filt(results, file="ICOS_ATC_L2_L2-2024.1_RGL_90.0_CTS.CH4")[0].get("species")

    results = search(site="rgl", instrument="g2301", store="group")
    assert results
    results = search(site="rgl", instrument="g2301", store="user")
    assert not results


def test_standardise_obs_openghg():
    """
    Based on reported Issue #477 where ValueError is raised when synchronising the metadata and attributes.
     - "inlet" and "inlet_height_magl" attribute within netcdf file was a float; "inlet" within metadata is converted to a string with "m" ("185m")
     - "inlet_height_magl" in metadata was just being set to "inlet" from metadata ("185m")
     - sync_surface_metadata was trying to compare the two values of 185.0 and "185m" but "185m" could not be converted to a float - ValueError
    """
    clear_test_store("user")
    filepath = get_surface_datapath(
        filename="DECC-picarro_TAC_20130131_co2-185m-20220929_cut.nc", source_format="OPENGHG"
    )

    results = standardise_surface(
        filepath=filepath,
        site="TAC",
        network="DECC",
        inlet=185,
        instrument="picarro",
        source_format="openghg",
        sampling_period="1h",
        force=True,
        store="user",
        update_mismatch="metadata",
    )

    results = filt(results, file="DECC-picarro_TAC_20130131_co2-185m-20220929_cut.nc")
    assert "co2" == results[0].get("species")


def test_standardise_obs_metadata_mismatch(reset_mock_user_config):
    """
    Test a mismatch between the derived attributes and derived metadata can be
    updated and data added to the object store.

    This will use the attributes data and update the metadata.

    Difference:
        - 'station_long_name'
            - Metadata (from mocked site_info) - 'Tacolneston Tower, UK'
            - Attributes (from file) - 'ATTRIBUTE DATA'

    Note: using fake height of 999m to not intefere with previous data at 185m
    """

    filename = "DECC-picarro_TAC_20130131_co2-999m-20220929_mismatch.nc"
    filepath = get_surface_datapath(filename=filename, source_format="OPENGHG")

    # Define update_mismatch as "from_source" / "attributes"
    update_mismatch = "from_source"

    results = standardise_surface(
        filepath=filepath,
        site="TAC",
        network="DECC",
        inlet="999m",
        instrument="picarro",
        source_format="openghg",
        sampling_period="1h",
        update_mismatch=update_mismatch,
        overwrite=True,
        store="user",
    )

    # Check data has been successfully processed
    results = filt(results, file=filename)

    assert "co2" == results[0].get("species")

    # Check retrieved data from the object store contains the updated metadata
    data = get_obs_surface(site="TAC", inlet="999m", species="co2")

    assert data is not None

    metadata = data.metadata

    # Check attribute value has been used for this key
    assert metadata["station_long_name"] == "ATTRIBUTE DATA"

    attrs = data.data.attrs
    assert attrs["station_long_name"] == "ATTRIBUTE DATA"

    # # Find and delete dummy Datasource so we can add this again below.
    # results = search_surface(site="TAC", inlet="999m", species="co2")
    # uuid = results.results.loc[0, "uuid"]

    # obs = ObsSurface.load()
    # obs.delete(uuid=uuid)


def test_local_obs_metadata_mismatch_meta():
    """
    Test a mismatch between the derived attributes and derived metadata can be
    updated and data added to the object store.

    This will use the metadata values and update the attributes.

    Difference:
        - 'station_long_name'
            - Metadata (from mocked site_info) - 'Tacolneston Tower, UK'
            - Attributes (from file) - 'ATTRIBUTE DATA'

    Same attributes / metadata as described in 'test_local_obs_metadata_mismatch()'
    but slightly different height used to not clash with previous data.
    """

    filename = "DECC-picarro_TAC_20130131_co2-998m-20220929_mismatch.nc"
    filepath = get_surface_datapath(filename=filename, source_format="OPENGHG")

    # Define update_mismatch as "from_definition" / "metadata"
    update_mismatch = "from_definition"

    results = standardise_surface(
        filepath=filepath,
        site="TAC",
        network="DECC",
        inlet="998m",
        instrument="picarro",
        source_format="openghg",
        sampling_period="1h",
        update_mismatch=update_mismatch,
        store="user",
    )

    # Check data has been successfully processed
    results = filt(results, file=filename)

    assert "co2" == results[0].get("species")

    # Check retrieved data from the object store contains the updated metadata
    data = get_obs_surface(site="TAC", inlet="998m", species="co2")

    assert data is not None

    metadata = data.metadata

    # Check attribute value has been used for this key
    assert metadata["station_long_name"] == "Tacolneston Tower, UK"

    attrs = data.data.attrs
    assert attrs["station_long_name"] == "Tacolneston Tower, UK"

    # # Find and delete dummy Datasource so we can add this again below.
    # results = search_surface(site="TAC", inlet="998m", species="co2")
    # uuid = results.results.loc[0, "uuid"]

    # obs = ObsSurface.load()
    # obs.delete(uuid=uuid)


def test_local_obs_metadata_mismatch_fail():
    """
    Test that a mismatch between attributes and metadata raises a AttrMismatchError
    when update_mismatch is set to 'never'.

    Same attributes / metadata as described in 'test_local_obs_metadata_mismatch()'.
    """
    from helpers import clear_test_stores

    clear_test_stores()
    filepath = get_surface_datapath(
        filename="DECC-picarro_TAC_20130131_co2-999m-20220929_mismatch.nc", source_format="OPENGHG"
    )

    with pytest.raises(AttrMismatchError) as e_info:
        standardise_surface(
            filepath=filepath,
            site="TAC",
            network="DECC",
            inlet="999m",
            instrument="picarro",
            source_format="openghg",
            sampling_period="1h",
            update_mismatch="never",
            force=True,
            store="user",
        )

        # Check different values are reported in error message
        assert "Tacolneston Tower, UK" in e_info
        assert "ATTRIBUTE DATA" in e_info

        # Check error message contains advice on how to bypass this error
        assert "update_mismatch" in e_info


def test_standardise_column():
    """
    Tests standardise column function and associated metadata keys
    for satellite column data.
    """
    filepath = get_column_datapath(filename="gosat-fts_gosat_20170318_ch4-column.nc")

    satellite = "GOSAT"
    selection = "LAND"
    species = "methane"
    obs_region = "BRAZIL"

    results = standardise_column(
        filepath=filepath,
        source_format="OPENGHG",
        satellite=satellite,
        species=species,
        obs_region=obs_region,
        selection=selection,
        force=True,
        store="user",
    )

    assert "ch4" == results[0].get("species")

    data = get_obs_column(species="ch4", max_level=3, satellite="gosat")

    assert data.metadata["obs_region"] == "brazil"
    assert data.metadata["selection"] == "land"


def test_standardise_footprint():
    """ This is to test standardise_footprint method.
    Additionally the get_footprint is also tested by supplying direct store path instead of name."""

    from openghg.objectstore import get_readable_buckets

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
        force=True,
        high_spatial_resolution=True,
        overwrite=True,
        store="user",
    )

    result = results[0]
    assert result["site"] == "tmb"
    assert result["domain"] == "europe"
    assert result["model"] == "test_model"
    assert result["inlet"] == "10m"

    # testing direct path supplied to get function should fetch results.
    buckets = get_readable_buckets()

    result = get_footprint(site=site, network=network,
                           height=height,domain=domain,store=buckets["user"])

    assert result is not None
    assert isinstance(result,FootprintData)
    assert result.metadata["site"] == "tmb"
    assert result.metadata["data_type"] == "footprints"


@pytest.mark.parametrize("source_format", ["paris", "flexpart"])
def test_standardise_footprint_flexpart(source_format):
    """
    Checking FLEXPART footprints can be added using either "paris" or "flexpart"
    source_format where "flexpart" is an alias for "paris".
    Both should use the same parse_paris function.
    """
    # clear_test_stores()

    datapath = get_footprint_datapath("MHD-10magl_FLEXPART_ECMWFHRES_TEST_inert_201809.nc")

    site = "mhd"
    inlet = "10m"
    domain = "test"
    model = "FLEXPART"
    met_model = "ecmwfhres"

    results = standardise_footprint(
        filepath=datapath,
        site=site,
        inlet=inlet,
        domain=domain,
        model=model,
        met_model=met_model,
        source_format=source_format,
        force=True,
        if_exists="new",
        store="user",
    )

    result = results[0]
    expected_metadata = {"domain": "test", "site": "mhd", "model": "FLEXPART", "inlet": "10m"}
    for k, v in expected_metadata.items():
        assert result[k].lower() == v.lower()


def test_standardise_align_footprint():
    """
    Tests that a footprint that is read in with slightly different lat-lon coordinates
    is aligned to the 'correct' coordinates in openghg_defs
    """
    datapath = get_footprint_datapath("footprint_align_test.nc")

    site = "JFJ"
    network = "AGAGE"
    height = "1000m"
    domain = "EUROPE"
    model = "test_model"

    standardise_footprint(
        filepath=datapath,
        site=site,
        model=model,
        network=network,
        height=height,
        domain=domain,
        force=True,
        overwrite=True,
        store="user",
    )

    data = get_footprint(site=site, network=network, height=height, domain=domain, model=model)

    true_lats, true_lons = find_domain(domain=domain)

    assert np.array_equal(data.data.lat.values, true_lats)
    assert np.array_equal(data.data.lon.values, true_lons)


def test_standardise_footprints_chunk(caplog):
    datapath = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")

    site = "TAC"
    network = "DECC"
    height = "185m"
    domain = "TEST"
    model = "UKV-chunked"

    standardise_footprint(
        filepath=datapath,
        site=site,
        model=model,
        network=network,
        height=height,
        domain=domain,
        force=True,
        store="user",
        chunks={"time": 2},
    )

    search_results = search_footprints(model="UKV-chunked", store="user")

    # Note: have to pass sort=False here for dask>=2024.8 as this returns different
    # chunks for time (1, 1, 1) rather than original chunks we're trying to check.
    fp_data = search_results.retrieve_all(sort=False)

    assert dict(fp_data.data.chunks) == {"time": (2, 1), "lat": (12,), "lon": (12,), "height": (20,)}


def test_standardise_flux():
    test_datapath = get_flux_datapath("co2-gpp-cardamom_EUROPE_2012.nc")

    proc_results = standardise_flux(
        filepath=test_datapath,
        species="co2",
        source="gpp-cardamom",
        domain="europe",
        time_resolved=False,
        force=True,
        store="user",
    )

    expected_metadata = {"species": "co2", "source": "gpp-cardamom", "domain": "europe"}
    result = proc_results[0]

    for k, v in expected_metadata.items():
        assert result[k].lower() == v.lower()


def test_standardise_flux_additional_keywords():
    test_datapath = get_flux_datapath("ch4-anthro_globaledgar_v5-0_2014.nc")

    proc_results = standardise_flux(
        filepath=test_datapath,
        species="ch4",
        source="anthro",
        domain="globaledgar",
        database="EDGAR",
        database_version="v50",
        store="user",
    )

    result = proc_results[0]
    expected_metadata = {"species": "ch4", "source": "anthro", "domain": "globaledgar"}

    for k, v in expected_metadata.items():
        assert result[k].lower() == v.lower()


def test_standardise_non_standard_flux_domain():
    """
    Checks that if a non-standard domain is used, the standardisation/alignment process throws no errors.
    """
    test_datapath = get_flux_datapath("co2-gpp-cardamom-EUROPE_2012-incomplete.nc")

    # this file is sliced, to cover only a small section of the EUROPE domain
    # assert that if we specify the domain as a non-standard domain, it standardises fine:

    domain = "TEST"

    proc_results = standardise_flux(
        filepath=test_datapath,
        species="co2",
        source="gpp-cardamom",
        domain=domain,
        high_time_resolution=False,
        force=True,
        store="user",
    )

    expected_metadata = {"species": "co2", "source": "gpp-cardamom", "domain": "test"}
    result = proc_results[0]

    for k, v in expected_metadata.items():
        assert result[k].lower() == v.lower()


def test_standardise_incomplete_flux():
    """
    Checks that if a non-standard set of lat-lons is used in the input file with
    a standard domain, we get an error
    """
    test_datapath = get_flux_datapath("co2-gpp-cardamom-EUROPE_2012-incomplete.nc")

    # assert that if we specify the domain as the standard EUROPE domain with an non-standard input file,
    # we get an error

    with pytest.raises(ValueError):
        standardise_flux(
            filepath=test_datapath,
            species="co2",
            source="gpp-cardamom",
            domain="EUROPE",
            high_time_resolution=False,
            force=True,
            store="user",
        )


def test_standardise_footprint_different_chunking_schemes(caplog):
    datapath_a = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    datapath_b = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")

    clear_test_stores()

    site = "TAC"
    network = "UKV"
    height = "100m"
    domain = "TEST"
    model = "chunk_model"

    standardise_footprint(
        filepath=datapath_a,
        site=site,
        model=model,
        network=network,
        height=height,
        domain=domain,
        store="user",
        chunks={"time": 2},
    )

    standardise_footprint(
        filepath=datapath_b,
        site=site,
        model=model,
        network=network,
        height=height,
        domain=domain,
        store="user",
        chunks={"time": 2, "lat": 5, "lon": 5},
    )

    search_results = search(data_type="footprints", model="chunk_model", store="user")
    fp_data = search_results.retrieve_all()

    # Check that the chunking scheme is what was specified with the first standardise call
    assert dict(fp_data.data.chunks) == {"time": (2, 2, 2), "lat": (12,), "lon": (12,), "height": (20,)}


def test_incompatible_species_for_flux_timeseries():
    """This function tests if incompatible species values is supplied to standardise"""

    data_path = get_flux_timeseries_datapath(filename="GBR_2023_2021_13042023_170954.xlsx")
    with pytest.raises(ValueError):
        standardise_flux_timeseries(
            filepath=data_path, species="hfc123", source="crf", period="years", continuous=False, store="user"
        )


def test_standardise_flux_timeseries():
    """This function tests flux_timeseries standardisation function"""

    data_path = get_flux_timeseries_datapath(filename="GBR_2023_2021_13042023_170954.xlsx")
    flux_results = standardise_flux_timeseries(
        filepath=data_path, species="ch4", source="crf", period="years", continuous=False, store="user"
    )

    expected_metadata = {"species": "ch4", "source": "crf", "region": "uk"}
    result = flux_results[0]
    print(result)
    for k, v in expected_metadata.items():
        assert result[k].lower() == v.lower()


def test_standardise_sorting_true():
    """Testing only the sorting of files here"""

    filepaths = [
        get_surface_datapath("DECC-picarro_TAC_20130131_co2-185m-20220929.nc", source_format="openghg"),
        get_surface_datapath("DECC-picarro_TAC_20130131_co2-185m-20220928.nc", source_format="openghg"),
    ]

    results = standardise_surface(
        store="user",
        filepath=filepaths,
        source_format="OPENGHG",
        site="tac",
        network="DECC",
        instrument="picarro",
        sampling_period="1h",
        update_mismatch="attributes",
        if_exists="new",
        sort_files=True,
    )

    assert "20220928.nc" in results[0]["file"]


def test_standardise_sorting_false(caplog):
    """Testing only the sorting of files here"""

    clear_test_stores()
    filepaths = [
        get_surface_datapath("DECC-picarro_TAC_20130131_co2-185m-20220929.nc", source_format="openghg"),
        get_surface_datapath("DECC-picarro_TAC_20130131_co2-185m-20220928.nc", source_format="openghg"),
    ]

    standardise_surface(
        store="user",
        filepath=filepaths,
        source_format="OPENGHG",
        site="tac",
        network="DECC",
        instrument="picarro",
        sampling_period="1h",
        update_mismatch="attributes",
        if_exists="new",
        sort_files=False,
    )

    log_messages = [record.message for record in caplog.records]

    assert "20220928.nc" in log_messages[-1]


def test_standardise_surface_niwa(caplog):
    """Testing NIWA network file gets standardised here"""

    data = get_surface_datapath(filename="niwa.nc", source_format="NIWA")

    results = standardise_surface(
        filepath=data,
        source_format="niwa",
        network="NIWA",
        site="LAU",
        store="user",
        verify_site_code=False,
        inlet="10m",
    )

    assert "ch4" in results[0]["species"]
    assert "LAU" in results[0]["site"]
    assert "10m" in results[0]["inlet"]
    assert "niwa" in results[0]["source_format"]


def test_standardise_footprints_satellite_raises_error():
    """
    Tests standardise footprint raises value error when site and obs_region values are not supplied.
    """
    datapath = get_footprint_datapath("GOSAT-BRAZIL-column_SOUTHAMERICA_201004_compressed.nc")

    satellite = "GOSAT"
    network = "GOSAT"
    domain = "SOUTHAMERICA"

    with pytest.raises(ValueError):
        standardise_footprint(
            filepath=datapath,
            satellite=satellite,
            network=network,
            model="CAMS",
            inlet="column",
            period="1S",
            domain=domain,
            selection="LAND",
            force=True,
            store="user",
            continuous=False,
        )


def test_standardise_footprint_satellite(caplog):
    """
    Tests standardise footprint for satellite data and associated metadata keys."""
    clear_test_stores()

    datapath = get_footprint_datapath("GOSAT-BRAZIL-column_SOUTHAMERICA_201004_compressed.nc")

    satellite = "GOSAT"
    network = "GOSAT"
    domain = "SOUTHAMERICA"
    obs_region = "BRAZIL"

    standardise_footprint(
        filepath=datapath,
        satellite=satellite,
        network=network,
        model="CAMS",
        inlet="column",
        period="varies",
        domain=domain,
        obs_region=obs_region,
        selection="LAND",
        store="user",
        continuous=True,
    )

    assert "'continuous' is set to `False`" in caplog.text

    data = get_footprint(satellite=satellite, domain=domain, obs_region=obs_region)

    assert data.metadata["time_period"] == "varies"
    assert data.metadata["obs_region"] == obs_region.lower()
    assert data.metadata["selection"] == "land"
    assert data.metadata["domain"] == domain.lower()


def test_icos_corso_l1_flask_data():
    """
    Test icos corso strandardisation flow for data_level l1 and flask measurement.
    """
    filepath = get_surface_datapath(
        filename="ICOS_ATC_L1_FAST_TRACK_L1-FastTrack-2025.1_CBW_207.0_1480_FLASK.14C",
        source_format="icos_corso",
    )

    results = standardise_surface(
        filepath=filepath,
        source_format="icos_corso",
        network="icos",
        site="CBW",
        instrument="flask",
        data_level=1,
        measurement_type="flask",
        platform="surface-flask",
        store="user",
    )

    assert "dco2c14" in results[0]["species"]
    assert "cbw" in results[0]["site"]
    assert "207.0" in results[0]["inlet"]
    assert "ICOS_CORSO" in results[0]["source_format"]
    assert "surface-flask" in results[0]["platform"]

    get_corso_data = get_obs_surface(site="cbw", species="dco2c14", data_level="1", platform="surface-flask")

    data = get_corso_data.data
    metadata = get_corso_data.metadata

    fetched_value = data["mf"].isel(time=0).values
    expected_value = -20.33
    assert np.allclose(fetched_value, expected_value)

    assert data["mf_sampling_period"].isel(time=4).values == 3601
    assert data["mf_sampling_period"].attrs["units"] == "s"
    assert "flask" in metadata["measurement_type"]
    assert "3600.0" in metadata["sampling_period"]


def test_icos_corso_l2_integrated_naoh():
    """
    Test icos corso strandardisation flow for data_level l2 and integrated-naoh measurement.
    """
    filepath = get_surface_datapath(
        filename="ICOS_ATC_L2_L2-2024.1_CBW_207.0_779.14C", source_format="icos_corso"
    )

    results = standardise_surface(
        filepath=filepath,
        source_format="icos_corso",
        network="icos",
        site="CBW",
        instrument="integrated-NAOH",
        data_level=2,
        measurement_type="integrated-NAOH",
        platform="surface-flask",
        store="user",
    )

    assert "dco2c14" in results[0]["species"]
    assert "cbw" in results[0]["site"]
    assert "207.0" in results[0]["inlet"]
    assert "ICOS_CORSO" in results[0]["source_format"]
    assert "surface-flask" in results[0]["platform"]
    assert "2" in results[0]["data_level"]

    get_corso_data = get_obs_surface(site="cbw", species="dco2c14", data_level="2", platform="surface-flask")

    data = get_corso_data.data
    metadata = get_corso_data.metadata

    fetched_value = data["mf"].isel(time=0).values
    expected_value = -0.84
    assert np.allclose(fetched_value, expected_value)
    assert data["mf_sampling_period"].isel(time=0).values == 1209600.0
    assert data["mf_sampling_period"].attrs["units"] == "s"

    assert "integrated-naoh" in metadata["measurement_type"]
    assert "multiple" in metadata["sampling_period"]


def test_icos_corso_l2_flask():
    """
    Test icos corso strandardisation flow for data_level l2 and flask measurement.
    """
    filepath = get_surface_datapath(
        filename="ICOS_ATC_L2_L2-2024.1_CBW_207.0_1480_FLASK.14C", source_format="icos_corso"
    )

    results = standardise_surface(
        filepath=filepath,
        source_format="icos_corso",
        network="icos",
        site="CBW",
        instrument="flask",
        data_level=2,
        platform="surface-flask",
        store="user",
    )

    assert "dco2c14" in results[0]["species"]
    assert "cbw" in results[0]["site"]
    assert "207.0" in results[0]["inlet"]
    assert "ICOS_CORSO" in results[0]["source_format"]
    assert "surface-flask" in results[0]["platform"]
    assert "2" in results[0]["data_level"]

    get_corso_data = get_obs_surface(
        site="cbw", species="dco2c14", data_level="2", instrument="flask", platform="surface-flask"
    )

    data = get_corso_data.data
    metadata = get_corso_data.metadata

    fetched_value = data["mf"].isel(time=0).values

    expected_value = -59.87
    assert np.allclose(fetched_value, expected_value)

    assert data["mf_sampling_period"].isel(time=5).values == 3600
    assert data["mf_sampling_period"].attrs["units"] == "s"
    assert "flask" in metadata["measurement_type"]
    assert "3600.0" in metadata["sampling_period"]


def test_icos_corso_clean_14_day():
    """
    Test icos corso strandardisation flow for clean data and integrated-naoh measurement.
    """
    filepath = get_surface_datapath(
        filename="uheicrl_l2_2025_1_jfj_5m_int_14day_clean.c14", source_format="icos_corso"
    )

    results = standardise_surface(
        filepath=filepath,
        source_format="icos_corso",
        network="icos",
        site="jfj",
        instrument="integrated-NAOH",
        data_level=2,
        platform="surface-flask",
        store="user",
    )

    assert "dco2c14" in results[0]["species"]
    assert "jfj" in results[0]["site"]
    assert "5m" in results[0]["inlet"]
    assert "ICOS_CORSO" in results[0]["source_format"]
    assert "2" in results[0]["data_level"]
    assert "surface-flask" in results[0]["platform"]

    get_corso_data = get_obs_surface(site="jfj", species="dco2c14", inlet="5m", platform="surface-flask")

    data = get_corso_data.data
    metadata = get_corso_data.metadata

    fetched_value = data["mf"].isel(time=0)
    assert data["mf_sampling_period"].isel(time=0).values == 1036800.0
    assert data["mf_sampling_period"].attrs["units"] == "s"

    expected_value = 189
    assert np.allclose(fetched_value, expected_value)
    assert "multiple" in metadata["sampling_period"]
    assert "integrated-naoh" in metadata["instrument"]


def test_icos_corso_l2_deltao2():
    """To test level 2 icos corso data for deltao2n2 species"""

    filepath = get_surface_datapath(
        filename="ICOS_ATC_L2_L2-2024.1_CBW_207.0_1667_FLASK.DELTAO2N2", source_format="icos_corso"
    )

    results = standardise_surface(
        filepath=filepath,
        source_format="icos_corso",
        network="icos",
        site="cbw",
        instrument="flask",
        data_level=2,
        platform="surface-flask",
        store="user",
    )

    assert "deltao2n2" in results[0]["species"]
    assert "cbw" in results[0]["site"]
    assert "ICOS_CORSO" in results[0]["source_format"]
    assert "2" in results[0]["data_level"]
    assert "surface-flask" in results[0]["platform"]
