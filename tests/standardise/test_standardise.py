from pathlib import Path
import pytest
from helpers import (
    get_flux_datapath,
    get_footprint_datapath,
    get_surface_datapath,
    get_column_datapath,
    get_flux_timeseries_datapath,
    clear_test_stores,
    clear_test_store,
)
from openghg.retrieve import get_obs_surface, search, get_footprint

from openghg.standardise import (
    standardise_column,
    standardise_flux,
    standardise_footprint,
    standardise_surface,
    standardise_flux_timeseries,
)
from openghg.types import AttrMismatchError, ObjectStoreError
from openghg.util import compress, find_domain
import numpy as np


def test_standardise_to_read_only_store():
    hfd_path = get_surface_datapath(filename="hfd.picarro.1minute.100m.min.dat", source_format="CRDS")

    with pytest.raises(ObjectStoreError):
        standardise_surface(
            filepaths=hfd_path,
            site="hfd",
            instrument="picarro",
            network="DECC",
            source_format="CRDS",
            overwrite=True,
            store="shared",
        )


def test_standardise_obs_two_writable_stores():
    hfd_path = get_surface_datapath(filename="hfd.picarro.1minute.100m.min.dat", source_format="CRDS")

    results = standardise_surface(
        filepaths=hfd_path,
        site="hfd",
        instrument="picarro",
        network="DECC",
        source_format="CRDS",
        force=True,
        store="user",
    )

    results = results["processed"]["hfd.picarro.1minute.100m.min.dat"]

    assert "error" not in results
    assert "ch4" in results
    assert "co2" in results

    results = search(site="hfd", inlet="100m", store="user")
    assert results
    results = search(site="hfd", inlet="100m", store="group")
    assert not results

    mhd_path = get_surface_datapath(filename="mhd.co.hourly.g2401.15m.dat", source_format="ICOS")

    results = standardise_surface(
        filepaths=mhd_path,
        site="mhd",
        inlet="15m",
        instrument="g2401",
        network="ICOS",
        source_format="ICOS",
        store="group",
    )

    assert "co" in results["processed"]["mhd.co.hourly.g2401.15m.dat"]

    results = search(site="mhd", instrument="g2401", store="group")
    assert results
    results = search(site="mhd", instrument="g2401", store="user")
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
        filepaths=filepath,
        site="TAC",
        network="DECC",
        inlet=185,
        instrument="picarro",
        source_format="openghg",
        sampling_period="1H",
        force=True,
        store="user",
    )

    results = results["processed"]["DECC-picarro_TAC_20130131_co2-185m-20220929_cut.nc"]

    assert "error" not in results
    assert "co2" in results


def test_standardise_obs_metadata_mismatch():
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
        filepaths=filepath,
        site="TAC",
        network="DECC",
        inlet="999m",
        instrument="picarro",
        source_format="openghg",
        sampling_period="1H",
        update_mismatch=update_mismatch,
        overwrite=True,
        store="user",
    )

    # Check data has been successfully processed
    results = results["processed"][filename]

    assert "error" not in results
    assert "co2" in results

    # Check retrieved data from the object store contains the updated metadata
    data = get_obs_surface(site="TAC", inlet="999m", species="co2")
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
        filepaths=filepath,
        site="TAC",
        network="DECC",
        inlet="998m",
        instrument="picarro",
        source_format="openghg",
        sampling_period="1H",
        update_mismatch=update_mismatch,
        store="user",
    )

    # Check data has been successfully processed
    results = results["processed"][filename]

    assert "error" not in results
    assert "co2" in results

    # Check retrieved data from the object store contains the updated metadata
    data = get_obs_surface(site="TAC", inlet="998m", species="co2")
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
            sampling_period="1H",
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
    filepath = get_column_datapath(filename="gosat-fts_gosat_20170318_ch4-column.nc")

    satellite = "GOSAT"
    domain = "BRAZIL"
    species = "methane"

    results = standardise_column(
        filepath=filepath,
        source_format="OPENGHG",
        satellite=satellite,
        domain=domain,
        species=species,
        force=True,
        store="user",
    )

    assert "error" not in results
    assert "ch4" in results  # Should this be a more descriprive key?


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
        force=True,
        high_spatial_resolution=True,
        overwrite=True,
        store="user",
    )

    assert "error" not in results
    assert "tmb_europe_test_model_10m" in results


def test_standardise_align_footprint():
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

    assert np.allclose(data.data.lat.values, true_lats, rtol=0, atol=1e-15)
    assert np.allclose(data.data.lon.values, true_lons, rtol=0, atol=1e-15)


from openghg.retrieve import search_footprints


def test_standardise_footprints_chunk(caplog):
    datapath = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")

    site = "TAC"
    network = "DECC"
    height = "185m"
    domain = "test_europe"
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
    fp_data = search_results.retrieve_all()

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

    assert "co2_gpp-cardamom_europe" in proc_results


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

    assert "ch4_anthro_globaledgar" in proc_results


def test_standardise_non_standard_flux_domain():
    test_datapath = get_flux_datapath("co2-gpp-cardamom-EUROPE_2012-incomplete.nc")

    # this file is sliced, to cover only a small section of the EUROPE domain
    # assert that if we specify the domain as a non-standard domain, it standardises fine:

    domain = "europe_incomplete"

    proc_results = standardise_flux(
        filepath=test_datapath,
        species="co2",
        source="gpp-cardamom",
        domain=domain,
        high_time_resolution=False,
        force=True,
        store="user",
    )

    assert "co2_gpp-cardamom_europe_incomplete" in proc_results
    assert "error" not in proc_results


def test_standardise_incomplete_flux():
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


def test_cloud_standardise(monkeypatch, mocker, tmpdir):
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


def test_standardise_footprint_different_chunking_schemes(caplog):
    datapath_a = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    datapath_b = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")

    clear_test_stores()

    site = "TAC"
    network = "UKV"
    height = "100m"
    domain = "test_europe"
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

    assert "ch4_crf_uk" in flux_results
