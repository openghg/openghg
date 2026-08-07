"""Explicit object-store data bundles for analyse integration tests.

Fixtures in this module populate the default ``user`` store and return that
store name so consumers can pass it to retrieval functions. Request the
narrowest bundle that supplies a test's stored-data requirements:

* ``tac_surface_store``: TAC 2012 CRDS surface observations (CH4 and CO2).
* ``tac_ch4_store``: TAC surface, anthropogenic and waste fluxes, boundary
  conditions, and the European footprint.
* ``tac_co2_store``: TAC surface, natural and ocean fluxes, boundary
  conditions, and the CO2 footprint.
* ``wao_radon_store``: WAO radon surface observations and footprint.
* ``satellite_cams_store``: GOSAT column observations and CAMS footprint.
* ``satellite_name_store``: GOSAT column observations, NAME footprint, and
  South American flux.

Synthetic tests should not request any of these fixtures. The bundles are
session-scoped and intended for read-only consumers, so tests that clear or
mutate an object store must use isolated store configuration rather than
invalidating them.
"""

import pytest
from helpers import (
    get_bc_datapath,
    get_column_datapath,
    get_flux_datapath,
    get_footprint_datapath,
    get_surface_datapath,
)

from openghg.standardise import (
    standardise_bc,
    standardise_column,
    standardise_flux,
    standardise_footprint,
    standardise_surface,
)


@pytest.fixture(scope="session")
def tac_surface_store(default_test_store: str) -> str:
    """Populate the default session store with TAC 2012 surface observations.

    Args:
        default_test_store: Writable store name to populate.

    Returns:
        The populated store name.
    """
    filepath = get_surface_datapath(filename="tac.picarro.1minute.100m.201208.dat", source_format="CRDS")
    standardise_surface(
        store=default_test_store,
        filepath=filepath,
        source_format="CRDS",
        site="tac",
        network="DECC",
    )
    return default_test_store


@pytest.fixture(scope="session")
def tac_ch4_store(tac_surface_store: str) -> str:
    """Extend the TAC surface store with the CH4 scenario data.

    Args:
        tac_surface_store: Session store containing TAC surface observations.

    Returns:
        The populated store name.
    """
    store = tac_surface_store
    for source, filename in (
        ("anthro", "ch4-anthro_EUROPE_2012.nc"),
        ("waste", "ch4-ukghg-waste_EUROPE_2012.nc"),
    ):
        standardise_flux(
            store=store,
            filepath=get_flux_datapath(filename),
            species="ch4",
            source=source,
            domain="EUROPE",
            time_resolved=False,
        )

    standardise_bc(
        store=store,
        filepath=get_bc_datapath("ch4_EUROPE_201208.nc"),
        species="ch4",
        domain="EUROPE",
        bc_input="MOZART",
        period="monthly",
    )
    standardise_footprint(
        store=store,
        filepath=get_footprint_datapath("TAC-100magl_EUROPE_201208.nc"),
        site="tac",
        model="NAME",
        network="DECC",
        height="100m",
        domain="EUROPE",
    )
    return store


@pytest.fixture(scope="session")
def tac_co2_store(default_test_store: str) -> str:
    """Populate the default session store with the TAC CO2 scenario data.

    Args:
        default_test_store: Writable store name to populate.

    Returns:
        The populated store name.
    """
    store = default_test_store
    standardise_surface(
        store=store,
        filepath=get_surface_datapath(filename="tac.picarro.1minute.100m.201407.dat", source_format="CRDS"),
        source_format="CRDS",
        site="tac",
        network="DECC",
    )

    standardise_flux(
        store=store,
        filepath=get_flux_datapath("co2-rtot-cardamom-2hr_TEST_2014.nc"),
        species="co2",
        source="natural-rtot",
        domain="TEST",
        time_resolved=True,
    )
    for filename in (
        "co2-nemo-ocean-mth_TEST_2013.nc",
        "co2-nemo-ocean-mth_TEST_2014.nc",
    ):
        standardise_flux(
            store=store,
            filepath=get_flux_datapath(filename),
            species="co2",
            source="ocean",
            domain="TEST",
            time_resolved=False,
            period="1 month",
        )

    standardise_bc(
        store=store,
        filepath=get_bc_datapath("co2_TEST_201407.nc"),
        species="co2",
        domain="TEST",
        bc_input="MOZART",
        period="monthly",
    )
    standardise_footprint(
        store=store,
        filepath=get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc"),
        site="tac",
        model="NAME",
        network="DECC",
        met_model="UKV",
        height="100m",
        domain="TEST",
        species="co2",
    )
    return store


@pytest.fixture(scope="session")
def wao_radon_store(default_test_store: str) -> str:
    """Populate the default session store with WAO radon data.

    Args:
        default_test_store: Writable store name to populate.

    Returns:
        The populated store name.
    """
    store = default_test_store
    standardise_surface(
        store=store,
        filepath=get_surface_datapath(
            filename="wao_rn_icos_standardised_2021-12-04.nc", source_format="OPENGHG"
        ),
        source_format="OPENGHG",
        site="wao",
        network="ICOS",
        inlet="10m",
        update_mismatch="metadata",
    )
    standardise_footprint(
        store=store,
        filepath=get_footprint_datapath("WAO-20magl_UKV_rn_TEST_202112.nc"),
        site="wao",
        model="NAME",
        network="ICOS",
        height="20m",
        domain="TEST",
        species="rn",
    )
    return store


@pytest.fixture(scope="session")
def satellite_cams_store(default_test_store: str) -> str:
    """Populate the default session store with GOSAT/CAMS validation data.

    Args:
        default_test_store: Writable store name to populate.

    Returns:
        The populated store name.
    """
    store = default_test_store
    standardise_column(
        store=store,
        filepath=get_column_datapath(filename="gosat-fts_gosat_20170318_ch4-column.nc"),
        source_format="OPENGHG",
        satellite="GOSAT",
        species="CH4",
        obs_region="BRAZIL",
        selection="LAND",
    )
    standardise_footprint(
        store=store,
        filepath=get_footprint_datapath("GOSAT-BRAZIL-column_SOUTHAMERICA_201004_compressed.nc"),
        satellite="GOSAT",
        network="GOSAT",
        model="CAMS",
        inlet="column",
        period="1S",
        domain="SOUTHAMERICA",
        obs_region="BRAZIL",
        selection="LAND",
        continuous=False,
    )
    return store


@pytest.fixture(scope="session")
def satellite_name_store(default_test_store: str) -> str:
    """Populate the default session store with GOSAT/NAME merge data.

    Args:
        default_test_store: Writable store name to populate.

    Returns:
        The populated store name.
    """
    store = default_test_store
    standardise_column(
        store=store,
        filepath=get_column_datapath("gosat-fts_gosat_20160101_ch4-column.nc"),
        species="ch4",
        platform="satellite",
        satellite="gosat",
        obs_region="brazil",
        network="gosat",
    )
    standardise_footprint(
        store=store,
        filepath=get_footprint_datapath("GOSAT-BRAZIL-column_SOUTHAMERICA_201601.nc"),
        model="name",
        domain="southamerica",
        satellite="gosat",
        obs_region="brazil",
        inlet="column",
    )
    standardise_flux(
        store=store,
        filepath=get_flux_datapath("ch4-all_SOUTHAMERICA_2016_SWAMPS-v32-5_Saunois-Annual-Mean_20160101.nc"),
        species="ch4",
        source="all",
        domain="southamerica",
    )
    return store
