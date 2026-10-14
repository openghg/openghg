"""Test shared-footprint alignment and calculations using existing file-backed data.

No synthetic observations, fluxes or footprints are needed here. Scaling copies
of retrieved sensitivities makes it possible to distinguish an explicit input
from the cached input without changing the stored data.
"""

from copy import deepcopy
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from helpers import get_footprint_datapath

from openghg.analyse import ModelScenario, make_integrated_low_freq_flux
from openghg.analyse._modelled_obs import fp_x_flux_integrated, time_resolved_and_residual_footprints
from openghg.dataobjects import FootprintData
from openghg.retrieve import get_flux, get_footprint, get_obs_column
from openghg.standardise import standardise_footprint
from openghg.standardise.footprints import parse_acrg_org, parse_paris


_STORES = {
    "co2": "shared-footprint-co2",
    "integrated": "shared-footprint-integrated",
    "satellite": "shared-footprint-satellite",
}


@pytest.fixture(scope="session")
def shared_footprint_store_config(
    tmp_path_factory, mock_configuration_paths, tac_co2_store_factory, satellite_name_store_factory
):
    """Populate dedicated stores that other tests cannot clear or re-standardise.

    Reuse the existing file bundles, but keep their paths separate from the
    user/group stores reset by direct-input tests. Do not modify session config.
    """
    config = deepcopy(mock_configuration_paths)
    root = tmp_path_factory.mktemp("shared-footprint-stores")
    for store in _STORES.values():
        config["object_store"][store] = {"path": str(root / store), "permissions": "rw"}

    with patch("openghg.objectstore._local_store.read_local_config", return_value=config):
        tac_co2_store_factory(_STORES["co2"])
        satellite_name_store_factory(_STORES["satellite"])
        standardise_footprint(
            filepath=get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc"),
            site="tac",
            model="NAME",
            network="DECC",
            met_model="UKV",
            height="100m",
            domain="TEST",
            species="co2",
            time_resolved=False,
            store=_STORES["integrated"],
        )
    return config


@pytest.fixture
def shared_footprint_stores(shared_footprint_store_config, mocker):
    """Expose dedicated stores only while a shared-footprint test is running."""
    mocker.patch(
        "openghg.objectstore._local_store.read_local_config", return_value=shared_footprint_store_config
    )
    return _STORES


@pytest.fixture
def stored_co2_scenario(shared_footprint_stores):
    """Retrieve TAC observations, hourly footprints and both real CO2 flux sources."""
    model = ModelScenario(
        site="tac",
        species="co2",
        inlet="100m",
        network="DECC",
        domain="TEST",
        sources=["natural-rtot", "ocean"],
        start_date="2014-07-01",
        end_date="2014-07-04",
        time_resolved=True,
        store=shared_footprint_stores["co2"],
    )
    # A missing source would turn mixed-sector coverage into a vacuous test.
    assert set(model.fluxes) == {"natural-rtot", "ocean"}
    # These files are already reduced test datasets. Load each retrieved object
    # once so repeated equality checks do not rebuild large lazy task graphs.
    model.obs.data.load()
    model.footprint.data.load()
    for flux in model.fluxes.values():
        flux.data.load()
    return model


@pytest.fixture
def stored_integrated_co2_scenario(stored_co2_scenario, shared_footprint_stores):
    """Use real TAC observations and CO2 fluxes with an integrated footprint search."""
    model = ModelScenario(
        obs=stored_co2_scenario.obs,
        flux=stored_co2_scenario.fluxes,
        bc=stored_co2_scenario.bc,
        domain="TEST",
        fp_inlet="100m",
        time_resolved=False,
        start_date="2014-07-01",
        end_date="2014-07-04",
        store=shared_footprint_stores["integrated"],
    )
    model.footprint.data.load()
    return model


@pytest.fixture
def stored_satellite_scenario(shared_footprint_stores):
    """Retrieve the existing irregular GOSAT soundings and matching NAME data."""
    model = ModelScenario(
        obs_column=get_obs_column(
            species="ch4",
            satellite="gosat",
            obs_region="brazil",
            max_level=17,
            start_date="2016-01-01 14:59:12.500000+00:00",
            end_date="2016-01-01 18:10:16.500000+00:00",
            store=shared_footprint_stores["satellite"],
        ),
        footprint=get_footprint(
            satellite="gosat",
            obs_region="brazil",
            domain="southamerica",
            model="name",
            start_date="2016-01-01 14:59:12.500000+00:00",
            end_date="2016-01-01 19:10:16.500000+00:00",
            store=shared_footprint_stores["satellite"],
        ),
        flux=get_flux(
            species="ch4", source="all", domain="southamerica", store=shared_footprint_stores["satellite"]
        ),
        platform="satellite",
    )
    model.obs.data.load()
    model.footprint.data.load()
    for flux in model.fluxes.values():
        flux.data.load()
    return model


@pytest.mark.parametrize("use_low_freq_flux", [None, False])
def test_integrated_calculation_uses_explicit_scenario(stored_integrated_co2_scenario, use_low_freq_flux):
    """An explicit footprint controls calculation even when another scenario is cached."""
    model = stored_integrated_co2_scenario
    cached = model.combine_obs_footprint()
    expected = model._calc_modelled_obs_integrated(output_fpXflux=True, use_low_freq_flux=use_low_freq_flux)
    alternate = cached.copy(deep=True)
    alternate["fp"] = alternate.fp * 2
    alternate.fp.attrs = cached.fp.attrs.copy()

    result = model._calc_modelled_obs_integrated(
        scenario=alternate, output_fpXflux=True, use_low_freq_flux=use_low_freq_flux
    )

    assert np.isfinite(result.mf_mod).all()
    assert np.any(result.mf_mod != 0)
    xr.testing.assert_allclose(result, expected * 2)
    assert model.scenario is cached
    model.scenario = None
    xr.testing.assert_identical(
        model._calc_modelled_obs_integrated(
            scenario=alternate, output_fpXflux=True, use_low_freq_flux=use_low_freq_flux
        ),
        result,
    )
    assert model.scenario is None


@pytest.mark.parametrize("representation", ["legacy", "resolved_and_residual"])
def test_hitres_calculation_uses_explicit_scenario(stored_co2_scenario, representation):
    """Both representations of the existing TAC sensitivities accept explicit scenarios."""
    model = stored_co2_scenario
    cached = model.combine_obs_footprint()
    expected = model._calc_modelled_obs_HiTRes(output_fpXflux=True)
    alternate = cached.copy(deep=True)
    if representation == "resolved_and_residual":
        # Reuse the production conversion of the real legacy footprint; do not
        # manufacture time-resolved sensitivities or shift dates to match PARIS.
        components = time_resolved_and_residual_footprints(alternate.fp_HiTRes)
        alternate = alternate.drop_vars("fp_HiTRes").merge(components)
    for name in ("fp_HiTRes", "fp_time_resolved", "fp_residual"):
        if name in alternate:
            attrs = alternate[name].attrs.copy()
            alternate[name] = alternate[name] * 2
            alternate[name].attrs = attrs

    result = model._calc_modelled_obs_HiTRes(scenario=alternate, output_fpXflux=True)

    assert np.isfinite(result.mf_mod_high_res).all()
    assert np.any(result.mf_mod_high_res != 0)
    xr.testing.assert_allclose(result, expected * 2)
    assert model.scenario is cached
    model.scenario = None
    xr.testing.assert_identical(
        model._calc_modelled_obs_HiTRes(scenario=alternate, output_fpXflux=True), result
    )
    assert model.scenario is None


def test_public_time_resolved_calculation_unchanged(stored_co2_scenario):
    """The default CO2 path still uses HiTRes with existing natural and ocean fluxes."""
    model = stored_co2_scenario
    result = model.calc_modelled_obs()

    assert "mf_mod_high_res" in result
    assert np.isfinite(result.mf_mod_high_res).all()
    assert np.any(result.mf_mod_high_res != 0)
    expected = model._calc_modelled_obs_HiTRes()
    xr.testing.assert_allclose(result, model.convert_units(expected))
    assert model.modelled_obs is result


@pytest.mark.xfail(
    strict=True,
    raises=ValueError,
    reason="Existing on devel: real time-resolved CO2 fp_x_flux retains incompatible flux units.",
)
def test_public_time_resolved_footprint_flux_output(stored_co2_scenario):
    """Keep the existing optional-output unit failure visible for a separate fix."""
    stored_co2_scenario.calc_modelled_obs(output_fp_x_flux=True)


@pytest.mark.parametrize("resample_to", [None, "2h"])
@pytest.mark.xfail(
    strict=True,
    raises=ValueError,
    reason="Existing on devel: real TAC lag coordinates produce unequal rolling-window and H_back lengths.",
)
def test_public_time_resolved_alternative_resampling(stored_co2_scenario, resample_to):
    """Keep the existing lag-window limitation visible without changing the kernel."""
    stored_co2_scenario.calc_modelled_obs(resample_to=resample_to)


@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize("representation", ["integrated", "legacy", "paris"])
def test_footprint_resolution_detection(representation, wrapped):
    """Parse actual footprint files; CO2 metadata alone must not select HiTRes."""
    if representation == "paris":
        parsed = parse_paris(
            filepath=get_footprint_datapath("MHD-10magl_NAME_UKV_TEST_co2_PARIS-format_201502.nc"),
            site="mhd",
            inlet="10m",
            model="NAME",
            met_model="ukv",
            species="co2",
            domain="EUROPE",
        )
    else:
        parsed = parse_acrg_org(
            filepath=get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc"),
            site="tac",
            inlet="100m",
            model="NAME",
            met_model="ukv",
            species="co2",
            domain="TEST",
            time_resolved=representation == "legacy",
        )
    entry = next(iter(parsed.values()))
    footprint = FootprintData(data=entry["data"], metadata=entry["metadata"]) if wrapped else entry["data"]

    assert ModelScenario._footprint_is_time_resolved(footprint) is (representation != "integrated")


def test_hitres_fallback_uses_explicit_integrated_scenario(stored_integrated_co2_scenario):
    """The HiTRes fallback forwards the supplied integrated footprint."""
    model = stored_integrated_co2_scenario
    aligned = model.combine_obs_footprint(cache=False)
    expected = model._calc_modelled_obs_integrated(scenario=aligned, ts_name="custom", output_fpXflux=True)

    result = model._calc_modelled_obs_HiTRes(scenario=aligned, ts_name="custom", output_fpXflux=True)

    xr.testing.assert_identical(result, expected)
    assert model.scenario is None


@pytest.mark.parametrize("resample_to", [None, "coarsest", "obs", "footprint", "2h"])
def test_explicit_footprint_alignment_preserves_attached_data(stored_integrated_co2_scenario, resample_to):
    """Align a shared footprint without replacing the attached data or caches."""
    model = stored_integrated_co2_scenario
    attached = model.footprint
    original_footprint = attached.data.copy(deep=True)
    original_obs = model.obs.data.copy(deep=True)
    cached_result = model.calc_modelled_obs(resample_to=resample_to)
    cached = model.scenario
    alternate = FootprintData(data=attached.data.copy(deep=True), metadata=attached.metadata.copy())
    alternate.data["fp"] = alternate.data.fp * 3
    alternate.data.fp.attrs = attached.data.fp.attrs.copy()
    reference = ModelScenario(obs=model.obs, footprint=alternate, flux=model.fluxes)

    result = model._scenario_for_footprint(alternate, resample_to=resample_to)

    xr.testing.assert_identical(result, reference.combine_obs_footprint(resample_to=resample_to))
    assert model.footprint is attached
    assert model.scenario is cached
    assert model.modelled_obs is cached_result
    xr.testing.assert_identical(attached.data, original_footprint)
    xr.testing.assert_identical(model.obs.data, original_obs)


@pytest.mark.parametrize("resample_to", [None, "coarsest", "2h"])
def test_explicit_footprint_alignment_without_observations(stored_integrated_co2_scenario, resample_to):
    """Footprint-only alignment uses the explicit input without populating caches."""
    footprint = stored_integrated_co2_scenario.footprint
    model = ModelScenario()
    result = model._scenario_for_footprint(footprint, resample_to=resample_to)

    expected = footprint.data
    if resample_to == "2h":
        expected = expected.resample(time="2h").mean()
    xr.testing.assert_identical(result, expected)
    assert model.footprint is None
    assert model.scenario is None


def test_explicit_alignment_without_attached_footprint(stored_integrated_co2_scenario):
    """Explicit footprints replace only the attached-footprint requirement."""
    reference = stored_integrated_co2_scenario
    model = ModelScenario(obs=reference.obs)
    footprint = reference.footprint

    result = model._scenario_for_footprint(footprint)
    xr.testing.assert_identical(result, reference.combine_obs_footprint(cache=False))
    assert model.footprint is None
    assert model.scenario is None

    for operation in (model.combine_obs_footprint, model._resample_obs_footprint, model.calc_modelled_obs):
        with pytest.raises(ValueError, match="Missing necessary footprint"):
            operation()


def test_observations_still_required_for_resampling(stored_integrated_co2_scenario):
    """Explicit footprints do not bypass observation checks on the resampling path."""
    model = ModelScenario(footprint=stored_integrated_co2_scenario.footprint)
    for kwargs in ({}, {"footprint": model.footprint}):
        with pytest.raises(ValueError, match="Missing necessary obs"):
            model._resample_obs_footprint(**kwargs)
    with pytest.raises(ValueError, match="Missing necessary obs"):
        model.combine_obs_footprint()


@pytest.mark.parametrize("method", ["_calc_modelled_obs_integrated", "_calc_modelled_obs_HiTRes"])
def test_calculations_still_require_aligned_data(method):
    """Default calculation paths retain their original missing-scenario error."""
    with pytest.raises(ValueError, match="Combined data must have been defined"):
        getattr(ModelScenario(), method)()


def test_explicit_satellite_alignment_keeps_irregular_sounding_times(stored_satellite_scenario):
    """GOSAT alignment retains actual sounding times rather than averaging them."""
    model = stored_satellite_scenario
    cached = model.combine_obs_footprint()
    alternate = FootprintData(
        data=model.footprint.data.copy(deep=True), metadata=model.footprint.metadata.copy()
    )
    alternate.data["fp"] = alternate.data.fp * 3
    alternate.data.fp.attrs = model.footprint.data.fp.attrs.copy()

    result = model._scenario_for_footprint(alternate, resample_to="2h")

    np.testing.assert_array_equal(result.time.values, cached.time.values)
    assert result.sizes["time"] > 1
    assert not np.all(np.diff(result.time.values) == np.diff(result.time.values)[0])
    xr.testing.assert_allclose(result.fp, cached.fp * 3)
    assert result.attrs["resample_to"] == "None"
    assert model.scenario is cached


def test_explicit_satellite_alignment_preserves_tolerance(stored_satellite_scenario):
    """Perturb real GOSAT times to exercise the existing nearest-match tolerance."""
    model = stored_satellite_scenario
    cached = model.combine_obs_footprint()
    alternate = FootprintData(
        data=model.footprint.data.copy(deep=True), metadata=model.footprint.metadata.copy()
    )
    # Match the first sounding inside 1 ms and put all other footprints outside
    # tolerance. Unequal lengths force reindexing rather than the existing
    # approximate-index equality shortcut in combine_datasets.
    times = cached.time.values.copy() + pd.Timedelta("2ms").to_timedelta64()
    times[0] = cached.time.values[0] + pd.Timedelta("0.5ms").to_timedelta64()
    alternate.data = alternate.data.isel(time=slice(0, len(times))).assign_coords(time=times)
    extra = alternate.data.isel(time=slice(-1, None)).assign_coords(time=[times[-1] + np.timedelta64(1, "h")])
    alternate.data = xr.concat([alternate.data, extra], dim="time")

    result = model._scenario_for_footprint(alternate, resample_to="2h")

    np.testing.assert_array_equal(result.time.values, cached.time.values[:1])
    xr.testing.assert_allclose(result.fp.isel(time=0, drop=True), alternate.data.fp.isel(time=0, drop=True))
    assert model.scenario is cached


def test_surface_integrated_co2_retrieval_and_calculation(stored_integrated_co2_scenario):
    """Real integrated retrieval uses monthly means and retains existing sector outputs."""
    model = stored_integrated_co2_scenario
    assert model.footprint is not None
    assert model.footprint.metadata["species"] == "co2"
    assert str(model.footprint.metadata["time_resolved"]).lower() == "false"
    assert "fp_HiTRes" not in model.footprint.data

    result = model.footprints_data_merge(calc_bc=False, calc_fp_x_flux=True, split_by_sectors=True)

    assert "mf_mod" in result
    assert "mf_mod_high_res" not in result
    assert np.isfinite(result.mf_mod).all()
    assert np.any(result.mf_mod != 0)
    flux_monthly = make_integrated_low_freq_flux(model.combine_flux_sources())
    expected = (
        fp_x_flux_integrated(result, flux_monthly).pint.quantify().sum(["lat", "lon"]).pint.dequantify()
    )
    expected = model.convert_units(expected.to_dataset(name="mf_mod"))
    xr.testing.assert_allclose(result.mf_mod, expected.mf_mod)
    for source in model.flux_sources:
        sector = model._calc_modelled_obs_integrated(
            sources=source,
            scenario=model.scenario,
            output_fpXflux=True,
        )
        sector = model.convert_units(sector)
        xr.testing.assert_allclose(result.mf_mod_sectoral.sel(source=source, drop=True), sector.mf_mod)
        xr.testing.assert_allclose(result.fp_x_flux_sectoral.sel(source=source, drop=True), sector.fp_x_flux)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Existing on devel: mixed-cadence flux sources with different time coverage give inconsistent sector totals.",
)
def test_mixed_cadence_integrated_co2_sector_totals(stored_integrated_co2_scenario):
    """Track the real-data limitation without changing scientific behaviour in this refactor.

    The shared-flux path aligns sources before taking monthly means; the sector
    paths take monthly means independently. The existing CARDAMOM and NEMO
    files expose different results. This is not caused by explicit scenarios.
    """
    result = stored_integrated_co2_scenario.footprints_data_merge(
        calc_bc=False, calc_fp_x_flux=True, split_by_sectors=True
    )
    xr.testing.assert_allclose(result.mf_mod_sectoral.sum("source"), result.mf_mod)
    xr.testing.assert_allclose(result.fp_x_flux_sectoral.sum("source"), result.fp_x_flux)


def test_public_footprint_only_calculation_unchanged(stored_integrated_co2_scenario):
    """The public modelling path still supports footprints and fluxes without obs."""
    reference = stored_integrated_co2_scenario
    model = ModelScenario(footprint=reference.footprint, flux=reference.fluxes)

    result = model.calc_modelled_obs(resample_to="2h", output_fp_x_flux=True)

    expected_scenario = reference.footprint.data.resample(time="2h").mean()
    xr.testing.assert_identical(model.scenario, expected_scenario)
    expected = model._calc_modelled_obs_integrated(scenario=expected_scenario, output_fpXflux=True)
    xr.testing.assert_allclose(result, model.convert_units(expected))


@pytest.fixture
def sector_co2_scenario(stored_co2_scenario):
    """TAC scenario with units declared on its lag-resolved sensitivities.

    The reduced legacy footprint file omits units from ``fp_HiTRes``, although
    its lag slices have the same sensitivity units as ``fp``. The metadata is
    added to a local copy so the stored data and earlier regression tests remain
    unchanged.
    """
    model = stored_co2_scenario
    model.footprint.data = model.footprint.data.copy(deep=True)
    model.footprint.data.fp_HiTRes.attrs["units"] = model.footprint.data.fp.attrs["units"]
    return model


def expected_sector_contribution(model, source, resolved, use_low_freq_flux=None):
    """Reference contribution from the existing integrated or lag-resolved kernel."""
    calculate = model._calc_modelled_obs_HiTRes if resolved else model._calc_modelled_obs_integrated
    with xr.set_options(keep_attrs=True):
        contribution = calculate(
            sources=source,
            scenario=model.scenario,
            output_TS=False,
            output_fpXflux=True,
            use_low_freq_flux=use_low_freq_flux,
        ).fp_x_flux.pint.quantify()
    return contribution.pint.to("mol/mol")


def test_mixed_sector_routing_reuses_one_footprint(sector_co2_scenario, mocker):
    """Biological and monthly ocean fluxes use different kernels, not different footprints."""
    model = sector_co2_scenario
    footprint = model.footprint
    original = footprint.data.copy(deep=True)
    keep_attrs = xr.get_options()["keep_attrs"]
    alignment = mocker.spy(model, "_scenario_for_footprint")
    integrated = mocker.spy(model, "_calc_modelled_obs_integrated")
    resolved = mocker.spy(model, "_calc_modelled_obs_HiTRes")
    retrieval = mocker.patch.object(model, "_get_data", side_effect=AssertionError("No additional retrieval"))

    result = model.calc_modelled_obs(
        time_resolved_by_sector={"natural-rtot": True, "ocean": False},
        split_by_sectors=True,
        output_fp_x_flux=True,
    )

    assert alignment.call_count == 1
    assert integrated.call_count == resolved.call_count == 1
    assert integrated.call_args.kwargs["sources"] == "ocean"
    assert resolved.call_args.kwargs["sources"] == "natural-rtot"
    assert integrated.call_args.kwargs["scenario"] is resolved.call_args.kwargs["scenario"]
    retrieval.assert_not_called()
    assert model.footprint is footprint
    xr.testing.assert_identical(footprint.data, original)
    assert "mf_mod_high_res" not in result
    assert result.source.values.tolist() == ["natural-rtot", "ocean"]
    assert result.mf_mod.dims == ("time",)
    assert np.isfinite(result.mf_mod).all()
    xr.testing.assert_allclose(result.mf_mod_sectoral.sum("source"), result.mf_mod)
    xr.testing.assert_allclose(result.fp_x_flux_sectoral.sum("source"), result.fp_x_flux)
    xr.testing.assert_allclose(
        result.fp_x_flux.pint.quantify().sum(["lat", "lon"]).pint.dequantify(), result.mf_mod
    )
    assert xr.get_options()["keep_attrs"] == keep_attrs


@pytest.mark.parametrize(
    "selection",
    [
        {"natural-rtot": True, "ocean": False},
        {"natural-rtot": False, "ocean": True},
        {"natural-rtot": True, "ocean": True},
        {"natural-rtot": False, "ocean": False},
        {"ocean": False},
    ],
)
def test_configured_sector_values_match_existing_kernels(sector_co2_scenario, selection):
    """Each source uses its selected kernel, with lag-resolved routing as the default."""
    model = sector_co2_scenario
    result = model.calc_modelled_obs(
        time_resolved_by_sector=selection, split_by_sectors=True, output_fp_x_flux=True
    )
    for source in model.flux_sources:
        contribution = expected_sector_contribution(model, source, selection.get(source, True))
        expected = model.convert_units(
            xr.Dataset(
                {"mf_mod": contribution.sum(["lat", "lon"]), "fp_x_flux": contribution}
            ).pint.dequantify()
        )
        xr.testing.assert_allclose(result.mf_mod_sectoral.sel(source=source, drop=True), expected.mf_mod)
        xr.testing.assert_allclose(
            result.fp_x_flux_sectoral.sel(source=source, drop=True), expected.fp_x_flux
        )


def test_sector_output_flag_does_not_change_total(sector_co2_scenario):
    """Sector diagnostics preserve the total contribution and its temporal averaging."""
    model = sector_co2_scenario
    selection = {"natural-rtot": False, "ocean": False}
    total = model.calc_modelled_obs(time_resolved_by_sector=selection, output_fp_x_flux=True)
    sectoral = model.calc_modelled_obs(
        time_resolved_by_sector=selection, output_fp_x_flux=True, split_by_sectors=True
    )
    xr.testing.assert_identical(total.mf_mod, sectoral.mf_mod)
    xr.testing.assert_identical(total.fp_x_flux, sectoral.fp_x_flux)
    xr.testing.assert_allclose(sectoral.mf_mod_sectoral.sum("source"), total.mf_mod)


def test_integrated_sector_release_time_override(sector_co2_scenario):
    """Integrated sector calculations retain the low-frequency flux-time override."""
    model = sector_co2_scenario
    result = model.calc_modelled_obs(
        sources="natural-rtot",
        time_resolved_by_sector={"natural-rtot": False},
        use_low_freq_flux=False,
    )
    contribution = expected_sector_contribution(model, "natural-rtot", False, use_low_freq_flux=False)
    expected = model.convert_units(
        contribution.sum(["lat", "lon"]).to_dataset(name="mf_mod").pint.dequantify()
    )
    xr.testing.assert_allclose(result.mf_mod, expected.mf_mod)


def test_selected_sources_only_are_routed(stored_integrated_co2_scenario):
    """Overrides for unselected sources do not require lag-resolved footprint data."""
    result = stored_integrated_co2_scenario.calc_modelled_obs(
        sources="ocean",
        time_resolved_by_sector={"natural-rtot": True, "ocean": False},
        split_by_sectors=True,
    )
    assert result.source.values.tolist() == ["ocean"]
    assert "fp_x_flux" not in result
    xr.testing.assert_allclose(result.mf_mod_sectoral.sum("source"), result.mf_mod)


@pytest.mark.parametrize("representation", ["legacy", "resolved_and_residual"])
def test_sector_routing_supports_both_time_resolved_formats(sector_co2_scenario, representation):
    model = sector_co2_scenario
    expected = model.calc_modelled_obs(time_resolved_by_sector={"ocean": False})
    if representation == "resolved_and_residual":
        data = model.footprint.data
        components = time_resolved_and_residual_footprints(data.fp_HiTRes)
        model.footprint.data = data.drop_dims("H_back").merge(components)
    result = model.calc_modelled_obs(time_resolved_by_sector={"ocean": False}, recalculate=True)
    xr.testing.assert_allclose(result, expected)
    assert result.attrs == expected.attrs


@pytest.mark.parametrize(
    "selection,error,match",
    [
        (True, TypeError, "must be a mapping"),
        ([True], TypeError, "must be a mapping"),
        ({1: True}, TypeError, "keys must be"),
        ({"missing": True}, ValueError, "Unknown flux source"),
        ({"ocean": "false"}, TypeError, "must be True or False"),
        ({"ocean": 1}, TypeError, "must be True or False"),
        ({"ocean": None}, TypeError, "must be True or False"),
    ],
)
def test_sector_selection_validation_precedes_cache(sector_co2_scenario, selection, error, match):
    model = sector_co2_scenario
    cached = model.calc_modelled_obs(time_resolved_by_sector={"ocean": False})
    aligned = model.scenario
    with pytest.raises(error, match=match):
        model.calc_modelled_obs(time_resolved_by_sector=selection)
    assert model.modelled_obs is cached
    assert model.scenario is aligned


def test_time_resolved_sector_requires_lag_sensitivities(stored_integrated_co2_scenario):
    with pytest.raises(ValueError, match="Time-resolved sectors require"):
        stored_integrated_co2_scenario.calc_modelled_obs(time_resolved_by_sector={"natural-rtot": True})


def test_integrated_sector_requires_integrated_sensitivity(sector_co2_scenario):
    model = sector_co2_scenario
    model.footprint.data = model.footprint.data.drop_vars("fp")
    with pytest.raises(ValueError, match="Integrated sectors require fp"):
        model.calc_modelled_obs(time_resolved_by_sector={"ocean": False})


def test_time_resolved_sector_requires_residual(sector_co2_scenario):
    model = sector_co2_scenario
    data = model.footprint.data
    components = time_resolved_and_residual_footprints(data.fp_HiTRes)
    model.footprint.data = data.drop_dims("H_back").merge(components).drop_vars("fp_residual")
    with pytest.raises(ValueError, match="also require fp_residual"):
        model.calc_modelled_obs(time_resolved_by_sector={"natural-rtot": True})


def test_sector_routing_requires_declared_units(stored_co2_scenario):
    """Unitless legacy sensitivities are rejected before sector contributions are combined."""
    with pytest.raises(ValueError, match="requires declared sensitivity units"):
        stored_co2_scenario.calc_modelled_obs(time_resolved_by_sector={"ocean": False})


@pytest.mark.parametrize("missing", ["coordinate", "variable_dimension"])
def test_time_resolved_sector_requires_lag_coordinate(sector_co2_scenario, missing):
    model = sector_co2_scenario
    if missing == "coordinate":
        model.footprint.data = model.footprint.data.drop_vars("H_back")
    else:
        model.footprint.data["fp_HiTRes"] = model.footprint.data.fp_HiTRes.sum("H_back")
    with pytest.raises(ValueError, match="require an H_back coordinate"):
        model.calc_modelled_obs(time_resolved_by_sector={"ocean": False})


def test_sector_cache_tracks_selection_and_output_options(sector_co2_scenario, mocker):
    model = sector_co2_scenario
    compute = mocker.spy(model, "_calc_modelled_obs_by_sector")
    first = model.calc_modelled_obs(time_resolved_by_sector={"ocean": False})
    assert model.calc_modelled_obs(time_resolved_by_sector={"ocean": False}) is first
    assert compute.call_count == 1
    all_integrated = model.calc_modelled_obs(time_resolved_by_sector={"natural-rtot": False, "ocean": False})
    assert all_integrated is not first
    selected = model.calc_modelled_obs(sources="ocean", time_resolved_by_sector={"ocean": False})
    assert selected is not all_integrated
    spatial = model.calc_modelled_obs(
        sources="ocean", time_resolved_by_sector={"ocean": False}, output_fp_x_flux=True
    )
    assert "fp_x_flux" in spatial
    assert compute.call_count == 4
    units = model.calc_modelled_obs(
        sources="ocean", time_resolved_by_sector={"ocean": False}, output_units="mol/mol"
    )
    xr.testing.assert_allclose(units.mf_mod, selected.mf_mod / 1e6)
    assert compute.call_count == 5


def test_configured_and_default_cache_do_not_cross(sector_co2_scenario):
    model = sector_co2_scenario
    configured = model.calc_modelled_obs(time_resolved_by_sector={"ocean": False})
    default = model.calc_modelled_obs()
    assert default is not configured
    assert "mf_mod_high_res" in default and "mf_mod" not in default
    assert model._sector_modelled_obs_cache_key is None
    assert model.calc_modelled_obs(time_resolved_by_sector={"ocean": False}) is not default


def test_empty_selection_keeps_existing_default(sector_co2_scenario):
    model = sector_co2_scenario
    default = model.calc_modelled_obs()
    assert model.calc_modelled_obs(time_resolved_by_sector={}) is default


def test_configured_cache_false_clears_previous_cached_results(sector_co2_scenario):
    model = sector_co2_scenario
    expected = model.calc_modelled_obs(time_resolved_by_sector={"ocean": False})
    result = model.calc_modelled_obs(time_resolved_by_sector={"ocean": False}, cache=False)
    xr.testing.assert_identical(result, expected)
    assert model.modelled_obs is model.scenario is model._sector_modelled_obs_cache_key is None


def test_configured_merge_replaces_stale_modelled_fields(sector_co2_scenario, mocker):
    model = sector_co2_scenario
    alignment = mocker.spy(model, "_scenario_for_footprint")
    first = model.footprints_data_merge(time_resolved_by_sector={"ocean": False}, calc_bc=False)
    assert alignment.call_count == 1
    second = model.footprints_data_merge(
        time_resolved_by_sector={"natural-rtot": False, "ocean": False}, calc_bc=False
    )
    assert alignment.call_count == 2
    assert "time_resolved_by_sector" in second.attrs
    assert not np.allclose(first.mf_mod, second.mf_mod)
    default = model.footprints_data_merge(calc_bc=False)
    assert "mf_mod_high_res" in default
    assert "time_resolved_by_sector" not in default.attrs


def test_configured_merge_validation_preserves_cache(sector_co2_scenario):
    model = sector_co2_scenario
    cached = model.footprints_data_merge(time_resolved_by_sector={"ocean": False}, calc_bc=False)
    modelled = model.modelled_obs
    with pytest.raises(TypeError, match="must be True or False"):
        model.footprints_data_merge(time_resolved_by_sector={"ocean": "false"}, calc_bc=False)
    assert model.scenario is cached
    assert model.modelled_obs is modelled


def test_sector_cache_tracks_mutated_mapping(sector_co2_scenario):
    model = sector_co2_scenario
    selection = {"ocean": False}
    first = model.calc_modelled_obs(time_resolved_by_sector=selection)
    selection["natural-rtot"] = False
    assert model.calc_modelled_obs(time_resolved_by_sector=selection) is not first


def test_sector_routing_rejects_incompatible_units(sector_co2_scenario):
    model = sector_co2_scenario
    model.fluxes["natural-rtot"].data.flux.attrs["units"] = "m"
    keep_attrs = xr.get_options()["keep_attrs"]
    with pytest.raises(ValueError, match="must have mole-fraction units"):
        model.calc_modelled_obs(time_resolved_by_sector={"ocean": False})
    assert xr.get_options()["keep_attrs"] == keep_attrs


def test_configured_footprint_only_model(sector_co2_scenario):
    reference = sector_co2_scenario
    model = ModelScenario(species="co2", footprint=reference.footprint, flux=reference.fluxes)
    result = model.calc_modelled_obs(time_resolved_by_sector={"ocean": False}, split_by_sectors=True)
    np.testing.assert_array_equal(result.time.values, model.footprint.data.time.values)
    xr.testing.assert_allclose(result.mf_mod_sectoral.sum("source"), result.mf_mod)


def test_configured_satellite_keeps_sounding_alignment(stored_satellite_scenario):
    model = stored_satellite_scenario
    aligned = model.combine_obs_footprint()
    result = model.footprints_data_merge(
        time_resolved_by_sector={"all": False}, split_by_sectors=True, calc_bc=False, resample_to="2h"
    )
    np.testing.assert_array_equal(result.time.values, aligned.time.values)
    assert result.attrs["resample_to"] == "None"
    xr.testing.assert_allclose(result.mf_mod_sectoral.sum("source"), result.mf_mod)


def test_sector_output_remains_lazy_and_serialisable(sector_co2_scenario, tmp_path):
    model = sector_co2_scenario
    model.footprint.data = model.footprint.data.chunk(time=12)
    for flux in model.fluxes.values():
        flux.data = flux.data.chunk(time=12)
    result = model.calc_modelled_obs(
        time_resolved_by_sector={"ocean": False}, split_by_sectors=True, output_fp_x_flux=True
    )
    assert result.fp_x_flux.chunks is not None
    path = tmp_path / "sector_output.nc"
    result.to_netcdf(path, engine="h5netcdf")
    with xr.open_dataset(path, engine="h5netcdf") as reopened:
        xr.testing.assert_allclose(reopened, result)
        assert reopened.attrs["time_resolved_by_sector"] == result.attrs["time_resolved_by_sector"]
