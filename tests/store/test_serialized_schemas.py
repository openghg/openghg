import pytest

from openghg.store import (
    BoundaryConditions,
    DataSchema,
    EulerianModel,
    Flux,
    FluxTimeseries,
    Footprints,
    ObsColumn,
    ObsSurface,
    SiteMet,
)


@pytest.mark.parametrize(
    "schema",
    [
        Flux.schema(),
        BoundaryConditions.schema(),
        ObsSurface.schema("ch4"),
        ObsColumn.schema("ch4"),
        FluxTimeseries.schema(),
        EulerianModel.schema(),
        SiteMet.schema(),
    ],
)
def test_packaged_json_schemas_load(schema):
    assert isinstance(schema, DataSchema)


def test_empty_data_variable_declarations_remain_empty_dicts():
    assert EulerianModel.schema().data_vars == {}
    assert SiteMet.schema().data_vars == {}


def test_footprint_json_fragments_compose():
    schema = Footprints.schema(
        high_spatial_resolution=True,
        time_resolved=True,
        short_lifetime=True,
        source_format="PARIS",
    )

    assert set(schema.data_vars) == {
        "fp_low",
        "fp_high",
        "fp_time_resolved",
        "fp_residual",
        "particle_locations_n",
        "particle_locations_e",
        "particle_locations_s",
        "particle_locations_w",
        "mean_age_particles_n",
        "mean_age_particles_e",
        "mean_age_particles_s",
        "mean_age_particles_w",
    }


def test_inner_domain_omits_particle_location_fragment():
    schema = Footprints.schema(inner_domain="inner")

    assert set(schema.data_vars) == {"fp"}


def test_schema_substitutions_cover_names_and_dimensions():
    schema = ObsColumn.schema("ch4", vertical_name="pressure")

    assert schema.data_vars["xch4_averaging_kernel"] == ("time", "pressure")
    assert schema.data_vars["ch4_profile_apriori"] == ("time", "pressure")


def test_unknown_schema_dtype_has_actionable_error():
    with pytest.raises(ValueError, match="Unknown schema dtype: unsupported"):
        DataSchema.from_dict({"dtypes": {"example": "unsupported"}})


def test_missing_schema_substitution_has_actionable_error():
    with pytest.raises(ValueError, match="Missing schema substitution: species"):
        DataSchema.from_dict({"data_vars": {"{species}": ["time"]}})


def test_unknown_schema_and_fragment_have_actionable_errors():
    with pytest.raises(ValueError, match="Unknown data schema: missing"):
        DataSchema.from_name("missing")

    with pytest.raises(ValueError, match="Unknown footprints schema fragment: missing"):
        DataSchema.from_name("footprints", fragments=["missing"])
