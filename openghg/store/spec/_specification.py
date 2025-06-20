from typing import Any
from openghg.types import (
    SurfaceTypes,
    ColumnTypes,
    FluxTypes,
    FootprintTypes,
    FluxTimeseriesTypes,
    FluxDatabases,
    BoundaryConditions,
    EulerianModelTypes,
)

__all__ = [
    "define_data_types",
    "define_data_type_classes",
    "define_standardise_parsers",
    "define_transform_parsers",
]


def define_data_types() -> tuple[str, ...]:
    """
    Define names of data types for objects within OpenGHG
    """

    data_types = (
        "surface",
        "column",
        "flux",
        "footprints",
        "boundary_conditions",
        "eulerian_model",
        "flux_timeseries",
    )

    return data_types


def define_data_type_classes() -> dict[str, Any]:
    """
    Define mapping between data types and associated input classes within OpenGHG
    """
    from openghg.store import (
        BoundaryConditions,
        Flux,
        EulerianModel,
        Footprints,
        ObsColumn,
        ObsSurface,
        FluxTimeseries,
    )

    data_type_classes = {
        "surface": ObsSurface,
        "column": ObsColumn,
        "flux": Flux,
        # "met": ???
        "footprints": Footprints,
        "boundary_conditions": BoundaryConditions,
        "eulerian_model": EulerianModel,
        "flux_timeseries": FluxTimeseries,
    }

    return data_type_classes


def define_standardise_parsers() -> dict[str, Any]:
    """
    Define mapping between data_types and standardise parser details
    """

    # TODO: May want to move away from representing these parser details as classes
    data_type_parsers = {
        "surface": SurfaceTypes,
        "column": ColumnTypes,
        "flux": FluxTypes,
        "flux_timeseries": FluxTimeseriesTypes,
        "footprints": FootprintTypes,
        "eulerian_model": EulerianModelTypes,
        "boundary_conditions": BoundaryConditions,
        # "met": ???,
    }

    return data_type_parsers


def define_transform_parsers() -> dict[str, Any]:
    """
    Define mapping between data_types and transform parser details
    """

    # TODO: May want to move away from representing these parser details as classes
    data_type_parsers = {
        "flux": FluxDatabases,
    }

    return data_type_parsers
