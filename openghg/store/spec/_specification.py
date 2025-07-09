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


def define_data_type_classes() -> dict[str, Any]:
    """Define mapping between data types and associated input classes within OpenGHG."""
    from openghg.store.base import BaseStore

    return BaseStore._registry.copy()


def define_data_types() -> tuple[str, ...]:
    """Define names of data types for objects within OpenGHG."""
    return tuple(define_data_type_classes().keys())


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
