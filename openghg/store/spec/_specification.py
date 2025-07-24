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
    "check_parser",
]


def define_data_type_classes() -> dict[str, Any]:
    """Define mapping between data types and associated input classes within OpenGHG."""
    from openghg.store.base import BaseStore

    return BaseStore._registry.copy()


def define_data_types() -> tuple[str, ...]:
    """Define names of data types for objects within OpenGHG."""
    return tuple(define_data_type_classes().keys())


def validate_data_type(data_type: str) -> None:
    """Raise TypeError if given data type is not a valid data type class."""
    expected_data_types = define_data_types()

    data_type = data_type.lower()
    if data_type not in expected_data_types:
        raise TypeError(f"Incorrect data type selected. Please select from one of {expected_data_types}")


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


def check_parser(data_type: str, source_format: str, framework: str = "standardise") -> str:
    """
    Check parser can be found for a given data_type and source_format. This can
    be applied for both the standardise and transform framework.

    Args:
        data_type: Name of the data type. See define_data_types() for options.
        source_format: Name of the source_format for the input data. This is the name
            for the parse_* function which will be applied to standardise/tranform the data.
        framework: Name of the framework we want to search. This includes:
            - "standardise"
            - "transform"
            Default = "standardise".
    Returns:
        str: source_format (defined by define_*_parsers() functions)
    Raises:
        ValueError: if there are no source formats defined for a data_type
        ValueError: if source_format cannot be found
    """
    try:
        if framework == "standardise":
            parsers = define_standardise_parsers()[data_type]
        elif framework == "transform":
            parsers = define_transform_parsers()[data_type]
    except KeyError:
        raise ValueError(f"The {framework} framework has no parsers defined for the {data_type} data_type.")

    try:
        source_format = parsers[source_format.upper()].value
    except KeyError:
        raise ValueError(f"Unknown data type {source_format} selected.")

    return source_format
