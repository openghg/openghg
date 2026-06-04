from importlib import import_module
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
    MetTypes,
)

__all__ = [
    "define_data_types",
    "define_data_type_classes",
    "get_data_type_class_target",
    "get_data_type_class",
    "define_standardise_parsers",
    "define_transform_parsers",
    "check_parser",
]

_DATA_TYPES = (
    "surface",
    "column",
    "flux",
    "flux_timeseries",
    "footprints",
    "eulerian_model",
    "boundary_conditions",
    "site_met",
)

_DATA_TYPE_CLASSES = {
    "surface": ("openghg.store._obssurface", "ObsSurface"),
    "column": ("openghg.store._obscolumn", "ObsColumn"),
    "flux": ("openghg.store._flux", "Flux"),
    "flux_timeseries": ("openghg.store._flux_timeseries", "FluxTimeseries"),
    "footprints": ("openghg.store._footprints", "Footprints"),
    "eulerian_model": ("openghg.store._eulerian_model", "EulerianModel"),
    "boundary_conditions": ("openghg.store._boundary_conditions", "BoundaryConditions"),
    "site_met": ("openghg.store._met", "SiteMet"),
    "mobile": ("openghg.store._obsmobile", "ObsMobile"),
}


def get_data_type_class_target(data_type: str) -> tuple[str, str]:
    """
    Return the import target for the store class for a data type.

    Keeping this as declarative metadata means data type discovery does not
    depend on importing every store implementation for subclass registration.
    """
    try:
        return _DATA_TYPE_CLASSES[data_type]
    except KeyError as exc:
        valid_data_types = tuple(_DATA_TYPE_CLASSES)
        raise ValueError(
            f"{data_type} is not a valid data type, please select one of {valid_data_types}"
        ) from exc


def get_data_type_class(data_type: str) -> Any:
    """
    Return the store class for a data type.

    This imports only the module needed for the requested data type, which keeps
    search startup from importing every store implementation.
    """
    module_name, class_name = get_data_type_class_target(data_type)
    module = import_module(module_name)
    return getattr(module, class_name)


def define_data_type_classes() -> dict[str, Any]:
    """
    Define mapping between data types and associated input classes within OpenGHG
    """
    return {data_type: get_data_type_class(data_type) for data_type in _DATA_TYPES}


def define_data_types() -> tuple[str, ...]:
    """
    Define names of configured searchable data types for objects within OpenGHG.

    Legacy class targets such as ``mobile`` can still be resolved explicitly via
    :func:`get_data_type_class`, but are not included here unless they have the
    metadata configuration needed for generic object-store search.
    """
    return _DATA_TYPES


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
        "site_met": MetTypes,
    }

    return data_type_parsers


def define_transform_parsers() -> dict[str, Any]:
    """
    Define mapping between data_types and transform parser details
    """

    # TODO: May want to move away from representing these parser details as classes
    data_type_parsers = {
        "flux": FluxDatabases,
        "boundary_conditions": BoundaryConditions,
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
