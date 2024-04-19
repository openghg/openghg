from typing import Any, Tuple, Dict, List

__all__ = ["define_data_types", "define_data_type_classes"]


def define_data_types() -> Tuple[str, ...]:
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


def define_data_type_classes() -> Dict[str, Any]:
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


def null_metadata_values() -> List:
    """
    Defines values which indicate metadata value is not specified.
    Returns:
        list: values to be seen as null
    """
    # TODO: Depending on how this is implemented, may want to update this to include np.nan values
    null_values = ["not_set", "NOT_SET"]

    return null_values
