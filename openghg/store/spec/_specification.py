from typing import Any, Dict, Tuple

__all__ = ["define_data_types", "define_data_type_classes"]


def define_data_types() -> Tuple[str, ...]:
    """
    Define names of data types for objects within OpenGHG
    """

    data_types = (
        "surface",
        "column",
        "emissions",
        # "met",
        "footprints",
        "boundary_conditions",
        "eulerian_model",
    )

    return data_types


def define_data_type_classes() -> Dict[str, Any]:
    """
    Define mapping between data types and associated input classes within OpenGHG
    """
    from openghg.store import (
        BoundaryConditions,
        Emissions,
        EulerianModel,
        Footprints,
        ObsColumn,
        ObsSurface,
    )

    data_type_classes = {
        "surface": ObsSurface,
        "column": ObsColumn,
        "emissions": Emissions,
        # "met": ???
        "footprints": Footprints,
        "boundary_conditions": BoundaryConditions,
        "eulerian_model": EulerianModel,
    }

    return data_type_classes
