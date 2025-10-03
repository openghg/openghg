# This holds store metadata for now
from openghg.store.base import BaseStore


def data_class_info() -> dict:
    """Return storage class information for each data type.

    Returns:
        dict: Dictionary of storage class names and UUIDs
    """
    # TODO - move this to read from object store config file?

    return {
        "surface": {"_root": "ObsSurface", "_uuid": "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"},
        "column": {"_root": "ObsColumn", "_uuid": "5c567168-0287-11ed-9d0f-e77f5194a415"},
        "flux": {"_root": "Flux", "_uuid": "c5c88168-0498-40ac-9ad3-949e91a30872"},
        "footprints": {"_root": "Footprints", "_uuid": "62db5bdf-c88d-4e56-97f4-40336d37f18c"},
        "boundary_conditions": {
            "_root": "BoundaryConditions",
            "_uuid": "4e787366-be91-4fc5-ad1b-4adcb213d478",
        },
        "eulerian_model": {"_root": "EulerianModel", "_uuid": "63ff2365-3ba2-452a-a53d-110140805d06"},
        "flux_timeseries": {"_root": "FluxTimeseries", "_uuid": "099b597b-0598-4efa-87dd-472dfe027f5d8"},
    }


def get_data_class(data_type: str) -> type[BaseStore]:
    """Return data class corresponding to given data type.

    Args:
        data_type: one of "surface", "column", "flux", "footprints",
    "boundary_conditions", "eulerian_model or flux_timeseries"

    Returns:
        Data class, one of `ObsSurface`, `ObsColumn`, `Flux`, `EulerianModel`,
    `Footprints`, `BoundaryConditions`, `FluxTimeseries`.
    """
    try:
        data_class = BaseStore._registry[data_type]
    except KeyError:
        raise ValueError(f"No data class for data type {data_type}.")
    else:
        return data_class
