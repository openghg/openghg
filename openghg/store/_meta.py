# This holds store metadata for now
from typing import Dict


def storage_class_info() -> Dict:
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
    }
