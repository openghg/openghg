""" Helper functions to provide datapaths etc used in the tutorial notebooks

"""
from pathlib import Path
from typing import List

__all__ = ["bilsdale_datapaths"]


def bilsdale_datapaths() -> List:
    """Return a list of paths to the Tacolneston data for use in the ranking
    tutorial

    Returns:
        list: List of paths
    """
    crds_path = (
        Path(__file__)
        .resolve()
        .parent.parent.parent.joinpath("tests/data/proc_test_data/CRDS")
    )

    return list(crds_path.glob("bsd.picarro.1minute.*.min.*"))
