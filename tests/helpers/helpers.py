""" Some helper functions for things we do in tests frequently
"""
from pathlib import Path

__all__ = ["get_datapath"]


def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")
