"""Auxilliary file used to test AutoRegistry in test_registry.py"""

from openghg.util._registry import AutoRegistry


registry = AutoRegistry(suffix="filter", skip_private=False)
registry2 = AutoRegistry(suffix="filter", skip_private=True)


def abc_filter():
    pass


def xyz_filter():
    pass


def abc_multiplier():
    pass


def _private_filter():
    pass
