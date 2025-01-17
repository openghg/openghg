#!/usr/bin/env python

from openghg.util._registry import AutoRegistry, Registry


def test_register():
    registry = Registry()

    @registry.register
    def func():
        pass

    assert "func" in registry.functions
    assert func == registry.functions["func"]


def test_register_name_formatting():
    # remove suffix "_filter" from names
    registry = Registry(suffix="filter")

    @registry.register
    def func_filter():
        pass

    assert "func" in registry.functions
    assert func_filter == registry.functions["func"]

    @registry.register
    def func_filt():
        pass

    assert "func_filt" in registry.functions

    # select by prefix
    registry = Registry(prefix="parse")

    @registry.register
    def parse_func():
        pass

    assert "func" in registry.functions
    assert parse_func == registry.functions["func"]


def test_get_params():
    registry = Registry()

    @registry.register
    def func(a, b, c="12"):
        pass

    assert ["a", "b", "c"] == registry.get_params("func")


def test_select_params():
    """Check that Registry.select_params only selects parameters relevant to given function."""
    registry = Registry()

    @registry.register
    def func1(a, b, c="12"):
        pass

    @registry.register
    def func2(a, d, e):
        pass

    params = {"a": 1, "b": 2, "c": 3, "d": 4}

    assert registry.select_params("func1", params) == {"a": 1, "b": 2, "c": 3}
    assert registry.select_params("func2", params) == {"a": 1, "d": 4}

    # check that values can be changed for specified functions
    params = {"a": 1, "b": 2, "c": 3, "d": 4, "func2__a": 10}

    assert registry.select_params("func1", params) == {"a": 1, "b": 2, "c": 3}
    assert registry.select_params("func2", params) == {"a": 10, "d": 4}


def test_autoregistry():
    registry = AutoRegistry(prefix="parse", location="openghg.standardise.surface")

    data_types = ("crds", "gcwerks", "icos", "noaa", "openghg", "agage")

    assert all(data_type in registry.functions for data_type in data_types)

    from openghg.standardise.surface import parse_agage

    assert registry.functions["agage"] == parse_agage


def test_autoregistry_infer_location():
    import registry_aux

    assert list(registry_aux.registry.functions.keys()) == ["abc", "xyz", "_private"]

    # second registry skips private functions
    assert list(registry_aux.registry2.functions.keys()) == ["abc", "xyz"]
