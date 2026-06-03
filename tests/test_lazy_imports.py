import importlib
import ast
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

LAZY_EXPORT_PACKAGES = [
    "openghg.dataobjects",
    "openghg.objectstore",
    "openghg.retrieve",
    "openghg.store",
    "openghg.types",
    "openghg.util",
]


def _run_python(code: str) -> None:
    """Run import assertions in a fresh Python process."""
    subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        check=True,
        text=True,
        capture_output=True,
    )


@pytest.mark.parametrize("package_name", LAZY_EXPORT_PACKAGES)
def test_lazy_export_manifest_matches_public_api(package_name: str):
    """Lazy package export manifests should stay aligned with the public API."""
    module = importlib.import_module(package_name)

    assert set(module.__all__) == set(module._EXPORTS)
    assert set(module.__all__) <= set(dir(module))


@pytest.mark.parametrize("package_name", LAZY_EXPORT_PACKAGES)
def test_lazy_export_targets_are_valid(package_name: str):
    """Each lazy export should resolve to an attribute on its target module."""
    module = importlib.import_module(package_name)

    for public_name, target_module_name in module._EXPORTS.items():
        target_module = importlib.import_module(target_module_name, package_name)
        assert hasattr(target_module, public_name), f"{package_name}.{public_name}"


@pytest.mark.parametrize("package_name", LAZY_EXPORT_PACKAGES)
def test_lazy_export_stub_matches_public_api(package_name: str):
    """Lazy packages should expose typed re-exports for static analysis."""
    module = importlib.import_module(package_name)
    stub_path = Path(module.__file__).with_suffix(".pyi")

    assert stub_path.exists(), f"{package_name} is missing {stub_path.name}"

    stub_tree = ast.parse(stub_path.read_text(), filename=str(stub_path))
    stub_exports = {
        alias.asname or alias.name
        for node in stub_tree.body
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }

    assert set(module.__all__) <= stub_exports


def test_retrieve_search_surface_import_stays_light():
    """Importing a search function should not import array/dataframe stacks."""
    _run_python("""
        import sys
        from openghg.retrieve import search_surface

        assert callable(search_surface)
        heavy_modules = ["pandas", "xarray", "zarr", "dask", "rich", "matplotlib"]
        loaded = [module for module in heavy_modules if module in sys.modules]
        assert loaded == [], loaded
        """)


def test_openghg_import_does_not_register_pint_xarray_accessor():
    """Plain openghg import should keep pint_xarray as an explicit opt-in."""
    _run_python("""
        import sys
        import openghg

        assert "pint_xarray" not in sys.modules
        assert "xarray" not in sys.modules
        """)


def test_enable_pint_xarray_registers_accessor():
    """The explicit pint-xarray helper should restore the xarray .pint accessor."""
    _run_python("""
        import openghg

        openghg.enable_pint_xarray()

        import xarray as xr

        assert hasattr(xr.DataArray([1]), "pint")
        """)


def test_top_level_submodule_manifest_allows_helpers():
    """The top-level lazy submodule manifest should exclude function helpers."""
    import openghg

    assert set(openghg._SUBMODULES) == set(openghg.__all__) - {"enable_pint_xarray"}
    assert "enable_pint_xarray" in dir(openghg)


def test_lazy_dataobject_export_imports_on_access():
    """Dataobject package import should be light, but from-import should still work."""
    _run_python("""
        import sys
        import openghg.dataobjects as dataobjects

        assert "SearchResults" in dir(dataobjects)
        assert "openghg.dataobjects._searchresults" not in sys.modules

        from openghg.dataobjects import SearchResults

        assert SearchResults.__name__ == "SearchResults"
        assert "openghg.dataobjects._searchresults" in sys.modules
        assert "pandas" not in sys.modules
        """)


def test_data_type_registry_is_complete_without_store_class_imports():
    """Data type discovery should not depend on imported BaseStore subclasses."""
    _run_python("""
        import sys
        from openghg.store.spec import define_data_types

        assert set(define_data_types()) >= {"surface", "column", "flux", "footprints"}
        assert "mobile" not in define_data_types()
        assert "openghg.store._obscolumn" not in sys.modules

        from openghg.store import get_data_class

        data_class = get_data_class("column")
        assert data_class.__name__ == "ObsColumn"
        assert "openghg.store._obscolumn" in sys.modules

        mobile_class = get_data_class("mobile")
        assert mobile_class.__name__ == "ObsMobile"
        assert "openghg.store._obsmobile" in sys.modules
        """)


def test_builtin_data_type_names_are_reserved_before_builtin_imports():
    """Custom stores should not claim built-in data type names before lazy imports."""
    _run_python("""
        from openghg.store.base import BaseStore
        from openghg.store.base._base import ClassDefinitionError

        try:
            class FakeSurface(BaseStore):
                _data_type = "surface"
        except ClassDefinitionError:
            pass
        else:
            raise AssertionError("reserved built-in data type was accepted")
        """)
