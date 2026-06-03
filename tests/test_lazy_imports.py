import subprocess
import sys
import textwrap


def _run_python(code: str) -> None:
    """Run import assertions in a fresh Python process."""
    subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        check=True,
        text=True,
        capture_output=True,
    )


def test_retrieve_search_surface_import_stays_light():
    """Importing a search function should not import array/dataframe stacks."""
    _run_python(
        """
        import sys
        from openghg.retrieve import search_surface

        assert callable(search_surface)
        heavy_modules = ["pandas", "xarray", "zarr", "dask", "rich", "matplotlib"]
        loaded = [module for module in heavy_modules if module in sys.modules]
        assert loaded == [], loaded
        """
    )


def test_lazy_dataobject_export_imports_on_access():
    """Dataobject package import should be light, but from-import should still work."""
    _run_python(
        """
        import sys
        import openghg.dataobjects as dataobjects

        assert "SearchResults" in dir(dataobjects)
        assert "openghg.dataobjects._searchresults" not in sys.modules

        from openghg.dataobjects import SearchResults

        assert SearchResults.__name__ == "SearchResults"
        assert "openghg.dataobjects._searchresults" in sys.modules
        assert "pandas" not in sys.modules
        """
    )


def test_data_type_registry_is_complete_without_store_class_imports():
    """Data type discovery should not depend on imported BaseStore subclasses."""
    _run_python(
        """
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
        """
    )


def test_builtin_data_type_names_are_reserved_before_builtin_imports():
    """Custom stores should not claim built-in data type names before lazy imports."""
    _run_python(
        """
        from openghg.store.base import BaseStore
        from openghg.store.base._base import ClassDefinitionError

        try:
            class FakeSurface(BaseStore):
                _data_type = "surface"
        except ClassDefinitionError:
            pass
        else:
            raise AssertionError("reserved built-in data type was accepted")
        """
    )
