import pytest

from HUGS.Modules import CRDS, GC

# If this doesn't exist in the object store, create it
@pytest.fixture(scope="session", autouse=True)
def check_crds():
    if not CRDS.exists():
        crds = CRDS.create()
        crds.save()


@pytest.fixture(scope="session", autouse=True)
def check_gc():
    if not GC.exists():
        gc = GC.create()
        gc.save()
