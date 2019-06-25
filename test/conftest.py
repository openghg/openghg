
import sys
import os

# load all of the common fixtures used by the mocked tests
pytest_plugins = ["mock.fixtures.mocked_services"]

# Added for import of services modules in tests
sys.path.insert(0, os.path.abspath("services"))

# Added for import of hugs from testing directory
sys.path.insert(0, os.path.abspath("."))

# Added for import of Acquire services modules in tests
acquire_dir = os.getenv("ACQUIRE_DIR")

if acquire_dir is None or len(acquire_dir) == 0:
    raise PermissionError("You need to supply the location of "
                          "the Acquire source in the environment "
                          "variable 'ACQUIRE_DIR'")

sys.path.insert(0, os.path.abspath(acquire_dir))
sys.path.insert(0, os.path.abspath("%s/services" % acquire_dir))
