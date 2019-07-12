import sys
# TODO - this will be removed in the future, currently using a testing branch of Acquire
if os.path.isdir("../../acquire"):
    sys.path.insert(0, "../../acquire")
    import Acquire
else:
    expected_path = os.path.abspath("../../acquire")
    raise ImportError("Please clone Acquire into the directory " + expected_path)

