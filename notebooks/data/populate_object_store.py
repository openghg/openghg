from openghg.modules import ObsSurface

import os
os.environ["OPENGHG_PATH"] = "/work/chxmr/objectStore"

from openghg.localclient._file_search import find_files

# Contains list of dictionaries to pass to the read_file() function for
# loading all data files.
all_inputs = find_files()
print(f"Keys for each entry: {list(all_inputs[0].keys())}")
inputs = [value for key, value in all_inputs[0].items() if key != "filepath"]
print(f"Files to load for first entry ({inputs}):\n {all_inputs[0]['filepath']}")

all_results = []
for params in all_inputs:
    try:
        results = ObsSurface.read_file(overwrite=True, **params)
    except ValueError:
        print("Unable to process file")
        print(params)
        # zeppelin-medusa.09.C +
        # barbados.00.C + IndexError
    else:
        all_results.append(results)