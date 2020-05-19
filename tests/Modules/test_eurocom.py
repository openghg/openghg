# import logging
# mpl_logger = logging.getLogger("matplotlib")
# mpl_logger.setLevel(logging.WARNING)

# import os
# import pandas as pd
# import pytest

# from HUGS.Modules import Datasource, EUROCOM
# from HUGS.ObjectStore import get_local_bucket

# def test_read_data():
#     filepath = "/Users/wm19361/Documents/Devel/hugs/raw_data/eurocom/MHD_air.hdf.all.COMBI_Drought2018_20190522.co2"

#     euro = EUROCOM.load()

#     data = euro.read_data(data_filepath=filepath, site="MHD")

#     print(data)


#     assert False


# def test_read_file():
#     filepath = "/Users/wm19361/Documents/Devel/hugs/raw_data/eurocom/MHD_air.hdf.all.COMBI_Drought2018_20190522.co2"

#     uuids = EUROCOM.read_file(data_filepath=filepath, source_name="MHD_air.hdf.all.COMBI_Drought2018_20190522", site="MHD")

#     print(uuids)

#     assert False
