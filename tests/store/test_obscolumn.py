import numpy as np
from helpers import get_column_datapath
from openghg.standardise import standardise_column
from openghg.objectstore import get_bucket
from openghg.store.base import Datasource
from pandas import Timestamp


def test_read_openghg_format():
    """
    Test that files already in OpenGHG format can be read. This file includes:
     - appropriate variable names and types
     - attributes which can be understood and intepreted
    """
    filename = "gosat-fts_gosat_20170318_ch4-column.nc"
    datafile = get_column_datapath(filename=filename)

    satellite = "GOSAT"
    domain = "BRAZIL"
    species = "methane"

    bucket = get_bucket()
    results = standardise_column(bucket=bucket,
                                 filepath=datafile,
                                 source_format="OPENGHG",
                                 satellite=satellite,
                                 domain=domain,
                                 species=species,
                                 )

    # Output style from ObsSurface - may want to use for ObsColumn as well
    # uuid = results["processed"][filename]["ch4"]["uuid"]

    # Output style for other object types
    assert "ch4" in results
    uuid = results["ch4"]["uuid"]

    bucket = get_bucket()

    ch4_data = Datasource.load(bucket=bucket, uuid=uuid, shallow=False).data()
    ch4_data = ch4_data['2017-03-18-15:32:54+00:00_2017-03-18-17:22:23+00:00']

    assert ch4_data.time[0] == Timestamp("2017-03-18T15:32:54")
    assert np.isclose(ch4_data["xch4"][0], 1844.2019)
