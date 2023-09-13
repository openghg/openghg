import datetime

import numpy as np
import pandas as pd
import xarray as xr

from openghg.util import DateRange

def test_daterange_from_dataframe():
    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)
    random_data = pd.DataFrame(
        data=np.random.randint(0, 100, size=(100, 4)),
        index=pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
        columns=list("ABCD"),
    )

    daterange = DateRange.from_dataframe(random_data)

    assert daterange.start == pd.Timestamp("1970-01-01 01:01:00+0000")
    assert daterange.end == pd.Timestamp("1970-04-10 01:01:00+0000")


def test_daterange_from_dataset():
    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)
    random_data = pd.DataFrame(
        data=np.random.randint(0, 100, size=(100, 4)),
        index=pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
        columns=list("ABCD"),
    )
    random_data.index.name = "time"
    random_xr_data = xr.Dataset.from_dataframe(random_data)

    daterange = DateRange.from_dataset(random_xr_data)

    assert daterange.start == pd.Timestamp("1970-01-01 01:01:00+0000")
    assert daterange.end == pd.Timestamp("1970-04-10 01:01:00+0000")
