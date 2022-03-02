from pandas import read_csv, to_datetime
from pathlib import Path
from typing import Dict, Optional
from addict import Dict as aDict


def parse_glasow_licor(filepath: Path, sampling_period: Optional[str] = None) -> Dict:
    """Read the Glasgow LICOR data from NPL

    Args:
        filepath: Path to data file
    Returns:
        dict: Dictionary of data
    """
    date_index = {"time": ["DATE", "TIME"]}
    use_cols = [0, 1, 3, 4, 5]
    nan_values = [",,,"]
    df = read_csv(
        filepath,
        parse_dates=date_index,
        na_values=nan_values,
        infer_datetime_format=True,
        index_col="time",
        usecols=use_cols,
    )

    rename_cols = {
        "LAT": "latitude",
        "LON": "longitude",
        "Methane_Enhancement_Over_Background(ppb)": "ch4",
    }
    df = df.rename(columns=rename_cols).dropna(axis="rows", how="any")
    df.index = to_datetime(df.index)

    ds = df.to_xarray()

    if sampling_period is None:
        sampling_period = "NOT_SET"

    metadata = {
        "units": "ppb",
        "notes": "measurement value is methane enhancement over background",
        "sampling_period": sampling_period,
    }

    data = aDict()
    data["ch4"]["metadata"] = metadata
    data["ch4"]["data"] = ds

    to_return: Dict = data.to_dict()
    return to_return
