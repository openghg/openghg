import numpy as np
from pathlib import Path
from openghg.store import infer_date_range


def parse_crf(
    filepath: Path,
    species: str,
    source: str = "anthro",
    region: str = "UK",
    domain: str | None = None,
    data_type: str = "flux_timeseries",
    database: str | None = None,
    database_version: str | None = None,
    model: str | None = None,
    period: str | tuple | None = None,
    continuous: bool = True,
) -> dict:
    """
    Parse CRF emissions data from the specified file.

    Args:
        filepath: Path to the '.xlsx' file containing CRF emissions data.
        species: Name of species
        source: Source of the emissions data, e.g. "energy", "anthro", default is 'anthro'.
        region: Region/Country of the CRF data
        domain: Geographic domain, default is 'None'. Instead region is used to identify area
        data_type: Type of data, default is 'flux_timeseries'.
        database: Database name if applicable.
        database_version: Version of the database if applicable.
        model: Model name if applicable.
        period: Period of measurements. Only needed if this can not be inferred from the time coords
            If specified, should be one of:
                - "yearly", "monthly"
                - suitable pandas Offset Alias
                - tuple of (value, unit) as would be passed to pandas.Timedelta function
        continuous: Whether time stamps have to be continuous.
    Returns:
        Dict: Parsed flux timeseries data in dictionary format.
    """
    import pandas as pd
    from openghg.util import timestamp_now

    # Dictionary of species corresponding to sheet names
    sheet_selector = {"ch4": "Table10s3", "co2": "Table10s1", "n2o": "Table10s4", "hfc": "Table10s5"}

    # Creating dataframe based on species name
    if species.lower() in sheet_selector:
        dataframe = pd.read_excel(filepath, sheet_name=sheet_selector[species.lower()], skiprows=4)
    else:
        raise ValueError(f"Species {species} is incorrect. Please select from {list(sheet_selector.keys())}")

    if species.lower() == "co2" or species.lower() == "hfc":
        dataframe = dataframe.iloc[1]
    else:
        dataframe = dataframe.iloc[49]

    dataframe = pd.DataFrame(dataframe).iloc[2:-1]
    dataframe = dataframe.rename(columns={dataframe.columns[0]: "flux_timeseries"}).astype(np.floating)
    dataframe.index = pd.to_datetime(dataframe.index, format="%Y")

    metadata = {}
    metadata["species"] = species
    if domain is not None:
        metadata["domain"] = domain
    metadata["source"] = source
    optional_keywords = {"database": database, "database_version": database_version, "model": model}

    for key, value in optional_keywords.items():
        if value is not None:
            metadata[key] = value

    author_name = "OpenGHG Cloud"
    metadata["author"] = author_name
    metadata["data_type"] = data_type
    metadata["processed"] = str(timestamp_now())
    metadata["source_format"] = "crf"

    dataframe = dataframe.rename_axis("time")
    dataarray = dataframe.to_xarray()
    dataarray = dataarray.assign_coords(time=dataarray.time)

    start_date, end_date, period_str = infer_date_range(
        dataarray.time, filepath=filepath, period=period, continuous=continuous
    )

    metadata["start-date"] = str(start_date)
    metadata["end-date"] = str(end_date)
    metadata["period"] = str(period_str)
    metadata["region"] = region

    key = "_".join((species, source, region))

    flux_timeseries_data: dict[str, dict] = {}
    flux_timeseries_data[key] = {}
    flux_timeseries_data[key]["data"] = dataarray
    flux_timeseries_data[key]["metadata"] = metadata

    return flux_timeseries_data
