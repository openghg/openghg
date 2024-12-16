from pathlib import Path

from pandas import DataFrame

__all__ = ["parse_beaco2n"]


def parse_beaco2n(
    filepath: str | Path,
    site: str,
    network: str,
    inlet: str,
    instrument: str | None = "shinyei",
    sampling_period: str | None = None,
    **kwargs: dict,
) -> dict:
    """Read BEACO2N data files

    Args:
        filepath: Data filepath
        site: Site name
        network: Network name
        inlet: Inlet height in metres
        instrument: Instrument name
        sampling_period: Measurement sampling period
    Returns:
        dict: Dictionary of data
    """
    from collections import defaultdict

    import pandas as pd
    from openghg.util import clean_string, load_internal_json, format_inlet

    if sampling_period is None:
        sampling_period = "NOT_SET"

    filepath = Path(filepath)
    datetime_columns = {"time": ["datetime"]}
    use_cols = [1, 5, 6, 7, 8, 9, 10]
    na_values = [-999.0]

    site = clean_string(site)

    try:
        data = pd.read_csv(
            filepath,
            index_col="time",
            usecols=use_cols,
            parse_dates=datetime_columns,
            na_values=na_values,
        )
    except ValueError as e:
        raise ValueError(
            f"Unable to read data file, please make sure it is in the standard BEACO2N format.\nError: {e}"
        )

    beaco2n_site_data = load_internal_json(filename="beaco2n_site_data.json")

    try:
        site_metadata = beaco2n_site_data[site.upper()]
    except KeyError:
        raise ValueError(f"Site {site} not recognized.")

    site_metadata["comment"] = "Retrieved from http://beacon.berkeley.edu/"

    # Check which columns we have in the data and build the rename dict
    possible_rename_cols = {
        "PM_ug/m3": "pm",
        "PM_ug/m3_QC_level": "pm_qc",
        "co2_ppm": "co2",
        "co2_ppm_QC_level": "co2_qc",
        "co_ppm": "co",
        "co_ppm_QC_level": "co_qc",
    }
    # Not all columns are in data from different sites, i.e. Glasgow has a CO column
    rename_cols = {k: v for k, v in possible_rename_cols.items() if k in data}
    # Set all values below zero to NaN
    data = data.rename(columns=rename_cols)

    # Read the columns available and make sure we have them to iterate over
    possible_measurement_types = ["pm", "co", "co2"]
    measurement_types = [c for c in possible_measurement_types if c in data]

    units = {"pm": "ug/m3", "co2": "ppm", "co": "ppm"}

    gas_data: defaultdict[str, dict[str, DataFrame | dict]] = defaultdict(dict)
    for mt in measurement_types:
        m_data = data[[mt, f"{mt}_qc"]]
        m_data = m_data.dropna(axis="rows", subset=[mt])

        # Some sites don't have data for each type, skip that type if all NaNs
        if m_data.index.empty:
            continue

        m_data = m_data.to_xarray()

        inlet = clean_string(inlet)
        inlet = format_inlet(inlet, key_name="inlet")

        species_metadata = {
            "units": units[mt],
            "site": site,
            "species": clean_string(mt),
            "inlet": inlet,
            "network": "beaco2n",
            "sampling_period": str(sampling_period),
            "instrument": instrument,
            "data_type": "surface",
            "source_format": "beaco2n",
        }

        # We'll put everything into metadata
        species_metadata.update(site_metadata)

        gas_data[mt]["data"] = m_data
        gas_data[mt]["metadata"] = species_metadata
        gas_data[mt]["attributes"] = site_metadata

    # TODO - add CF Compliant attributes?

    return gas_data
