import numpy as np
from openghg.util._inlet import extract_inlet_value


def _get_site_pressure(inlet_heights: list, site_height: float) -> list[float]:
    """Calculate the pressure levels required, in hPa, for the given inlet heights and site height.

    Args:
        inlet_height: Height(s) of inlets
        site_height: Height of site
    Returns:
        list: List of pressures
    """

    scale_height = 7640  # scale height of atmosphere in metres

    if not isinstance(inlet_heights, list):
        inlet_heights = [inlet_heights]

    measured_pressure = []
    for h in inlet_heights:
        try:
            # Extract the number from the inlet height str using regex
            inlet = extract_inlet_value(h)
            measurement_height = inlet + float(site_height)
            # Calculate the pressure
            pressure = float(1000 * np.exp((-1 * measurement_height) / scale_height))
            measured_pressure.append(pressure)
        except IndexError:
            pass

    return measured_pressure


def _altitude_to_ecmwf_pressure(measure_pressure: list[float], return_closest: bool = False) -> list[float]:
    """Returns list of closest ERA5 pressure levels for each given measurement pressures.

    Args:
        measure_pressure: List of pressures
        return_closest: Whether to return the closest pressure level for each measure_pressure (True) or the relevant levels for all measure_pressure heights (False)
            e.g. for for TAC with height  64m and inlets (['185magl', '54magl', '100magl'])
            return_closest=True returns ['975','975','975'] (one per inlet)
            return_closest=False returns ['950', '975', '1000'] (relevant levels for all inlets)
    Returns:
        list: List of valid ERA5 pressures
    """
    from openghg.util import load_internal_json

    ecwmf_info_file = "ecmwf_dataset_info.json"
    ecmwf_metadata = load_internal_json(ecwmf_info_file)
    dataset_metadata = ecmwf_metadata["datasets"]
    valid_levels = np.array(dataset_metadata["reanalysis_era5_pressure_levels"]["valid_levels"])

    # choosing the closest pressure level
    pressure_levels = []
    if return_closest:
        for p in measure_pressure:
            closest_level = valid_levels[np.argmin(np.abs(valid_levels - p))]
            pressure_levels.append(float(closest_level))
    else:
        # choosing the closest two pressure levels from ERA5 for each measurement pressure
        # Match pressure to ERA5 pressure levels
        ecwmf_pressure_indices = np.zeros(len(measure_pressure) * 2)

        for index, p in enumerate(measure_pressure):
            ecwmf_pressure_indices[(index * 2) : (index * 2 + 2)] = _two_closest_values(p - valid_levels)

        pressure_levels = valid_levels[np.unique(ecwmf_pressure_indices).astype(int)]
        pressure_levels = [float(x) for x in pressure_levels]

    return pressure_levels


def _two_closest_values(diff: np.ndarray) -> np.ndarray:
    """Get location of two closest values in an array of differences.

    Args:
        diff: Numpy array of values
    Returns:
        np.ndarry: Numpy array of two closes values
    """
    closest_values: np.ndarray = np.argpartition(np.abs(diff), 2)[:2]
    return closest_values


def _get_ecmwf_area(site_lat: float, site_long: float) -> list:
    """Find out the area required from ERA5.

    Args:
        site_lat: Latitude of site. expected in format -90 to 90
        site_long: Site longitude. expected in format -180 to 180
    Returns:
        list: List of min/max lat long values
    """
    assert -90 <= site_lat <= 90, "Latitude must be between -90 and 90"
    assert -180 <= site_long <= 180, "Longitude must be between -180 and 180"

    ecwmf_lat = np.arange(-90, 90.25, 0.25)
    ecwmf_lon = np.arange(-180, 180.25, 0.25)

    ecwmf_lat_indices = _two_closest_values(ecwmf_lat - site_lat)
    ecwmf_lon_indices = _two_closest_values(ecwmf_lon - site_long)

    return [
        float(np.max(ecwmf_lat[ecwmf_lat_indices])),
        float(np.min(ecwmf_lon[ecwmf_lon_indices])),
        float(np.min(ecwmf_lat[ecwmf_lat_indices])),
        float(np.max(ecwmf_lon[ecwmf_lon_indices])),
    ]
