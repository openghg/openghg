import logging

from xarray import Dataset
from openghg_calscales.functions import convert

logger = logging.getLogger("openghg.util")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def update_scale(
    data: Dataset,
    species: str,
    calibration_scale: str | None = None,
) -> Dataset:
    """
    Rename calibration_scale into scale in data attributes.
    If calibration_scale is not None, update it using openghg_calscales.functions.convert.
    Args:
        data: data containing species concentrations. Should have a global attribute "calibration_scale" with the current calibration scale.
        species: Species identifier e.g. ch4 for methane.
        calibration_scale: Convert to this calibration scale
    Returns:
        dataset with converted scale and updated attr name.
    """

    data.attrs["scale"] = data.attrs.pop("calibration_scale")
    existing_calibration_scale = data.attrs["scale"]

    if calibration_scale is not None:
        target_scale = calibration_scale
        original_scale = existing_calibration_scale

        if original_scale and target_scale and original_scale != target_scale:
            logger.warning(f"Converting from calibration scale '{original_scale}' to '{target_scale}'.")
            for var_name in (
                v for v in data.data_vars if isinstance(v, str) and (v == "mf" or v.startswith("mf_"))
            ):
                # Convert function from openghg_calscales
                data[var_name] = convert(
                    c=data[var_name],
                    species=species,
                    scale_original=original_scale,
                    scale_new=target_scale,
                )
                data[var_name].attrs["calibration_scale"] = target_scale

        data.attrs["scale"] = target_scale
    return data
