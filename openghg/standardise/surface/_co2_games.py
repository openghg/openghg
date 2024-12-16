import logging
from pathlib import Path
import copy
import xarray as xr

from openghg.types import optionalPathType
from openghg.standardise.meta import (
    assign_attributes,
    dataset_formatter,
)

logger = logging.getLogger("openghg.standardise.surface")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_co2_games(
    filepath: str | Path,
    site: str,
    measurement_type: str,
    inlet: str | None = None,
    network: str = "icos",
    instrument: str | None = None,
    sampling_period: str | None = None,
    update_mismatch: str = "never",
    site_filepath: optionalPathType = None,
    units: str = "mol/mol",
    **kwarg: dict,
) -> dict:
    """Read co2 verification games files.
       Current scope is for Paris required by Eric
    Args:
        filepath: Data filepath
        site: Three letter site code
        inlet: Inlet height (as value unit e.g. "10m")
        measurement_type: One of ("flask", "insitu", "pfp")
        network: Network, defaults to paris_simulation
        instrument: Instrument name
        sampling_period: Sampling period
        update_mismatch: This determines how mismatches between the internal data
            attributes and the supplied / derived metadata are handled.
            This includes the options:
                - "never" - don't update mismatches and raise an AttrMismatchError
                - "attributes" - update mismatches based on input attributes
                - "metadata" - update mismatches based on input metadata
        site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
        units: currently defaults to mol/mol
    Returns:
        dict: Dictionary of data and metadata
    """

    list_of_models = ["BASE", "PTEN", "ATEN", "HGER", "HFRA", "DFIN"]

    gas_data: dict = {}

    with xr.open_dataset(filepath) as dataset:
        # Use dictionary comprehension to split data variables into individual datasets
        attributes = dataset.attrs
        metadata = {
            "site": attributes["site_code"],
            "species": "co2",
            "inlet": attributes["dataset_intake_ht"],
            "inlet_height_magl": attributes["dataset_intake_ht"],
            "network": network,
            "instrument": "NOT_SET",
            "sampling_period": attributes["dataset_data_frequency_unit"],
            "calibration_scale": attributes["dataset_calibration_scale"],
            "data_owner": "NOT_SET",
            "data_owner_email": "NOT_SET",
            "station_longitude": attributes["site_longitude"],
            "station_latitude": attributes["site_latitude"],
            "station_long_name": attributes["site_name"],
            "station_height_masl": attributes["site_elevation"],
            "measurement_type": measurement_type,
            "units": units,
        }

        gas_dataset = {f"co2_{model}": dataset[[model]] for model in list_of_models}
        for model in gas_dataset.keys():
            data_var = list(gas_dataset[model].data_vars.keys())[0]
            gas_data[model] = {}
            gas_data[model]["data"] = gas_dataset[model].rename({data_var: "co2"}).copy()
            gas_data[model]["metadata"] = copy.deepcopy(metadata)
            gas_data[model]["metadata"]["dataset_source"] = data_var
            gas_data[model]["attributes"] = copy.deepcopy(attributes)

        # Formats data variables typos
        gas_data = dataset_formatter(data=gas_data)

        # Assign attributes to the data for CF compliant NetCDFs
        gas_data = assign_attributes(
            data=gas_data, site=site, update_mismatch=update_mismatch, site_filepath=site_filepath
        )

        return gas_data
