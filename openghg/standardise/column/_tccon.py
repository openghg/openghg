from pathlib import Path
from typing import cast
from collections.abc import MutableMapping
from datetime import datetime
import numpy as np
import xarray as xr

from openghg.types import pathType

import logging

logger = logging.getLogger("openghg.standardise.column._tccon")

final_units = {"ch4": 1e-9}


def filter_and_resample(ds: xr.Dataset, species: str, quality_filt: bool, resample: bool) -> xr.Dataset:
    """
    Filter (if quality_filt = True) the data keeping those for which "extrapolation_flags_ak_x{species}" is equal to 2.
    Then resample the data on an hourly scale.
    Args:
        ds: dataset with column concentrations
        species: species name e.g. "ch4"
        quality_filt: if True, filters the data keeping those for which "extrapolation_flags_ak_x{species}" is equal to 2.
        resample: if True resamples the data at hourly scale.
    Returns:
        dataset resampled and filtered (if asked)
    """
    if quality_filt:
        logger.info(f"Applying filter based on variable 'extrapolation_flags_ak_x{species}'.")
        ds = ds.where(abs(ds[f"extrapolation_flags_ak_x{species}"]) != 2)
    ds.dropna("time").sortby("time")

    if not resample:
        return ds

    output = ds.resample(time="h").mean(dim="time")
    output[f"x{species}_uncertainty"] = ds[f"x{species}_error"].resample(time="h").max(dim="time")
    logger.warning(
        "Not sure that we should resample at this stage (and also resample the uncertainty like that)."
    )
    del output[f"extrapolation_flags_ak_x{species}"]
    output = output.dropna("time")

    return output


def define_var_attrs(ds: xr.Dataset, species: str, method: str) -> xr.Dataset:
    """
    Add attributes to "x{species}_uncertainty" and "pressure_weights" variables, add global attribute "derivation_method" that keeps track of the method used to derive the pressure weights.
    Estimate the min and max values of each variables and store them in the attributes.
    Args:
        ds: dataset with column concentrations
        species: species name e.g. "ch4"
        method: method used to calculate the integration_operator.
            Options possible : "pressure_weight" and "integration_operator". method use to calculate the integration_operator.
            Options possible : "pressure_weight" and "integration_operator".
            See https://tccon-wiki.caltech.edu/Main/AuxiliaryDataGGG2020#Calculating_comparison_quantities for description.
    Returns:
        dataset with updated attributes.
    """
    ds[f"x{species}_uncertainty"].attrs = {
        "long_name": f"uncertainty on the x{species} measurement",
        "description": f"max of x{species}_error on resampling period",
        "unit": ds[f"x{species}"].units,
    }

    if method == "pressure_weights":
        ds["pressure_weights"].attrs["long_name"] = "pressure_weights derived using pressure weight method"
        ds["pressure_weights"].attrs[
            "description"
        ] = "see doc https://tccon-wiki.caltech.edu/Main/AuxiliaryDataGGG2020#Using_pressure_weights"
        ds.attrs["derivation_method"] = "pressure weight"

    elif method == "integration_operator":
        ds["pressure_weights"].attrs["long_name"] = "pressure_weights derived using integration_operator"
        ds["pressure_weights"].attrs[
            "description"
        ] = "see doc https://tccon-wiki.caltech.edu/Main/AuxiliaryDataGGG2020#Using_the_integration_operator"
        ds.attrs["derivation_method"] = "integration operator"

    for var in ds.data_vars:
        ds[var].attrs["vmin"] = f"{ds[var].values.min():.1f}"
        ds[var].attrs["vmax"] = f"{ds[var].values.max():.1f}"

    return ds


def convert_prior_profile_to_dry(ds: xr.Dataset, species: list | str) -> None:
    """
    Calculate (inplace) the dry "prior_h2o", "integration_operator" and "prior_{sp}" and ovewrite the ones that are wet.
    Args:
        ds: dataset with column concentrations
        species: species name(s) e.g. "ch4"
    """
    logger.warning(
        f"According to the variables attributes, 'x{species}' is dry but the profiles 'prior_h2o' and 'prior_{species}' are wet, so we dry the profiles. Should check that with the TCCON team before starting to really use the data."
    )

    if isinstance(species, str):
        species = [
            species,
        ]

    if ds["prior_h2o"].attrs["standard_name"] == "wet_atmosphere_mole_fraction_of_water":
        h2o_attrs = ds["prior_h2o"].attrs
        ds["prior_h2o"] = ds["prior_h2o"] / (1 - ds["prior_h2o"])
        ds["prior_h2o"].attrs = {k: v.replace("wet", "dry") for k, v in h2o_attrs.items() if k != "note"}
    elif ds["prior_h2o"].attrs["standard_name"] != "dry_atmosphere_mole_fraction_of_water":
        raise ValueError("'standard_name' of 'prior_h2o' is not what expected. Please check.")

    if "wet" in ds["integration_operator"].attrs["description"]:
        io_attrs = ds["integration_operator"].attrs
        ds["integration_operator"] = ds["integration_operator"] / (1 + ds["prior_h2o"])
        ds["integration_operator"].attrs = {k: v.replace("wet", "dry") for k, v in io_attrs.items()}
    elif "dry" in ds["integration_operator"].attrs["description"]:
        logger.info("'integration_operator' already dried, skipping conversion from wet to dry.")
    else:
        raise ValueError("'description' of 'integration_operator' is not what expected. Please check.")

    for sp in species:
        if ds[f"prior_{sp}"].attrs["standard_name"][:32] == "wet_atmosphere_mole_fraction_of_":
            sp_attrs = ds[f"prior_{sp}"].attrs
            ds[f"prior_{sp}"] = ds[f"prior_{sp}"] * (1 + ds["prior_h2o"])
            ds[f"prior_{sp}"].attrs = {k: v.replace("wet", "dry") for k, v in sp_attrs.items() if k != "note"}
        elif ds[f"prior_{sp}"].attrs["standard_name"][:32] == "dry_atmosphere_mole_fraction_of_":
            logger.info(f"Prior profile of {sp} already dried, skipping conversion from wet to dry.")
        else:
            raise ValueError(f"'standard_name' of 'prior_{sp}' is not what expected. Please check.")


def reformat_convert_units(
    ds: xr.Dataset, species: list | str, final_units: dict | float = {"ch4": 1e-9}
) -> xr.Dataset:
    """
    Convert f"prior_{sp}", f"prior_x{sp}", f"x{sp}", f"x{sp}_uncertainty", f"x{sp}_error" units to the one specified in final_units
    Args:
        ds: dataset with column concentrations
        species: species name(s) e.g. "ch4"
        final_units: dict with species as keys containing the targeted unit (e.g. 1e-9) for each species as values. If float, unit is applied to all.
    Returns:
        dataset with variables converted to desired units.
    """
    if isinstance(species, str):
        species = [
            species,
        ]
    if not isinstance(final_units, dict):
        final_units = {sp: final_units for sp in species}

    unit_converter = {"ppm": 1e-6, "ppb": 1e-9, "ppt": 1e-12}

    for sp in species:
        for var in [f"prior_{sp}", f"prior_x{sp}", f"x{sp}", f"x{sp}_uncertainty", f"x{sp}_error"]:
            with xr.set_options(keep_attrs=True):
                ds[var] = ds[var] * unit_converter[ds[var].attrs["units"]] / final_units[sp]
            ds[var].attrs["units"] = final_units[sp]
    return ds


def parse_tccon(
    filepath: pathType,
    species: str,
    domain: str | None = None,
    pressure_weights_method: str = "integration_operator",
    quality_filt: bool = True,
    resample: bool = True,
    chunks: dict | None = None,
) -> dict:
    """
    Parse and extract data from netcdf downloaded from tccon archive (https://tccondata.org/).

    Args:
        filepath: Path of observation file
        species: Species name or synonym e.g. "ch4"
        domain:
        pressure_weights_method: method use to calculate the integration_operator.
            Options possible : "pressure_weight" and "integration_operator".
            See https://tccon-wiki.caltech.edu/Main/AuxiliaryDataGGG2020#Calculating_comparison_quantities for description.
        quality_filt: If True, filters data keeping data with extrapolation_flags_ak_x{species} equal to 2.
        resample: If True, resample data at hourly scale.
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
            To disable chunking pass in an empty dictionary.
    Returns:
        Dict : Dictionary of source_name : data, metadata, attributes

    """

    filepath = Path(filepath)

    if filepath.suffix.lower() != ".nc":
        raise ValueError("Input file must be a .nc (netcdf) file.")

    splitted_filename = filepath.name.split(".")
    if splitted_filename[1:] != ["public", "qc", "nc"]:
        raise ValueError(
            "File should be of the ending by 'public.qc.nc' when downloaded directly from the tccon archive."
        )

    var_to_read = [
        f"x{species}",
        f"prior_x{species}",
        f"prior_{species}",
        f"ak_x{species}",
        f"extrapolation_flags_ak_x{species}",
        f"x{species}_error",
        "integration_operator",
        "long",
        "lat",
        "prior_h2o",
        "ak_pressure",
        "prior_gravity",
    ]

    data = xr.open_dataset(filepath)[var_to_read].chunk(chunks)

    # Create metadata #
    attributes = cast(MutableMapping, data.attrs)

    attributes["file_start_date"] = datetime.strptime(splitted_filename[0][2:10], "%Y%m%d").strftime(
        "%Y-%m-%d"
    )
    attributes["file_end_date"] = datetime.strptime(splitted_filename[0][11:19], "%Y%m%d").strftime(
        "%Y-%m-%d"
    )

    site_tccon_shortname = splitted_filename[0][:2]

    attributes["species"] = species
    attributes["domain"] = domain
    attributes["site"] = "T" + site_tccon_shortname.upper()
    attributes["network"] = "TCCON"
    attributes["platform"] = "site"
    attributes["inlet"] = "column"

    attributes["original_file_description"] = attributes["description"]
    attributes["description"] = (
        f"TCCON data standardised from {filepath.name}, with the pressure weights estimated via '{pressure_weights_method}'."
    )

    attributes["calibration_scale"] = data[f"x{species}"].attrs.get("wmo_or_analogous_scale", "unknown")

    contact = attributes["contact"].split(" ")
    if "@" in contact[-1]:
        attributes["data_owner"] = (" ").join(contact[:-1])
        attributes["data_owner_email"] = contact[-1]
    else:
        raise ValueError(
            "Couldn't parse the data owner and data owner email, sorry, might have to update the code."
        )

    if data.long.values.std() > 1e-3 or data.lat.values.std() > 1e-3:
        raise ValueError(
            "Longitude and/or latitude seems to be changing over time. This situation is not currently being handled."
        )
    attributes["longitude"] = f"{data.long.values.mean():.3f}"
    attributes["latitude"] = f"{data.lat.values.mean():.3f}"
    logger.warning("Add a check here that the site is really in the domain")

    # Prepare data #
    # Align units
    if data[f"prior_{species}"].units == "ppb" and data[f"prior_x{species}"].units == "ppm":
        with xr.set_options(keep_attrs=True):
            data[f"prior_{species}"] = data[f"prior_{species}"] * 1e-3
        data[f"prior_{species}"].attrs["units"] = "ppm"
    if data[f"prior_{species}"].units != data[f"prior_x{species}"].units:
        raise ValueError(
            f"'prior_{species}' and 'prior_x{species}' have different units, please update this part of code to correct that."
        )

    # Convert wet profile into dry
    convert_prior_profile_to_dry(data, species=species)

    # Define integartion_operator
    if pressure_weights_method == "integration_operator":
        if attributes["file_format_version"][:4] == "2020" and attributes["data_revision"] == "R0":
            raise ValueError(
                f"A bug is affecting the 'integration_operator' variable in version 2020.R0 (see https://tccon-wiki.caltech.edu/Main/AuxiliaryDataGGG2020#Using_the_integration_operator, last access:2025/07/17). Therefore the 'pressure weights should be used instead of 'integration_operator' while standardising {filepath}."
            )

    elif pressure_weights_method == "pressure_weight":
        # Derive pressure thickness
        press = data.ak_pressure.values[:-1] - data.ak_pressure.values[1:]
        press = np.concatenate([press, [data.ak_pressure.values[-1]]]) / data.ak_pressure.values[0]
        data = data.assign({"dpj": (("prior_altitude"), press)})

        # Derive pressure weight (hj), wet to dry conversion factor,
        # dry mole fraction of water (fdry_h2o) and prior dry xch4
        if data["prior_h2o"].attrs["standard_name"] != "dry_atmosphere_mole_fraction_of_water":
            raise ValueError("Looks like the data haven't been dried..")
        M_dryH2O, M_dryAir = 18.0153, 28.9647
        data["hj"] = data["dpj"] / (
            data["prior_gravity"] * M_dryAir * (1 + (data["prior_h2o"] * M_dryH2O / M_dryAir))
        )

        data["integration_operator"] = data["hj"] / data["hj"].sum(dim="prior_altitude")

        # Clean dataset
        data = data.drop_vars(["dpj", "hj"])

    else:
        raise ValueError(
            f"pressure_weights_method = '{pressure_weights_method}' is not a valid option. Options available: 'pressure_weight' or 'integration_operator'."
        )

    data = data.drop_vars(["prior_gravity", "prior_h2o", "long", "lat"])

    # Test coherency between dry and wet stuff
    max_diff = (
        (
            abs(
                data["prior_xch4"]
                - (data["prior_ch4"] * data["integration_operator"]).sum(dim="prior_altitude")
            )
            / data["prior_xch4"]
        )
        .max()
        .values
    )
    if max_diff > 1e-6:
        logger.warning(
            f"Incoherencies between 'x{species}_prior' (supposed dry) and its recalculation from the derived integration operator and dried {species} profile (abs. rel. diff up to {100 * max_diff:.1f}% of 'x{species}_prior'). Is 'x{species}_prior' in tccon file really dry? Or have I misunderstood something?"
        )

    # Filter the data and resample to hourly
    data = filter_and_resample(data, species, quality_filt, resample)

    # reformat units
    data = reformat_convert_units(data, species)

    # Rename variables
    data = data.rename(
        {
            "integration_operator": "pressure_weights",
            "ak_pressure": "pressure_levels",
            f"prior_{species}": f"{species}_profile_apriori",
            f"prior_x{species}": f"x{species}_apriori",
            f"ak_x{species}": f"x{species}_averaging_kernel",
        }
    )

    # Define attributes
    data = define_var_attrs(data, species, pressure_weights_method)

    # Align dimensions
    if all(data["ak_altitude"].values == data["prior_altitude"].values):
        data["altitude"] = data["ak_altitude"]
        lev_coord = np.arange(data["ak_altitude"].size)
        for var in data.data_vars:
            if "ak_altitude" in data[var].dims:
                old_dim = "ak_altitude"
            elif "prior_altitude" in data[var].dims:
                old_dim = "prior_altitude"
            else:
                continue
            new_var = data[var].rename({old_dim: "lev"})
            new_var["lev"] = lev_coord
            data[var] = new_var

        data = data.drop_dims(["ak_altitude", "prior_altitude"])

        data["lev"].attrs["short_description"] = "Number for each level within the vertically resolved data."

    else:
        raise ValueError("'ak_altitude' and ' prior_altitude' are different.")

    # Define metadata
    required_metadata = [
        "species",
        "domain",
        "inlet",
        "site",
        "network",
        "platform",
        "longitude",
        "latitude",
        "data_owner",
        "data_owner_email",
        "file_start_date",
        "file_end_date",
        "file_format_version",
        "data_revision",
        "description",
        "calibration_scale",
    ]
    metadata = {k: attributes[k] for k in required_metadata}

    # Prepare dict to return
    gas_data = {species: {"metadata": metadata, "data": data, "attributes": attributes}}

    return gas_data
