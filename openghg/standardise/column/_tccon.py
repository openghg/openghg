from pathlib import Path
from typing import cast
from collections.abc import MutableMapping
from datetime import datetime
import numpy as np
import xarray as xr

from openghg.types import pathType

import logging

logger = logging.getLogger("openghg.standardise.column._tccon")

def filter_and_resample(ds,species,quality_filt):
    if quality_filt:
        logger.info(f"Applying filter based on variable'extrapolation_flags_ak_x{species}'.")
        ds = ds.where(abs(ds[f'extrapolation_flags_ak_x{species}'])!=2)
    ds.dropna('time').sortby('time')
    tmp = ds.resample(time='h').mean(dim='time')
    tmp[f'x{species}_uncertainty'] = ds[f'x{species}_error'].resample(time='h').max(dim='time')
    del tmp[f'extrapolation_flags_ak_x{species}']
    tmp = tmp.dropna('time')

    return tmp

def define_var_attrs(ds,species,method):
    ds[f'x{species}_uncertainty'].attrs={'long_name':f'uncertainty on the x{species} measurement',
                               'description':f'max of x{species}_error on resampling period',
                               'unit':ds[f"x{species}"].units,
                               'vmin':str(ds[f'x{species}_uncertainty'].values.min()),
                               'vmax':str(ds[f'x{species}_uncertainty'].values.max())}

    if method=='integration_operator':
        ds['obs'].attrs['description']='xch4-prior_xch4-sum(ak_footprint*prior_ch4,dim="ak_altitude")'

        ds[f"x{species}_averaging_kernel"].attrs['long_name']='transformed ak using integration operator method'
        ds[f"x{species}_averaging_kernel"].attrs['description']='ak_xch4*integration_operator'
        ds.attrs['derivation_method'] = 'Integration operator'
        # ds['dry_to_wet'].attrs={'long_name':'dry_to_wet',
        #                         'description':'ratio to use to convert from dry to wet mole fraction. Derived as 1/(1+prior_h2o)',
        #                         'units':1,
        #                         'vmin':str(ds.dry_to_wet.values.min()),
        #                         'vmax':str(ds.dry_to_wet.values.max())}

    elif method=='pressure_weights':
        ds[f"x{species}_averaging_kernel"].attrs['long_name']='ak derived using pressure weight method'
        ds[f"x{species}_averaging_kernel"].attrs['description']='see doc xxx'  
        ds.attrs['derivation_method'] = 'Pressure weight' 
        
        # ds['wet_to_dry'].attrs={'long_name':'wet_to_dry',
        #                         'description':'ratio to use to convert from wet to wet dry fraction. Derived as 1/(1-prior_h2o)',
        #                         'units':1,
        #                         'vmin':str(ds.wet_to_dry.values.min()),
        #                         'vmax':str(ds.wet_to_dry.values.max())}

    return ds

def parse_tccon(
    filepath: pathType,
    domain: str | None = None,
    species: str | None = None,
    pressure_weights_method: str = "integration_operator",
    max_level: int = 24,
    quality_filt: bool = True,
    chunks: dict | None = None,
) -> dict:
    """
    Parse and extract data from netcdf downloaded from tccon archive (https://tccondata.org/).
    """
    from openghg.standardise.meta import define_species_label
    from openghg.util import clean_string

    # from openghg.standardise.meta import attributes_default_keys, assign_attributes

    filepath = Path(filepath)

    if filepath.suffix.lower() != ".nc":
        raise ValueError("Input file must be a .nc (netcdf) file.")

    splitted_filename = filepath.name.split('.')
    if splitted_filename[1:] != ['public', 'qc', 'nc']:
        raise ValueError("File should be of the ending by 'public.qc.nc' when downloaded directly from the tccon archive.")
    
    var_to_read = [f"x{species}", f"prior_x{species}",
                   f"prior_{species}", f"ak_x{species}",
                   f'extrapolation_flags_ak_x{species}',
                   f'x{species}_error',
                   "integration_operator",
                   "long","lat",
                   "prior_h2o",
                   "ak_pressure",
                   "prior_gravity"
                   ]

    data = xr.open_dataset(filepath)[var_to_read].chunk(chunks)

    ### Create metadata ###
    attributes = cast(MutableMapping, data.attrs)

    attributes["file_start_date"] = datetime.strptime(splitted_filename[0][2:10], "%Y%m%d"
                                                      ).strftime("%Y-%m-%d")
    attributes["file_end_date"] = datetime.strptime(splitted_filename[0][11:19], "%Y%m%d"
                                                    ).strftime("%Y-%m-%d")

    site_tccon_shortname = splitted_filename[0][:2]

    attributes["species"] = species
    attributes["domain"] = domain
    attributes["site"] = "T" + site_tccon_shortname.upper()
    attributes["network"] = "TCCON"
    attributes["platform"] = "site"

    attributes["original_file_description"] = attributes["description"]
    attributes["description"] = f"TCCON data standardised from {filepath.name}, with the pressure weights estimated via '{pressure_weights_method}'."

    contact = attributes["contact"].split(' ')
    if "@" in contact[-1]:
        attributes["data_owner"] = (" ").join(contact[:-1])
        attributes["data_owner_email"] = contact[-1]
    else:
        raise ValueError("Couldn't parse the data owner and data owner email, sorry, might have to update the code.")
    
    if data.long.values.std() > 1e-3 or data.lat.values.std() > 1e-3:
        raise ValueError("Longitude and/or latitude seems to be changing over time. This situation is not currently being handled.")
    attributes["longitude"] = f"{data.long.values.mean():.3f}"
    attributes["latitude"] = f"{data.lat.values.mean():.3f}"
    logger.warning("Add a check here that the site is really in the domain")

    ### Prepare data ###
    # Select the levels
    if max_level > data.ak_altitude.size:
        raise ValueError(f"max_level ({max_level}) is greater than actual number of levels ({data.ak_altitude.size}).")
    data = data.isel(ak_altitude=slice(0,max_level)).isel(prior_altitude=slice(0,max_level))

    # Align units
    if data[f'prior_{species}'].units == 'ppb' and data[f'prior_x{species}'].units == 'ppm' :
        data[f'prior_{species}'].attrs['units'] = 'ppm'
    if data[f'prior_{species}'].units != data[f'prior_x{species}'].units:
        raise ValueError(f"'prior_{species}' and 'prior_x{species}' have different units, please update this part of code to correct that.")

    if pressure_weights_method == "integration_operator":
        if attributes["file_format_version"][:4] == "2020" and attributes["data_revision"] == "R0":
            raise ValueError(f"A bug is affecting the 'integration_operator' variable in version 2020.R0 (see https://tccon-wiki.caltech.edu/Main/AuxiliaryDataGGG2020#Using_the_integration_operator, last access:2025/07/17). Therefore the 'pressure weights should be used instead of 'integration_operator' while standardising {filepath}.")
        # data['dry_to_wet'] = 1/(1+data['prior_h2o'])

    elif pressure_weights_method == "pressure_weight":
        # Derive pressure thickness
        press = data.ak_pressure.values[:-1]- data.ak_pressure.values[1:]
        press = np.concatenate([press,[data.ak_pressure.values[-1]]])/data.ak_pressure.values[0]
        data = data.assign({'dpj':(('prior_altitude'),press)})
    
        # Derive pressure weight (hj), wet to dry conversion factor,
        # dry mole fraction of water (fdry_h2o) and prior dry xch4
        M_dryH2O,M_dryAir = 18.0153,28.9647
        data['wet_to_dry'] = 1/(1-data['prior_h2o'])
        data['fdry_h2o'] = data['prior_h2o']*data['wet_to_dry']
        data['hj'] = data['dpj']/(data['prior_gravity']
                                    *M_dryAir
                                    *(1+(data['fdry_h2o']
                                         *M_dryH2O/M_dryAir)))
        
        data["integration_operator"] = data['hj']/data['hj'].sum(dim='prior_altitude')

        data['prior_xch4'] = (data['integration_operator']*data['wet_to_dry']*data['prior_ch4']
                              ).sum(dim='prior_altitude')                             
        logger.warning("The rewriting of 'prior_xch4' by a 'dried' one seems odd to me. But I feel like I followed what is written in https://tccon-wiki.caltech.edu/Main/AuxiliaryDataGGG2020#Using_pressure_weights")

        data = data.drop_vars(['dpj','wet_to_dry','fdry_h2o','hj'])

    else:
        raise ValueError(f"pressure_weights_method = '{pressure_weights_method}' is not a valid option. Options available: 'pressure_weight' or 'integration_operator'.")
    
    data = data.drop_vars(["prior_gravity","prior_h2o","long","lat"])

    logger.warning("The fuss about dry and wet mole fractions between the two methods should be checked again.")

    # Filter the data and resample to hourly
    data = filter_and_resample(data,species,quality_filt)
    
    # Rename variables
    data = data.rename({"integration_operator": "pressure_weights",
                          'ak_pressure':'pressure_levels',
                          f"prior_{species}":f"{species}_profile_apriori",
                          f"prior_x{species}":f"x{species}_apriori",
                          f"ak_x{species}": f"x{species}_averaging_kernel"})
    
    # Define attributes
    data = define_var_attrs(data,species,pressure_weights_method)

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

        data = data.drop_dims(["ak_altitude","prior_altitude"])

        data["lev"].attrs["short_description"] = "Number for each level within the vertically resolved data."
    
    else:
        raise ValueError("'ak_altitude' and ' prior_altitude' are different.")
        
    ### Define metadata
    required_metadata = ["species","domain",
                         "site","network","platform",
                         "longitude","latitude",
                         "data_owner","data_owner_email",
                         "file_start_date","file_end_date",
                         "file_format_version","data_revision",
                         "description"
                         ]
    metadata = {k:attributes[k] for k in required_metadata}

    ### Prepare dict to return
    gas_data = {species: {"metadata": metadata, "data": data, "attributes": attributes}}

    return gas_data