import numpy as np
from numpy import ndarray
import xarray as xr
from xarray import DataArray, Dataset
from pathlib import Path
from typing import DefaultDict, Dict, Optional, Union


ArrayType = Optional[Union[ndarray, DataArray]]


def parse_edgar(filepath: Path, 
                species: str, 
                year: Union[str, int],
                domain: Optional[str] = None,
                lat_out: ArrayType = None,
                lon_out: ArrayType = None,
                period: Optional[Union[str, tuple]] = None,
                edgar_version: Optional[str] = None) -> Dict:
    """
    Read and parse input EDGAR data

    Args:
        filepath: Path to data file
    Returns:
        dict: Dictionary of data
    """
    # from openghg.util import synonyms, molar_mass
    from openghg.util import molar_mass, timestamp_now
    from openghg.util._domain import find_domain
    from openghg.store import infer_date_range
    from openghg.standardise.meta import assign_flux_attributes
    from collections import defaultdict
    import zipfile
    import re
    import os

    # Currently based on acrg.name.emissions_helperfuncs.getedgarannualtotals()
    # Additional edgar functions which could be incorporated.
    # - getedgarv5annualsectors
    # - getedgarv432annualsectors

    # TODO: Add once functionality is available in devel
    # species = synonyms(species, lower=False)
    species_label = species.upper()

    # TODO: Work out how to select frequency
    # - could try and relate to period e.g. "monthly" versus "yearly" etc. 
    # TODO: What about sectoral emissions? Could just start with total for now
    # and move up to sectoral perhaps. Again, something in the readme?

    raw_edgar_domain = "GLOBAL-01x01"

    if lat_out is not None or lon_out is not None:
        if domain is None:
            raise ValueError("Please specify new 'domain' name if selecting new latitude, longitude values")

    if domain is not None:
        try:
            lat_domain, lon_domain = find_domain(domain)
        except ValueError:
            if lat_out is None or lon_out is None:
                raise ValueError("To create new domain please input 'lat_out' and 'lon_out' values.")
        else:
            if lat_out is not None:
                if not np.array_equals(lat_domain, lat_out):
                    raise ValueError(f"Latitude values should not be specified when using pre-defined domain {domain} (values don't match).")
            else:
                lat_out = lat_domain

            if lon_out is not None:
                if not np.array_equals(lon_domain, lon_out):
                    raise ValueError(f"Longitude values should not be specified when using pre-defined domain {domain} (values don't match).")
            else:
                lon_out = lon_domain
    else:
        domain = raw_edgar_domain

    # TODO: Add check for period? Only monthly or yearly (or equivalent inputs)

    # Check if zip file and read.
    if zipfile.is_zipfile(filepath):
        zipped = True
        zip_folder = zipfile.ZipFile(filepath)
        # file = zip_folder.read("")
    else:
        zipped = False

    # Work out version if possible
    # "v6.0"
    #  - "TOTALS_nc.zip" is what is downloaded from website
    #  - contains "_readme.html" file
    #    - top line is "Release: EDGAR v6.0_GHG of May 2021"
    # "v5.0"
    #  - "v50_CH4_1970_2015.zip" can be downloaded (within "CH4" folder)
    #  - all sectors - otherwise sector folders are separate
    #  - contains "_readme.html" file
    #    - top line is "Release: EDGAR v5.0 of November 2019"
    # "v4.3.2"
    #  - "v432_CH4_1970_2012.zip" can be downloaded (within "CH4" folder)
    #  - no readme file this time, just the .xls file

    # TODO: Decide if we actually just want to allow v6.0 for now.

    known_version = ["v432", "v50", "v6.0"]

    # Check for readme html file and, if present, extract version 
    if edgar_version is None:
        readme_filename = "_readme.html"
        if zipped:
            try:
                readme_data: Optional[str] = zip_folder.read(readme_filename)
            except ValueError:
                readme_data = None
        else:
            try:
                readme_filepath = os.path.join(filepath, readme_filename)
                readme_data = open(readme_filepath, "r").read()
            except ValueError:
                readme_data = None            

        if readme_data is not None:
            try:
                title_line = re.search("<title.*?>(.+?)</title>", readme_data).group()
                edgar_version = re.search("v\d[.]+\d[.]?\d*", title_line).group()
            except:
                pass
            else:
                if edgar_version not in known_version:
                    edgar_version = edgar_version.replace('.', '')

    # Extract list of data files
    if zipped:
        folder_filelist = list(zipped.namelist())
    else:
        folder_filelist = list(filepath.glob("*"))

    # Extract netcdf files (for now)
    data_files = [file for file in folder_filelist if file.suffix == ".nc"]

    if not data_files:
        raise ValueError(f"No suitable EDGAR files ('.nc') found within filepath: {filepath}")

    # If version not yet found, extract version from file naming scheme
    if edgar_version is None:
        for file in data_files:
            file = os.path.split(file)[-1]
            possible_version = file.split('_')[0]
            if possible_version in known_version:
                edgar_version = possible_version
                break

    if edgar_version not in known_version:
        raise ValueError(f"Unable to infer EDGAR version ({edgar_version}). Please pass as an argument")

    if isinstance(year, int):
        year = str(year)

    year_search = "\d{4}"
    start_search_str = f"{edgar_version}_{species_label}_{year_search}"

    files_by_year = {}
    for file in data_files:
        try:
            name = file.name
            start = re.search(start_search_str, name).group()
            year_from_file = re.search(year_search, start).group()
        except:
            continue

        files_by_year[year_from_file] = file

        if year_from_file == year:
            edgar_file = file
            break
    else:
        all_years = list(files_by_year.keys())
        all_years.sort()
        start_year, end_year = all_years[0], all_years[-1]
        if year < start_year:
            raise ValueError(f"EDGAR {edgar_version} range: {start_year}-{end_year}. {year} is before this period.")
        elif year > end_year:
            print(f"Using last available year from EDGAR {edgar_version} range: {start_year}-{end_year}.")
            edgar_file = files_by_year[end_year]

    # Dimension - (lat, lon) - no time dimension
    # time is not included in the file just in the filename *sigh*!

    # v432_CH4_1978.0.1x0.1.nc (or .zip)
    # v50_CH4_1978.0.1x0.1.nc (or .zip)
    # v6.0_CH4_1978_TOTALS.0.1x0.1.nc

    # v50_CO2_excl_short-cycle_org_C_1978.0.1x0.1.nc (or .zip)
    # v50_CO2_org_short-cycle_C_1978.0.1x0.1.nc (or .zip)
    # v50_N2O_1978.0.1x0.1.zip (or .zip)

    # e.g. "emis_ch4", "emi_co2"

    edgar_ds = xr.open_dataset(edgar_file)
    name = f'emi_{species.lower()}'

    # Convert from kg/m2/s to mol/m2/s
    species_molar_mass = molar_mass(species)
    kg_to_g = 1e3

    flux_values = edgar_ds[name].values * kg_to_g / species_molar_mass
    units = "mol/m2/s"

    lat_in = edgar_ds.lat.values
    lon_in = edgar_ds.lon.values

    # TODO: Implement regridding below when xesmf / iris can be installed
    # using some combination of pip and conda (or otherwise) for C libraries.
    if domain != raw_edgar_domain:
        # REMOVE WHEN READY
        print(f"Regridding not implemented yet. Saving native EDGAR domain {raw_edgar_domain}")

    if lat_out is not None and lon_out is not None:
        pass
        # # Check range of longitude values and convert to -180 - +180
        # mtohe = lon_in > 180
        # lon_in[mtohe] = lon_in[mtohe] - 360 
        # ordinds = np.argsort(lon_in)
        # lon_in = lon_in[ordinds]
        # flux_values = flux_values[:, ordinds] 
        
        # nlat = len(lat_out)
        # nlon = len(lon_out) 
        
        # narr = np.zeros((nlat, nlon))    

        # # TODO: Sort out which regridding algorithm to use
        # flux_values, reg = regrid2d(flux_values, lat_in, lon_in,
        #                         lat_out, lon_out)
    else:
        lat_out = lat_in
        lon_out = lon_in

    edgar_attrs = edgar_ds.attrs

    time = np.array([f"{year}-01-01"], dtype="datetime64[ns]")
    flux = flux_values[np.newaxis, ...]

    dims = ("time", "lat", "lon")

    em_data = Dataset({"flux": (dims, flux)},
                      coords={"time": time, "lat": lat_out, "lon": lon_out},
                      attrs=edgar_attrs)

    # Some attributes are numpy types we can't serialise to JSON so convert them
    # to their native types here
    attrs = {}
    for key, value in em_data.attrs.items():
        try:
            attrs[key] = value.item()
        except AttributeError:
            attrs[key] = value

    author_name = "OpenGHG Cloud"
    em_data.attrs["author"] = author_name

    date = year
    source = f"edgar{edgar_version}-yearly"

    metadata = {}
    metadata.update(attrs)

    metadata["species"] = species
    metadata["domain"] = domain
    metadata["source"] = source
    metadata["date"] = date
    metadata["author"] = author_name
    metadata["processed"] = str(timestamp_now())

    attrs = {"author": metadata["author"],
             "processed": metadata["processed"]}

    # Infer the date range associated with the flux data
    em_time = em_data.time
    start_date, end_date, period_str = infer_date_range(
        em_time, filepath=filepath, period=period
    )    

    prior_info_dict = {"EDGAR": {"version": f"EDGAR {edgar_version}",
                                 "filename": edgar_file.name,
                                 "raw_resolution": "0.1 degrees x 0.1 degrees",
                                 "reference": edgar_ds.attrs["source"]}
                       }

    metadata["start_date"] = str(start_date)
    metadata["end_date"] = str(end_date)

    metadata["max_longitude"] = round(float(em_data["lon"].max()), 5)
    metadata["min_longitude"] = round(float(em_data["lon"].min()), 5)
    metadata["max_latitude"] = round(float(em_data["lat"].max()), 5)
    metadata["min_latitude"] = round(float(em_data["lat"].min()), 5)

    metadata["time_resolution"] = "standard"
    metadata["time_period"] = period_str

    key = "_".join((species, source, domain, date))

    emissions_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
    emissions_data[key]["data"] = em_data
    emissions_data[key]["metadata"] = metadata
    emissions_data[key]["attributes"] = attrs

    emissions_data = assign_flux_attributes(emissions_data, prior_info_dict=prior_info_dict)

    return emissions_data


# def _find_edgar_version(filepath: Path):
# TODO: Decide how/if this functionality could be split into this separate function.

# def getedgarv5annualsectors(year, lon_out, lat_out, edgar_sectors, species='CH4'):
#     """
#     Get annual emission totals for species of interest from EDGAR v5.0 data
#     for sector or sectors.
#     Regrids to the desired lats and lons.
    
#     CURRENTLY ONLY 2012 AND 2015 ANNUAL SECTORS IN SHARED DIRECTORY. OTHER YEARS NEED DOWNLOADING.
    
#     Args:
#         year (int): 
#             Year of interest
#         lon_out (array): 
#             Longitudes to output the data on
#         lat_out (array):
#             Latitudes to output the data on
#         edgar_sectors (list of str) (optional):
#             EDGAR sectors to include. If list of values, the sum of these will be used.
#             See below for list of possible sectors and full names.
#         species (str):
#             Which species you want to look at. 
#             e.g. species = 'CH4'
#             Default = 'CH4'
#             Currently only works for CH4.
            
#     Returns: 
#         narr (array): 
#             Array of regridded emissions in mol/m2/s.
#             Dimensions are [lat, lon]
            
#     If there is no data for the species you are looking at you may have to 
#     download it from: 
#     https://edgar.jrc.ec.europa.eu/overview.php?v=50_GHG
#     and place in:
#     /data/shared/Gridded_fluxes/<species>/EDGAR_v5.0/yearly_sectoral/ 
    
#     Note:
#         EDGAR sector names:
#         "AGS" = Agricultural soils
#         "AWB" = Agricultural waste burning
#         "CHE" = Chemical processes
#         "ENE" = Power industry
#         "ENF" = Enteric fermentation
#         "FFF" = Fossil fuel fires
#         "IND" = Combustion for manufacturing
#         "IRO" = Iron and steel production
#         "MNM" = Maure management
#         "PRO_COAL" = Fuel exploitation - coal
#         "PRO_GAS" = Fuel exploitation - gas
#         "PRO_OIL" = Fuel expoitation - oil
#         "PRO" = Fuel exploitation - contains coal, oil, gas
#         "RCO" = Energy for buildings
#         "REF_TRF" = Oil refineries and transformational industries
#         "SWD_INC" = Solid waste disposal - incineration
#         "SWD_LDF" = Solid waste disposal - landfill
#         "TNR_Aviation_CDS" = Aviation - climbing and descent
#         "TNR_Aviation_CRS" = Aviation - cruise
#         "TNR_Aviation_LTO" = Aviation - landing and takeoff 
#         "TNR_Other" = Railways, pipelines and off-road transport
#         "TNR_Ship" = Shipping
#         "TRO" = Road transportation
#         "WWT" = Waste water treatment
        
#     """
    
#     edgarfp = os.path.join(data_path,"Gridded_fluxes",species.upper(),"EDGAR_v5.0/yearly_sectoral")
    
#     EDGARsectorlist = ["AGS","AWB","CHE","ENE","ENF","FFF","IND","IRO","MNM",
#                        "PRO_COAL","PRO_GAS","PRO_OIL","PRO","RCO","REF_TRF","SWD_INC",
#                        "SWD_LDF","TNR_Aviation_CDS","TNR_Aviation_CRS",
#                        "TNR_Aviation_LTO","TNR_Other","TNR_Ship","TRO","WWT"]
    
#     if edgar_sectors is not None:
#         print('Including EDGAR sectors.')
    
#         for EDGARsector in edgar_sectors:
#             if EDGARsector not in EDGARsectorlist:
#                 print('EDGAR sector {0} not one of: \n {1}'.format(EDGARsector,EDGARsectorlist))
#                 print('Returning None')
#                 return None
            
#         #edgar flux in kg/m2/s
#         for i,sector in enumerate(edgar_sectors):
        
#             edgarfn = "v50_" + species.upper() + "_" + str(year) + "_" + sector + ".0.1x0.1.nc"

#             with xr.open_dataset(os.path.join(edgarfp,edgarfn)) as edgar_file:
#                 edgar_flux = np.nan_to_num(edgar_file['emi_'+species.lower()].values,0.)
#                 edgar_lat = edgar_file.lat.values
#                 edgar_lon = edgar_file.lon.values

#             if i == 0:
#                 edgar_total = edgar_flux
#             else:
#                 edgar_total = np.add(edgar_total,edgar_flux)
            
#         edgar_regrid_kg,arr = regrid2d(edgar_total,edgar_lat,edgar_lon,lat_out,lon_out)
    
#         #edgar flux in mol/m2/s
#         speciesmm = molar_mass(species)
#         edgar_regrid = (edgar_regrid_kg.data*1e3) / speciesmm
        
#     return(edgar_regrid)
            
# def getedgarv432annualsectors(year, lon_out, lat_out, edgar_sectors, species='CH4'):
#     """
#     Get annual emission totals for species of interest from EDGAR v4.3.2 data
#     for sector or sectors.
#     Regrids to the desired lats and lons.
    
#     If there is no data for the species you are looking at you may have to 
#     download it from: 
#     http://edgar.jrc.ec.europa.eu/overview.php?v=432_GHG&SECURE=123
#     and placed in: 
#     /data/shared/Gridded_fluxes/<species>/EDGAR_v4.3.2/<species>_sector_yearly/ 
    
#     Args:
#         year (int): 
#             Year of interest
#         lon_out (array): 
#             Longitudes to output the data on
#         lat_out (array):
#             Latitudes to output the data on
#         edgar_sectors (list):
#             List of strings of EDGAR sectors to get emissions for.
#             These will be combined to make one array.
#             See 'Notes' for names of sectors
#         species (str):
#             Which species you want to look at. 
#             e.g. species = 'CH4'
#             Default = 'CH4'
    
#     Returns:
#         narr (array): 
#             Array of regridded emissions in mol/m2/s.
#             Dimensions are [lat, lon]
        
#     Notes:
#         Names of EDGAR sectors:
#            'powerindustry'; 
#            'oilrefineriesandtransformationindustry'; 
#            'combustionformanufacturing'; 
#            'aviationclimbinganddescent';  
#            'aviationcruise'; 
#            'aviationlandingandtakeoff';  
#            'aviationsupersonic'; 
#            'roadtransport'; 
#            'railwayspipelinesandoffroadtransport'; 
#            'shipping';  
#            'energyforbuildings';  
#            'fuelexploitation'; 
#            'nonmetallicmineralsproduction';  
#            'chemicalprocesses';
#            'ironandsteelproduction'; 
#            'nonferrousmetalsproduction'; 
#            'nonenergyuseoffuels'; 
#            'solventsandproductsuse'; 
#            'entericfermentation'; 
#            'manuremanagement';  
#            'agriculturalsoils';  
#            'indirectN2Oemissionsfromagriculture'; 
#            'agriculturalwasteburning';  
#            'solidwastelandfills';  
#            'wastewaterhandling';  
#            'Solid waste incineration';  
#            'fossilfuelfires'; 
#            'indirectemissionsfromNOxandNH3';  
#     """
#     species = species.upper() #Make sure species is uppercase
        
# #Path to EDGAR files
#     edpath = os.path.join(data_path,'Gridded_fluxes/'+species+'/EDGAR_v4.3.2/'+species+'_sector_yearly/')

#     #Dictionary of codes for sectors
#     secdict = {'powerindustry' : '1A1a', 
#                'oilrefineriesandtransformationindustry' : '1A1b_1A1c_1A5b1_1B1b_1B2a5_1B2a6_1B2b5_2C1b',
#                'combustionformanufacturing' : '1A2',
#                'aviationclimbinganddescent' : '1A3a_CDS',
#                'aviationcruise' : '1A3a_CRS',
#                'aviationlandingandtakeoff' : '1A3a_LTO',
#                'aviationsupersonic' : '1A3a_SPS',
#                'roadtransport' : '1A3b',
#                'railwayspipelinesandoffroadtransport' : '1A3c_1A3e',
#                'shipping' : '1A3d_1C2',
#                'energyforbuildings' : '1A4',
#                'fuelexploitation' : '1B1a_1B2a1_1B2a2_1B2a3_1B2a4_1B2c',
#                'nonmetallicmineralsproduction' : '2A',
#                'chemicalprocesses': '2B',
#                'ironandsteelproduction' : '2C1a_2C1c_2C1d_2C1e_2C1f_2C2',
#                'nonferrousmetalsproduction' : '2C3_2C4_2C5',
#                'nonenergyuseoffuels' : '2G',
#                'solventsandproductsuse' :  '3',
#                'entericfermentation' : '4A',
#                'manuremanagement' : '4B',
#                'agriculturalsoils' : '4C_4D',
#                'indirectN2Oemissionsfromagriculture' : '4D3',
#                'agriculturalwasteburning' : '4F',
#                'solidwastelandfills' : '6A_6D',
#                'wastewaterhandling' : '6B',
#                'Solid waste incineration' : '6C',
#                'fossilfuelfires' : '7A',
#                'indirectemissionsfromNOxandNH3' : '7B_7C'           
#     } 

#     #Check to see range of years. If desired year falls outside of this range 
#     #then take closest year
#     possyears = np.empty(shape=[0,0],dtype=int)
#     for f in glob.glob(edpath+'v432_'+species+'_*'):
#         fname = f.split('/')[-1]
#         fyear = fname[9:13]      #Extract year from filename
#         possyears = np.append(possyears, int(fyear))
#     if year > max(possyears):
#         print("%s is later than max year in EDGAR database" % str(year))
#         print("Using %s as the closest year" % str(max((possyears))))
#         year = max(possyears)
#     if year < min(possyears):
#         print("%s is earlier than min year in EDGAR database" % str(year))
#         print("Using %s as the closest year" % str(min((possyears))))
#         year = min(possyears)
    
        
#     #Species molar mass
#     speciesmm = molar_mass(species)
# #    if species == 'CH4':
# #        #speciesmm = 16.0425
# #        speciesmm = molar_mass(species)
# #    elif species == 'N2O':
# #        speciesmm = 44.013
# #    else:
# #        print "No molar mass for species %s." % species
# #        print "Please add this and rerun the script"
# #        print "Returning None"
# #        return(None)
    
    
#     #Read in EDGAR data of annual mean CH4 emissions for each sector
#     #These are summed together
#     #units are in kg/m2/s
#     tot = None
#     for sec in edgar_sectors:
#         edgar = edpath+'v432_'+species+'_'+str(year)+'_IPCC_'+secdict[sec]+'.0.1x0.1.nc'    
#         if os.path.isfile(edgar):
#             ds = xr.open_dataset(edgar)
#             soiname = 'emi_'+species.lower()
#             if tot is None:
#                 tot = ds[soiname].values*1e3 / speciesmm
#             else:
#                 tot += ds[soiname].values*1e3 / speciesmm
#         else:
#             print('No annual file for sector %s and %s' % (sec, species))
        
#     lat_in = ds.lat.values
#     lon_in = ds.lon.values
    
#     nlat = len(lat_out)
#     nlon = len(lon_out) 
    
#     narr = np.zeros((nlat, nlon))    
#     narr, reg = regrid2d(tot, lat_in, lon_in,
#                              lat_out, lon_out)
    
#     return(narr)   

# def getedgarmonthlysectors(lon_out, lat_out, edgar_sectors, months=[1,2,3,4,5,6,7,8,9,10,11,12],
#                            species='CH4'):
#     """
#     Get 2010 monthly emissions for species of interest from EDGAR v4.3.2 data
#     for sector or sectors.
#     Regrids to the desired lats and lons.
#     If there is no data for the species you are looking at you may have to 
#     download it from: 
#     http://edgar.jrc.ec.europa.eu/overview.php?v=432_GHG&SECURE=123
#     and place it in: 
#     /data/shared/Gridded_fluxes/<species>/EDGAR_v4.3.2/<species>_sector_monthly/
    
#     Args:
#         lon_out (array): 
#             Longitudes to output the data on
#         lat_out (array):
#             Latitudes to output the data on
#         edgar_sectors (list):
#             List of strings of EDGAR sectors to get emissions for.
#             These will be combined to make one array.
#             See 'Notes' for names of sectors
#         months (list of int; optional): 
#             Desired months.
#         species (str, optional):
#             Which species you want to look at. 
#             e.g. species = 'CH4'
#             Default = 'CH4'
    
#     Returns:
#         narr (array): 
#             Array of regridded emissions in mol/m2/s.
#             Dimensions are [no of months, lat, lon]
        
#     Notes:
#         Names of EDGAR sectors:
#            'powerindustry'; 
#            'oilrefineriesandtransformationindustry'; 
#            'combustionformanufacturing'; 
#            'aviationclimbinganddescent';  
#            'aviationcruise'; 
#            'aviationlandingandtakeoff';  
#            'aviationsupersonic'; 
#            'roadtransport'; 
#            'railwayspipelinesandoffroadtransport'; 
#            'shipping';  
#            'energyforbuildings';  
#            'fuelexploitation'; 
#            'nonmetallicmineralsproduction';  
#            'chemicalprocesses';
#            'ironandsteelproduction'; 
#            'nonferrousmetalsproduction'; 
#            'nonenergyuseoffuels'; 
#            'solventsandproductsuse'; 
#            'entericfermentation'; 
#            'manuremanagement';  
#            'agriculturalsoils';  
#            'indirectN2Oemissionsfromagriculture'; 
#            'agriculturalwasteburning';  
#            'solidwastelandfills';  
#            'wastewaterhandling';  
#            'Solid waste incineration';  
#            'fossilfuelfires'; 
#            'indirectemissionsfromNOxandNH3';  
#     """
#     species = species.upper() #Make sure species is uppercase
#     #Path to EDGAR files
#     edpath = os.path.join(data_path,'Gridded_fluxes/'+species+'/EDGAR_v4.3.2/'+species+'_sector_monthly/')
    
#     #Dictionary of codes for sectors
#     secdict = {'powerindustry' : '1A1a', 
#                'oilrefineriesandtransformationindustry' : '1A1b_1A1c_1A5b1_1B1b_1B2a5_1B2a6_1B2b5_2C1b',
#                'combustionformanufacturing' : '1A2',
#                'aviationclimbinganddescent' : '1A3a_CDS',
#                'aviationcruise' : '1A3a_CRS',
#                'aviationlandingandtakeoff' : '1A3a_LTO',
#                'aviationsupersonic' : '1A3a_SPS',
#                'roadtransport' : '1A3b',
#                'railwayspipelinesandoffroadtransport' : '1A3c_1A3e',
#                'shipping' : '1A3d_1C2',
#                'energyforbuildings' : '1A4',
#                'fuelexploitation' : '1B1a_1B2a1_1B2a2_1B2a3_1B2a4_1B2c',
#                'nonmetallicmineralsproduction' : '2A',
#                'chemicalprocesses': '2B',
#                'ironandsteelproduction' : '2C1a_2C1c_2C1d_2C1e_2C1f_2C2',
#                'nonferrousmetalsproduction' : '2C3_2C4_2C5',
#                'nonenergyuseoffuels' : '2G',
#                'solventsandproductsuse' :  '3',
#                'entericfermentation' : '4A',
#                'manuremanagement' : '4B',
#                'agriculturalsoils' : '4C_4D',
#                'indirectN2Oemissionsfromagriculture' : '4D3',
#                'agriculturalwasteburning' : '4F',
#                'solidwastelandfills' : '6A_6D',
#                'wastewaterhandling' : '6B',
#                'Solid waste incineration' : '6C',
#                'fossilfuelfires' : '7A',
#                'indirectemissionsfromNOxandNH3' : '7B_7C'           
#     }
    
#     print('Note that the only year for monthly emissions is 2010 so using that.')
        
#     #Species molar mass
#     speciesmm = molar_mass(species)
# #    if species == 'CH4':
# #        speciesmm = 16.0425
# #    elif species == 'N2O':
# #        speciesmm = 44.013
# #    else:
# #        print "No molar mass for species %s." % species
# #        print "Please add this and rerun the script"
# #        print "Returning None"
# #        return(None)
    
    
#     #Read in EDGAR data of annual mean CH4 emissions for each sector
#     #These are summed together
#     #units are in kg/m2/s
#     warnings = []
#     first = 0
#     for month in months:
#         tot = np.array(None)
#         for sec in edgar_sectors:
#             edgar = edpath+'v432_'+species+'_2010_'+str(month)+'_IPCC_'+secdict[sec]+'.0.1x0.1.nc'    
#             if os.path.isfile(edgar):
#                 ds = xr.open_dataset(edgar)
#                 soiname = 'emi_'+species.lower()
#                 if tot.any() == None:
#                     tot = ds[soiname].values*1e3 / speciesmm
#                 else:
#                     tot += ds[soiname].values*1e3 / speciesmm
#             else:
#                 warnings.append('No monthly file for sector %s' % sec)
#                 #print 'No monthly file for sector %s' % sec
        
#             if first == 0:
#                 emissions = np.zeros((len(months), tot.shape[0], tot.shape[1]))
#                 emissions[0,:,:] = tot
#             else:
#                 first += 1
#                 emissions[first,:,:] = tot
            
#     for warning in np.unique(warnings):
#         print(warning)
                           
#     lat_in = ds.lat.values
#     lon_in = ds.lon.values
    
#     nlat = len(lat_out)
#     nlon = len(lon_out) 
    
#     narr = np.zeros((nlat, nlon, len(months)))
       
#     for i in range(len(months)):
#        narr[:,:,i], reg = regrid2d(emissions[i,:,:], lat_in, lon_in,
#                              lat_out, lon_out)
#     return(narr)
