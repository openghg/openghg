#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""

This file taken from the ACRG repo from 


Created on Fri Dec 14 14:02:12 2018

@author: chxmr
"""
from __future__ import print_function

import json
import pandas as pd
import glob
from os import getenv
from os.path import join, split
import xarray as xr
from acrg_obs.utils import attributes, output_filename
import pytz
import numpy as np

# Site info file
acrg_path = getenv("ACRG_PATH")
data_path = getenv("DATA_PATH")
site_info_file = join(acrg_path, "acrg_site_info.json")
with open(site_info_file) as sf:
    site_params = json.load(sf)

# Set default obs folder
obs_directory = join(data_path, "obs_2018/")



def wdcgg_read(fname, species,
               permil = False,
               repeatability_column = None):
    '''
    Read data from World Data Centre for Greenhouse Gases
    '''
    
    skip = 0
    with open(fname, 'r') as f:
        for li in range(200):
            line = f.readline()
            if line[0] == "C":
                skip += 1
                header_line = line
            else:
                break
    
    columns = header_line[4:].split()
    columns[2] = columns[2] + "_2"
    columns[3] = columns[3] + "_2"
    
    df = pd.read_csv(fname, skiprows = skip,
                     sep = r"\s+", names = columns,
                     parse_dates = {"time": ["DATE", "TIME"]},
                     index_col = "time")
    
    sp_file = species[:-1].upper() + species[-1].lower()
    
    df = df.rename(columns = {sp_file: species})
    
    output_columns = {species: species}
    if repeatability_column:
        output_columns[repeatability_column] = species.lower() + " repeatability"
    
    df = df[output_columns.keys()]

    df.rename(columns = output_columns, inplace = True)

    if not permil:
        df = df[df[species] > 0.]
    
    # Drop duplicates
    df.index.name = "index"
    df = df.reset_index().drop_duplicates(subset='index').set_index('index')              
    df.index.name = "time"

    if type(df.index) != pd.tseries.index.DatetimeIndex:
        # Index is not of type time. This is sometimes because time is 99:99
        keep_row = []
        for i in df.index:
            d, t = i.split(" ")
            if t[0:2] == "99":
                keep_row.append(False)
            else:
                keep_row.append(True)

        df = df[keep_row]
        df.index = pd.to_datetime(df.index)

    return df


def nies_read(network, site,
              instrument = "",
              global_attributes = {},
              assume_repeatability = None):
    '''
    Read NIES data files
    '''
    
    directories = glob.glob("/data/shared/obs_raw/" + network + \
                            "/" + site + "/*/*")

    species = []
    fnames = []
    for directory in directories:
        species.append(directory.split("/")[-1])
        fnames.append(glob.glob("/data/shared/obs_raw/" + network + \
                                "/" + site + "/*/" + species[-1] + "/event/*.dat")[0])
                                
    for sp, fname in zip(species, fnames):

        df = wdcgg_read(fname, sp)
        
        if assume_repeatability:
            df[sp + " repeatability"] = df[sp]*assume_repeatability
            global_attributes["Assumed_repeatability_%"] = int(assume_repeatability*100.)
        
        # Sort and convert to dataset
        ds = xr.Dataset.from_dataframe(df.sort_index())
        
        # Add attributes
        ds = attributes(ds,
                        sp,
                        site.upper(),
                        global_attributes = global_attributes,
                        sampling_period = 60,
                        units = "ppt")

        if assume_repeatability:
            ds[sp.lower() + " repeatability"].attrs["Comment"] = \
                "NOTE: This is an assumed value. Contact data owner."
    
        # Write file
        nc_filename = output_filename(obs_directory,
                                      network,
                                      instrument,
                                      site.upper(),
                                      ds.time.to_pandas().index.to_pydatetime()[0],
                                      ds.species)
        
        print("Writing " + nc_filename)        
        ds.to_netcdf(nc_filename)


def nies_wdcgg():

    global_attributes = {"data_owner": "NIES",
                         "data_owner_email": "lnmukaih@nies.go.jp"
                         }
    
    nies_read("NIES", "HAT",
              global_attributes = global_attributes,
              instrument = "GCMS",
              assume_repeatability = 0.03)
    nies_read("NIES", "COI",
              global_attributes = global_attributes,
              instrument = "GCMS",
              assume_repeatability = 0.03)



def nies(fname, species, site, units = "ppt"):
    '''
    Examples of files that this will process:
        fname = "/data/shared/obs_raw/NIES/HAT/HAT_20170804_PFC-318.xlsx"
        species = "PFC-318"
        site = "HAT"
        
        fname = "/data/shared/obs_raw/NIES/HAT/HAT_20170628_CHCl3.txt"
        species = "CHCl3"
        site = "HAT"
    '''

    global_attributes = {"data_owner": "Takuya Saito",
                         "data_owner_email": "saito.takuya@nies.go.jp"
                         }
    
    
    repeatability = {"CFC-11": 0.008}
    
    if fname.split(".")[1] == "xlsx":
        df = pd.read_excel(fname, parse_dates = [0], index_col = [0])
    elif fname.split(".")[1] == "csv":
        df = pd.read_csv(fname, sep = ",", parse_dates = [0], index_col = [0],
                         skipinitialspace=True)
    else:
        df = pd.read_csv(fname, sep = "\t", parse_dates = [0], index_col = [0])
    
    print("Assuming data is in JST. Check input file. CONVERTING TO UTC.")

    df.index = df.index.tz_localize(pytz.timezone("Japan")).tz_convert(None) # Simpler solution

    # Sort
    df.sort_index(inplace = True)

    # Rename columns to species
    df.rename(columns = {df.columns[0]: species}, inplace = True)
    
    # Drop duplicates and rename index
    df.index.name = "index"
    df = df.reset_index().drop_duplicates(subset='index').set_index('index')              
    df.index.name = "time"

    
    # Add a repeatability column
    df[species + " repeatability"] = df[species]*repeatability[species]

    # Convert to dataset
    ds = xr.Dataset.from_dataframe(df)

    ds[species + " repeatability"].attrs["Comment"] = \
        "NOTE: This is an assumed value. Contact data owner."
    
    
    # Add attributes
    ds = attributes(ds,
                    species,
                    site,
                    scale = "Check raw data file or contact data owner",
                    global_attributes = global_attributes,
                    sampling_period = 60,
                    units = units)

    # Write file
    nc_filename = output_filename(obs_directory,
                                  "NIES",
                                  "GCMS",
                                  site.upper(),
                                  ds.time.to_pandas().index.to_pydatetime()[0],
                                  ds.species)

    print("Writing " + nc_filename)
    ds.to_netcdf(nc_filename)


def nies_n2o_ch4(species):
    '''
    N2O files from NIES
    '''            
    params = {
        "site" : "COI",
        "scale": {
            "CH4": "NIES-94",
            "N2O": "NIES-96"},
        "instrument": {
                "CH4": "GCFID",
                "N2O": "GCECD"},
        "global_attributes" : {
                "contact": "Yasunori Tohjima (tohjima@nies.go.jp)" ,
                "averaging": "20 minutes"
                }
        }
    if species.lower() == 'ch4':
        fname = "/data/shared/obs_raw/NIES/COI/COICH4_Hourly_withSTD.TXT"
        df = pd.read_csv(fname, skiprows=1,
                 delimiter=",", names = ["Time", species.upper(), 'sd', 'N'],
                 index_col = "Time", parse_dates=["Time"],
                 dayfirst=True)
    elif species.lower() == 'n2o':
        fname = "/data/shared/obs_raw/NIES/COI/COIN2O_Hourly_withSTD.txt"
        df = pd.read_csv(fname, skiprows=1,
                 delimiter=",", names = ["Time", species.upper(), 'STD', 'n'],
                 index_col = "Time", parse_dates=["Time"],
                 dayfirst=True)
              
    
    print("Assuming data is in JST. Check input file. CONVERTING TO UTC.")
    
    df.index = df.index.tz_localize(pytz.timezone("Japan")).tz_convert(None) # Simpler solution

    # Sort
    df.sort_index(inplace = True)

    # Rename columns to species
    df.rename(columns = {df.columns[0]: species.upper()}, inplace = True)

    df.rename(columns = {df.columns[1]: species.upper() + " repeatability"}, inplace = True)
    df.rename(columns = {df.columns[2]: species.upper() + " number_of_observations"}, inplace = True)
    
    # Drop duplicates and rename index
    df.index.name = "index"
    df = df.reset_index().drop_duplicates(subset='index').set_index('index')              
    df.index.name = "time"
    
    # remove 9999
    df = df[df[species.upper()]<9999]

    # Convert to dataset
    ds = xr.Dataset.from_dataframe(df)
    

    ds = ds.where((ds[species.upper() + " repeatability"] < 9000), drop = True)
    
    # Add attributes

    ds = attributes(ds,
                    species.upper(),
                    params['site'].upper(),
                    global_attributes = params["global_attributes"],
                    scale = params["scale"][species.upper()])
   
    # Write file
    nc_filename = output_filename(obs_directory,
                                  "NIES",
                                  params["instrument"][species.upper()],
                                  params["site"],
                                  ds.time.to_pandas().index.to_pydatetime()[0],
                                  ds.species)
    
    print("Writing " + nc_filename)
    ds.to_netcdf(nc_filename)



def sio_carbon_cycle(species):
    ''' 
    Observations from the SIO carbon cycle group
    
    '''    
    params = {
        "site" : "THD",
        "scale": {
            "CO2": "WMO X2007",
            "DO2N2": "SIO",
            "APO": "SIO"},
        "instrument": {
                "CO2": "LICOR",
                "DO2N2": "DFCA",
                "APO": "DFCA"},
        "directory" : "/data/shared/obs_raw/SIOcarboncycle/",
        "global_attributes" : {
                "contact": "Timothy Lueker <tlueker@ucsd.edu>" ,
                "averaging": "8 minute"
                }
        }
            
    fname = "/data/shared/obs_raw/SIOcarboncycle/Trinidad_full.csv"
    
    if species.lower() == 'do2n2':
        species = 'do2n2'

    df = pd.read_csv(fname, skiprows=3,
             delimiter=",", names = ["YYYY","MM", "D", "HH", "mm", "VLV", "FG-C", "FG-O", 'CO2','DO2N2', 'APO'],
             parse_dates = [[0,1,2,3,4]], index_col = False,  engine='python')

    df["YYYY_MM_D_HH_mm"] = pd.to_datetime(df["YYYY_MM_D_HH_mm"], format = '%Y %m %d %H %M')     
    df = df.set_index("YYYY_MM_D_HH_mm", drop = True)

    # remove duplicates
    df.index.name = "index"
    df = df.reset_index().drop_duplicates(subset='index').set_index('index')              
    df.index.name = "time"

    # hack to filter spurious CO2 and APO data but need more permanent fix!!!! #
    df['CO2'] = np.array(df['CO2'], dtype=float)
    df['DO2N2'] = np.array(df['DO2N2'], dtype=float)
    df['APO'] = np.array(df['APO'], dtype=float)
    df = df[df['CO2']<450]
    df = df[df['CO2']>350]
    df = df[df['APO']<0]
    df = df[df['APO']>-400]

#    df.index = df.index.tz_localize(pytz.timezone("Africa/Johannesburg")).tz_convert(None) # Simpler solution
    if species.lower() == 'co2':
        df = df.drop(["VLV", "FG-C", "FG-O",'DO2N2', 'APO'], axis=1)
    elif species.lower() == 'do2n2':
        df = df.drop(["VLV", "FG-C", "FG-O", 'CO2', 'APO'], axis=1)   
    elif species.lower() == 'apo':
        df = df.drop(["VLV", "FG-C", "FG-O", 'CO2','DO2N2'], axis=1)  
       
    # remove NaN
    df[species.upper()] = np.array(df[species.upper()], dtype=float)
    df = df[np.isfinite(df[species.upper()])]    
    
    # Sort and convert to dataset
    ds = xr.Dataset.from_dataframe(df.sort_index())
    
    
    # Add attributes
    ds = attributes(ds,
                    species.upper(),
                    params['site'].upper(),
                    global_attributes = params["global_attributes"],
                    scale = params["scale"][species.upper()],
                    sampling_period = 8*60)


    # Write file
    nc_filename = output_filename(obs_directory,
                                  "SIOCC",
                                  params["instrument"][species.upper()],
                                  params["site"],
                                  ds.time.to_pandas().index.to_pydatetime()[0],
                                  ds.species)

    print("Writing " + nc_filename)
    ds.to_netcdf(nc_filename)




def ufrank(site = "TAU"):
    '''
    Process Goethe Frankfurt University data files for Taunus

    Inputs are site code and species

    Assumes that file names start with a date, routine will pick the latest one
    '''

    params = {
        "directory" : "/data/shared/obs_raw/UFrank/",
        "directory_output" : "/data/shared/obs_2018",
        "scale": {
            "C4F8": "SIO-14",
            "SO2F2": "SIO-07",
            "HFC32": "SIO-07",
            "HFC125": "UB-98",
            "HFC134A": "SIO-05",
            "HFC143A": "SIO-07",
            "HFC152A": "SIO-05",
            "HFC227EA": "Empa-2005",
            "HFC236FA": "Empa-2009-p",
            "HFC245FA": "Empa-2005",
            "HCFC22": "SIO-05",
            "HCFC141B": "SIO-05",
            "HCFC142B": "SIO-05",
            "CFC11": "SIO-05",
            "CFC12": "SIO-05",
            "CFC113": "SIO-05",
            "CFC114": "SIO-05",
            "CFC115": "SIO-05",
            "HALON1301": "SIO-05",
            "HALON1211": "SIO-05",
            "CH3CL": "SIO-05",
            "CH3BR": "SIO-05",
            "CH3I": "NOAA-Dec09",
            "CH2CL2": "UB-98",
            "CHBR3": "NOAA-Dec09",
            "CH3CCL3": "SIO-05",
            "C2CL4": "NOAA-2003B",
            "COS": "NOAA-SIO-p1"},
        "TAU" : {
            "ufrank_name": "tau",
            "instrument": "GCMS",
            "inlet": "8m",
            "global_attributes": {
                "data_owner": "Tanja Schuck",
                "data_owner_email": "schuck@iau.uni-frankfurt.de"
            }
            }
    }

    # Find name of species
    fnames = glob.glob(join(params["directory"], "*%s*_ufrank.csv" % site.lower()))
    species_ufrank = set([split(f)[-1].split("_")[-2] for f in fnames])
    
    for species in species_ufrank:

        #Pick most recent file
        fname = sorted(glob.glob(join(params["directory"], "*%s*%s*_ufrank.csv" % 
                                      (site.lower(), species))))[-1]

        print("Reading %s ..." %fname)
        
        date_col = "date"
        
        df = pd.read_csv(fname,
                         parse_dates = [date_col],
                         index_col = [date_col]).sort_index()

        df.rename(columns = {species + "_SD": species + " repeatability"},
                  inplace = True)
        
        # Drop duplicates
        df.index.name = "index"
        df = df.reset_index().drop_duplicates(subset='index').set_index('index')              
        df.index.name = "time"
        
        ds = xr.Dataset.from_dataframe(df.sort_index())
        
        global_attributes = params[site]["global_attributes"]
        global_attributes["inlet_magl"] = params[site]["inlet"]
        
        ds = attributes(ds,
                        species,
                        site,
                        global_attributes = global_attributes,
                        scale = params["scale"][species.upper()],
                        sampling_period = 60,
                        units = "ppt")
        
        # Write file
        nc_filename = output_filename(params["directory_output"],
                                      "UFRANK",
                                      params[site]["instrument"],
                                      site.upper(),
                                      ds.time.to_pandas().index.to_pydatetime()[0],
                                      ds.species,
                                      None)
        
        print(" ... writing " + nc_filename)
        
        ds.to_netcdf(nc_filename)

