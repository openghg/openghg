#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 22 13:42:42 2021

This is created to find data files on the Blue Pebble system using the
expected file naming scheme.

This is adapted from acrg_obs.process_gcwerks and makes use of a copy
of the process_gcwerks_parameters.json file (also in this folder).

@author: rt17603
"""

from pathlib import Path
import glob
import json
from os.path import join

# Read site info file
info_file = "process_gcwerks_parameters.json"
with open(info_file) as sf:
    params = json.load(sf)


def find_gc_files(site, instrument):
    '''
    Find files from GC instruments.
    
    Args:
        site (str) - three-letter site code e.g. "MHD"
        instrument (str) - one of "GCMD, "GCMS" or "medusa"
    Returns:
        list(tuple):
            List of tuple pairs for data file and associated 
            GCWERKS precision data file.
    '''

    try:
        site_gcwerks = params["GC"][site]["gcwerks_site_name"]
        instrument_gcwerks = params["GC"]["instruments"][instrument]
        data_folder = params["GC"]["directory"][instrument]

        suffixes = params["GC"]["instruments_suffix"][instrument]
    except KeyError:
        print("Unable to extract data files")
        print(f"Instrument {instrument} or site {site} not found within json parameters file")
        return []
    
    for suffix in suffixes:

        fname_search = f"{site_gcwerks}{instrument_gcwerks}{suffix}.??.C"
        search_string = join(data_folder, fname_search)
        
        data_files = sorted(glob.glob(search_string))
        
        if len(data_files) > 0:
            break

    precision_files = [data_file[0:-2] + ".precisions.C" \
                            for data_file in data_files]

    data_tuples = [(data_file, precision_file) 
                  for data_file, precision_file in zip(data_files,precision_files)]
    
    return data_tuples

def find_crds_files(site):
    '''
    Find files from CRDS instruments.
    
    Args:
        site (str) - three-letter site code e.g. "MHD"
    Returns:
        list:
            List of data file names for that site
    '''
    
    try:
        # Get directories and site strings
        params_crds = params["CRDS"]
        site_string = params_crds[site]["gcwerks_site_name"]
    
        data_folder = params_crds["directory"].replace("%site", site_string)
    except KeyError:
        print("Unable to extract data files")
        print(f"site {site} not found within json parameters file for CRDS instrument")
        return []     

    # Find files
    fname_search = f"{site.lower()}.*.1minute.*.dat"
    data_file_search = join(data_folder, fname_search)
    data_files = glob.glob(data_file_search)

    return data_files

def find_icos_files(site):
    '''
    Find data files for ICOS sites.

    Args:
        site (str) - three-letter site code e.g. "MHD"
    Returns:
        list:
            List of data file names for that site
    '''    
    
    try:
        # Get directories and site strings
        params_icos = params["ICOS"]
        site_string = params_icos[site]["gcwerks_site_name"]
    
        data_folder = params_icos["directory"].replace("%site", site_string)

    except KeyError:
        print("Unable to extract data files")
        print(f"site {site} not found within json parameters file for CRDS instrument")
        return []

    # Find files
    fname_search = f"{site.lower()}.*.1minute.*.dat"
    data_file_search = join(data_folder, fname_search)
    data_files = glob.glob(data_file_search)

    return data_files

def data_type_function():
    
    data_type_dict = {"GCWERKS": find_gc_files,
                      "CRDS": find_crds_files,
                      "ICOS": find_icos_files}
    return data_type_dict
    

def site_all():
    '''
    Defines inputs needed to find the files for sites within the AGAGE, 
    DECC and ICOS networks which are loaded as standard into our 
    object store.
    
    This is split into three data types (based on the necessary processing):
        - GCWERKS
        - CRDS
        - ICOS
    
    To find the data files and then to load the data details are needed
    for:
        - site
        - network
        - instrument (for GCWERKS only)
    
    Returns:
        dict :
            Associated data definitions for each data type.
    '''
    # GCWERKS needs both site and instrument to find the file name
    gc_werks_input = \
      [
        #AGAGE Medusa
        {"site": "MHD", "instrument":"medusa","network":"AGAGE"},
        {"site": "CGO", "instrument":"medusa","network":"AGAGE"},
        {"site": "GSN", "instrument":"medusa","network":"AGAGE"},        
        {"site": "SDZ", "instrument":"medusa","network":"AGAGE"},
        {"site": "THD", "instrument":"medusa","network":"AGAGE"},
        {"site": "RPB", "instrument":"medusa","network":"AGAGE"},
        {"site": "SMO", "instrument":"medusa","network":"AGAGE"},
        {"site": "SIO", "instrument":"medusa","network":"AGAGE"},
        {"site": "JFJ", "instrument":"medusa","network":"AGAGE"},
        {"site": "CMN", "instrument":"medusa","network":"AGAGE"},
        {"site": "ZEP", "instrument":"medusa","network":"AGAGE"},         
        # AGAGE GC data
        {"site": "RPB", "instrument":"GCMD","network":"AGAGE"},
        {"site": "CGO", "instrument":"GCMD","network":"AGAGE"},
        {"site": "MHD", "instrument":"GCMD","network":"AGAGE"},
        {"site": "SMO", "instrument":"GCMD","network":"AGAGE"},
        {"site": "THD", "instrument":"GCMD","network":"AGAGE"},
        # AGAGE GCMS data
        {"site": "CGO", "instrument":"GCMS","network":"AGAGE"},
        {"site": "MHD", "instrument":"GCMS","network":"AGAGE"},
        {"site": "RPB", "instrument":"GCMS","network":"AGAGE"},
        {"site": "SMO", "instrument":"GCMS","network":"AGAGE"},
        {"site": "THD", "instrument":"GCMS","network":"AGAGE"},
        {"site": "JFJ", "instrument":"GCMS","network":"AGAGE"},
        {"site": "CMN", "instrument":"GCMS","network":"AGAGE"},
        {"site": "ZEP", "instrument":"GCMS","network":"AGAGE"},
        # GAUGE and DECC GC data
        {"site": "BSD", "instrument":"GCMD","network":"DECC"},
        {"site": "HFD", "instrument":"GCMD","network":"DECC"},
        {"site": "TAC", "instrument":"GCMD","network":"DECC"},
        {"site": "RGL", "instrument":"GCMD","network":"DECC"},
        # DECC Medusa
        {"site": "TAC", "instrument":"medusa","network":"DECC"},
      ]
    
    crds_input = \
      [
        # AGAGE CRDS data
        {"site": "RPB","network":"AGAGE"},
        # GAUGE and DECC CRDS data
        {"site": "HFD","network":"DECC"},
        {"site": "BSD","network":"DECC"},
        {"site": "TTA","network":"DECC"},
        {"site": "RGL","network":"DECC"},
        {"site": "TAC","network":"DECC"}        
      ]


    icos_input = \
      [
        {"site": "MHD","network":"ICOS"}
      ]
    
    instrument_details = {"GCWERKS":gc_werks_input,
                          "CRDS":crds_input,
                          "ICOS":icos_input}

    return instrument_details

def find_all_files():
    '''
    Finds all the filenames for sites within the AGAGE, 
    DECC and ICOS networks which are loaded as standard into our 
    object store.
    See site_all() function for full list.
    
    Each input contains the keys needed for ObsSurface.read_file method:
     - "filepath"
     - "data_type"
     - "site"
     - "network"
    
    Returns:
        list (dict) :
            List of each input as a dictionary in the form 
            appropriate to pass to the read_file() function.
    
    '''
    all_instrument_details = site_all()
    find_functions = data_type_function()
    
    data_types = list(all_instrument_details.keys())
    
    data_files = []
    for data_type in data_types:
        data_details = all_instrument_details[data_type]
        fn_find = find_functions[data_type]
        for data in data_details:
            # Find all expected parameters in function and extract matching
            # parameters from the inputs
            input_param = fn_find.__code__.co_varnames[:fn_find.__code__.co_argcount]
            param = {key: value for key, value in data.items() if key in input_param}

            files = fn_find(**param)
            
            read_input_dict = {"filepath":files,
                               "data_type":data_type,
                               "site": data["site"],
                               "network":data["network"]}
            
            data_files.append(read_input_dict)
    
    return data_files




###

#from pathlib import Path
#data_directory = Path("/work/chxmr/shared/obs_raw/")

# def find_gc_files(sitename, instrument, 
#                   data_directory = Path("/work/chxmr/shared/obs_raw/")):
#     '''
#     DEPRECATED IN FAVOUR OF OPTION ABOVE - this is independent of json
#     file
#     Args:
#         sitename (str) - match to name within file for now e.g. "macehead"
#         instrument (str) - one of "GCMD, "GCMS" or "medusa"
#     Returns:
#         list(tuple):
#             List of tuple pairs for data file and associated 
#             GCWERKS precision data file.
#     '''
#     if instrument == "GCMD":
#         data_directory = data_directory / "AGAGE_GCWerks/data/"
#         suffixes = ["", "-md"]
#     elif instrument == "GCMS" or instrument == "medusa":
#         data_directory = data_directory / "AGAGE_GCWerks/data-gcms/"
#         if instrument == "GCMS":
#             suffixes = ["", "-gcms"]
#         else:
#             suffixes = ["-medusa"]
    
#     for suffix in suffixes:
    
#         search_str = str(data_directory / f"{sitename}{suffix}.??.C")
#         data_files = glob.glob(search_str)
#         ##data_files = list(data_directory.glob(f"{sitename}{suffix}.??.C"))
        
#         if len(data_files) > 0:
#             break

#     precision_files = [data_file[0:-2] + ".precisions.C" \
#                             for data_file in data_files]

#     data_tuples = [(data_file, precision_file) 
#                   for data_file, precision_file in zip(data_files,precision_files)]
    
#     return data_tuples
