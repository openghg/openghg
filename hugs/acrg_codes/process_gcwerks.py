# -*- coding: utf-8 -*-
"""

This file taken from the ACRG repo
under acrg_obs

Created on Fri Oct 16 14:08:07 2015

@author: chxmr
"""
from __future__ import print_function

from builtins import zip
from builtins import range
import numpy as np
import pandas as pd
from os.path import join, split
from datetime import datetime as dt
import glob
import xarray as xray
import json
from os import getenv, stat
import fnmatch
from acrg_obs.utils import attributes, output_filename, cleanup

# Read site info file
acrg_path = getenv("ACRG_PATH")
info_file = join(acrg_path,
                 "acrg_obs/process_gcwerks_parameters.json")
with open(info_file) as sf:
    params = json.load(sf)

site_info_file = join(acrg_path, "acrg_site_info.json")
with open(site_info_file) as sf:
    site_params = json.load(sf)

# Output unit strings (upper case for matching)
unit_species = {"CO2": "1e-6",
                "CH4": "1e-9",
                "C2H6": "1e-9",
                "N2O": "1e-9",
                "CO": "1e-9",
                "DCH4C13": "1",
                "DCH4D": "1",
                "DCO2C13": "1",
                "DCO2C14": "1",
                "DO2N2" : "1",
                "APO" : "1"}

# If units are non-standard, this attribute can be used
unit_species_long = {"DCH4C13": "permil",
                     "DCH4D": "permil",
                     "DCO2C13": "permil",
                     "DCO2C14": "permil",
                     "DO2N2" : "per meg",
                     "APO" : "per meg"}

unit_interpret = {"ppm": "1e-6",
                  "ppb": "1e-9",
                  "ppt": "1e-12",
                  "ppq": "1e-15",
                  "else": "unknown"}

# Default calibration scales
# TODO: Remove this? seems dangerous
scales = {"CO2": "NOAA-2007",
          "CH4": "NOAA-2004A",
          "N2O": "SIO-98",
          "CO": "Unknown"}


# For species which need more than just a hyphen removing or changing to lower case
# First element of list is the output variable name,
# second is the long name for variable standard_name and long_name
# Keys are upper case
species_translator = {"CO2": ["co2", "carbon_dioxide"],
                      "CH4": ["ch4", "methane"],
                      "ETHANE": ["c2h6", "ethane"],
                      "PROPANE": ["c3h8", "propane"],
                      "C-PROPANE": ["cc3h8", "cpropane"],
                      "BENZENE": ["c6h6", "benzene"],
                      "TOLUENE": ["c6h5ch3", "methylbenzene"],
                      "ETHENE": ["c2f4", "ethene"],
                      "ETHYNE": ["c2h2", "ethyne"],
                      "N2O": ["n2o", "nitrous_oxide"],
                      "CO": ["co", "carbon_monoxide"],
                      "H-1211": ["halon1211", "halon1211"],
                      "H-1301": ["halon1301", "halon1301"],
                      "H-2402": ["halon2402", "halon2402"],
                      "PCE": ["c2cl4", "tetrachloroethylene"],
                      "TCE": ["c2hcl3", "trichloroethylene"],
                      "PFC-116": ["c2f6", "hexafluoroethane"],
                      "PFC-218": ["c3f8", "octafluoropropane"],
                      "PFC-318": ["c4f8", "cyclooctafluorobutane"],
                      "F-113": ["cfc113", "cfc113"],
                      "H2_PDD": ["h2", "hydrogen"],
                      "NE_PDD": ["Ne", "neon"],                      
                      "DO2N2": ["do2n2", "ratio_of_oxygen_to_nitrogen"],
                      "DCH4C13": ["dch4c13", "delta_ch4_c13"],
                      "DCH4D": ["dch4d", "delta_ch4_d"],
                      "DCO2C13": ["dco2c13", "delta_co2_c13"],
                      "DCO2C14": ["dco2c14", "delta_co2_c14"],
                      "APO": ["apo", "atmospheric_potential_oxygen"]
                      }

# Translate header strings
crds_header_string_interpret = {"C": "",
                                "stdev": " variability",
                                "N": " number_of_observations"}

def parser_YYMMDD(yymmdd):
    return dt.strptime(yymmdd, '%y%m%d')


def get_directories(default_input_directory,
                    default_output_directory,
                    user_specified_input_directory = None,
                    user_specified_output_directory = None):

    # If an output directory directory is set, use that, otherwise from json file
    if user_specified_output_directory:
        output_folder = user_specified_output_directory
    else:
        output_folder = default_output_directory

    # If an input directory directory is set, use that, otherwise from json file
    if user_specified_input_directory:
        if user_specified_output_directory is None:
            print("EXITING: You must also specify an output folder if you specify an input")
            return None
        data_folder = user_specified_input_directory
    else:
        data_folder = default_input_directory

    return data_folder, output_folder


# ICOS
########################################################

def icos_data_read(data_file, species):

    print("Reading " + data_file)

    # Find out how many header lines there are
    nheader = 0
    with open(data_file, "rb") as f:
        for l in f:
            if l[0] != "#":
                break
            nheader += 1

    # Read CSV file
    df =  pd.read_csv(data_file,
                      skiprows = nheader-1,
                      parse_dates = {"time": ["Year", "Month", "Day", "Hour", "Minute"]},
                      index_col = "time",
                      sep = ";",
                      usecols = ["Day", "Month", "Year", "Hour", "Minute",
                                 str(species.lower()), "SamplingHeight",
                                 "Stdev", "NbPoints"],
                      dtype = {"Day": np.int,
                               "Month": np.int,
                               "Year": np.int,
                               "Hour": np.int,
                               "Minute": np.int,
                               species.lower(): np.float,
                               "Stdev": np.float,
                               "SamplingHeight": np.float,
                               "NbPoints": np.int},
                      na_values = "-999.99")

    # Format time
    df.index = pd.to_datetime(df.index, format = "%Y %m %d %H %M")

    df = df[df[species.lower()] >= 0.]

    # Remove duplicate indices
    df.reset_index(inplace = True)
    df = df.reset_index().drop_duplicates(subset='index').set_index('index')

    # Rename columns
    df.rename(columns = {species.lower(): species.upper(),
                         "Stdev": species.upper() + " variability",
                         "NbPoints": species.upper() + " number_of_observations"},
               inplace = True)

    df.index.name = "time"

    # Convert to Dataset
    ds = xray.Dataset.from_dataframe(df.sort_index())

    return ds


def icos(site, network = "ICOS",
         input_directory = None,
         output_directory = None,
         date_range = None,
         version = None):

    # Get directories and site strings
    params_icos = params["ICOS"]
    site_string = params_icos[site]["gcwerks_site_name"]

    data_folder, output_folder = \
            get_directories(params_icos["directory"].replace("%site", site_string),
                            params_icos["directory_output"],
                            user_specified_input_directory = input_directory,
                            user_specified_output_directory = output_directory)

    # Search for species and inlets from file names
    data_file_search = join(data_folder, site.lower() + ".*.1minute.*.dat")
    data_files = glob.glob(data_file_search)
    data_file_names = [split(f)[1] for f in data_files]
    species_and_inlet = [(f.split(".")[1], f.split(".")[-2]) \
                         for f in data_file_names]

    for i, (species, inlet) in enumerate(species_and_inlet):

        if stat(data_files[i]).st_size > 0:

            # Create Pandas dataframe
            ds = icos_data_read(data_files[i], species.upper())

            # Sort out attributes
            global_attributes = params_icos[site.upper()]["global_attributes"]
            global_attributes["inlet_height_magl"] = float(params_icos[site]["inlet_rename"][inlet][:-1])

            ds = attributes(ds,
                            species.upper(),
                            site.upper(),
                            global_attributes = global_attributes,
                            sampling_period = 60,
                            date_range = date_range)

            if len(ds.time.values) == 0:
                
                # Then must have not passed date_range filter?
                print(" ... no data in range")
                # then do nothing

            else:
    
                # Write file
                nc_filename = output_filename(output_folder,
                                              network,
                                              "CRDS",
                                              site.upper(),
                                              ds.time.to_pandas().index.to_pydatetime()[0],
                                              ds.species,
                                              params_icos[site]["inlet_rename"][inlet],
                                              version = version)
    
                ds.to_netcdf(nc_filename)
    
                print("Written " + nc_filename)

        else:
            print("Skipping empty file: %s" % data_files[i])

# GC FUNCTIONS
###############################################################

def gc_data_read(dotC_file, scale = {}, units = {}):

    species = []

    # Read header
    header = pd.read_csv(dotC_file,
                         skiprows=2,
                         nrows=2,
                         header = None,
                         sep=r"\s+")

    # Read data
    df = pd.read_csv(dotC_file,
                     skiprows=4,
                     sep=r"\s+")

    # Time index
    
    time = []
    time_analysis = []
    for i in range(len(df)):
        # sampling time
        time.append(dt(df.yyyy[i], df.mm[i], df.dd[i], df.hh[i], df.mi[i]))
        # Read analysis time
        if "ryyy" in list(df.keys()):
            time_analysis.append(dt(df.ryyy[i], df.rm[i], df.rd[i], df.rh[i], df.ri[i]))
        
    df.index = time
#    df["analysis_time"] = time_analysis

    # Drop duplicates
    df = df.reset_index().drop_duplicates(subset='index').set_index('index')

    # Rename flag column with species name
    for i, key in enumerate(df.keys()):
        if key[0:4] == "Flag":
            quality_flag = []
            area_height_flag = []
            for flags in df[key].values:

                # Quality flag
                if flags[0] == "-":
                    quality_flag.append(0)
                else:
                    quality_flag.append(1)

                # Area/height
                if flags[1] == "-":
                    area_height_flag.append(0)  # Area
                else:
                    area_height_flag.append(1)  # Height

            df = df.rename(columns = {key: list(df.keys())[i-1] + "_flag"})

            df[list(df.keys())[i-1] + " status_flag"] = quality_flag

            df[list(df.keys())[i-1] + " integration_flag"] = area_height_flag

            scale[list(df.keys())[i-1]] = header[i-1][0]

            units[list(df.keys())[i-1]] = header[i-1][1]

            species.append(list(df.keys())[i-1])

    return df, species, units, scale


def gc_precisions_read(precisions_file):

    # Read precision species
    precision_species = list(pd.read_csv(precisions_file,
                                         skiprows=3,
                                         nrows = 1,
                                         header = None,
                                         sep=r"\s+").values[0][1:])

    # Read precisions
    precision = pd.read_csv(precisions_file,
                            skiprows=5,
                            header = None,
                            sep=r"\s+", dtype = str,
                            index_col = 0,
                            parse_dates = True,
                            date_parser = parser_YYMMDD)

    # Rename index column
    precision.index.names = ["index"]

    # Drop duplicates
    precision = precision.reset_index().drop_duplicates(subset='index').set_index('index')

    return precision, precision_species


def gc(site, instrument, network,
       input_directory = None,
       output_directory = None,
       date_range = None,
       version = None):
    """
    Process GC data per site and instrument
    Instruments can be:
        "GCMD": GC multi-detector (output will be labeled GC-FID or GC-ECD)
        "GCMS": GC ADS (output GC-ADS)
        "medusa": GC medusa (output GC-MEDUSA)

    Network is the network name for output file.
    """

    # Get site name
    site_gcwerks = params["GC"][site]["gcwerks_site_name"]
    # Get instrument name
    instrument_gcwerks = params["GC"]["instruments"][instrument]


    data_folder, output_folder = \
            get_directories(params["GC"]["directory"][instrument],
                            params["GC"]["directory_output"],
                            user_specified_input_directory = input_directory,
                            user_specified_output_directory = output_directory)
            
    search_strings = []
    for suffix in params["GC"]["instruments_suffix"][instrument]:
        # Search string
        search_string = join(data_folder,
                             site_gcwerks + \
                             instrument_gcwerks + \
                             suffix + ".??.C")
        search_strings.append(search_string)

        data_files = sorted(glob.glob(search_string))

        if len(data_files) > 0:
            break

    # Error if can't find files
    if len(data_files) == 0.:
        print("ERROR: can't find any files: " + \
              ",\r".join(search_strings))
        return None

    precision_files = [data_file[0:-2] + ".precisions.C"  for data_file in data_files]

    # List to hold lists to be converted into
    # Pandas dataframes
    dfs = []
    scale = {}
    units = {}

    # Start reading in data files here
    for fi, data_file in enumerate(data_files):

        print("Reading " + data_file)

        # Get observations
        df, species, units, scale = gc_data_read(data_file, scale = scale, units = units)

        # Get precision
        precision, precision_species = gc_precisions_read(precision_files[fi])

        # Merge precisions into dataframe
        for sp in species:
            precision_index = precision_species.index(sp)*2+1
            
            df[sp + " repeatability"] = precision[precision_index].\
                                            astype(float).\
                                            reindex_like(df, "pad")

        dfs.append(df)

    # Concatenate
    dfs = pd.concat(dfs).sort_index()

    # Apply timestamp correction, because GCwerks currently outputs
    #   the CENTRE of the sampling period
    dfs["new_time"] = dfs.index - pd.Timedelta(seconds = params["GC"]["sampling_period"][instrument]/2.0)
    
    dfs.set_index("new_time", inplace = True, drop = True)
    
    # Label time index
    dfs.index.name = "time"

    # Convert to xray dataset
    ds = xray.Dataset.from_dataframe(dfs)

    # Get species from scale dictionary
    species = list(scale.keys())

    inlets = params["GC"][site]["inlets"]

    # Process each species in file
    for sp in species:
        
        # These are the details of the owner of the site
        global_attributes = params["GC"][site.upper()]["global_attributes"]
        global_attributes["comment"] = params["GC"]["comment"][instrument]

        # Now go through each inlet (if required)
        # Here inlets are different heights
        for inleti, inlet in enumerate(inlets):

            # There is only one inlet, just use all data, and don't label inlet in filename
            if (inlet == "any") or (inlet == "air"):
                
                print("Processing %s, assuming single inlet..." %sp)
                
                ds_sp = ds[[sp, sp + " repeatability", sp + " status_flag",  sp + " integration_flag", "analysis_time", "Inlet"]]
                # No inlet label in file name
                inlet_label = None
            
            # If there are specific heights
            else:
                # Get dated inlet
                print("Processing " + sp + ", " + inlet + "...")
                # if inlet is in the format "date_YYYYMMDD_YYYYMMDD", split by date
                if inlet[0:4] == "date":
                    dates = inlet.split("_")[1:]
                    slice_dict = dict(time = slice(dates[0], dates[1]))
                    
                    ds_sliced = ds.loc[slice_dict]
                    ds_sp = ds_sliced[[sp,
                                       sp + " repeatability",
                                       sp + " status_flag",
                                       sp + " integration_flag",
#                                       "analysis_time",
                                       "Inlet"]]
                
                # Get specific height inlet
                else:
                    
                    # Use UNIX pattern matching to find matching inlets
                    # select_inlet is a list of True or False
                    select_inlet = [fnmatch.fnmatch(i, inlet) for i in ds.Inlet.values]
                    # now create a DataArray of True or False
                    select_ds = xray.DataArray(select_inlet, coords = [ds.time],
                                               dims = ["time"])
                    
                    # sub-set ds
                    ds_sp = ds.where(select_ds, drop = True)[[sp,
                                                              sp + " repeatability",
                                                              sp + " status_flag",
                                                              sp + " integration_flag",
#                                                              "analysis_time"
                                                              "Inlet"]]

                # re-label inlet if required
                if "inlet_label" in list(params["GC"][site].keys()):
                    inlet_label = params["GC"][site]["inlet_label"][inleti]
                else:
                   inlet_label = inlet

            if inlet_label == None:
                global_attributes["inlet_magl"] = params["GC"][site]["inlet_label"][inleti]
            else:
                global_attributes["inlet_magl"] = inlet_label
            
            # Record Inlets from the .C file, for the record
            # TODO: figure out why xarray raises an error at this line
            #   if "analysis time" column is included (commented out above)
            Inlets = set(ds_sp.where(ds_sp[sp + " status_flag"] == 0, drop = True).Inlet.values)
            global_attributes["inlet_gcwerks"] = ", ".join(Inlets)           
            # Now remove "Inlet" column from dataframe. Don't need it
            ds_sp = ds_sp.drop(["Inlet"])
    

            # Drop NaNs
            ds_sp = ds_sp.dropna("time")

            if len(ds_sp.time) == 0:

                print("... no data in file, skipping " + sp)

            else:

                # Sort out attributes
                ds_sp = attributes(ds_sp, sp, site.upper(),
                                   global_attributes = global_attributes,
                                   units = units[sp],
                                   scale = scale[sp],
                                   sampling_period = params["GC"]["sampling_period"][instrument],
                                   date_range = date_range)

                if len(ds_sp.time.values) == 0:
                    
                    # Then must have not passed date_range filter?
                    print(" ... no data in range")
                    # then do nothing

                else:
    
                    # Get instrument name for output
                    if sp.upper() in params["GC"]["instruments_out"][instrument]:
                        instrument_out = params["GC"]["instruments_out"][instrument][sp]
                    else:
                        instrument_out = params["GC"]["instruments_out"][instrument]["else"]
    
                    # Write file
                    nc_filename = output_filename(output_folder,
                                                  network,
                                                  instrument_out,
                                                  site.upper(),
                                                  ds_sp.time.to_pandas().index.to_pydatetime()[0],
                                                  ds_sp.species,
                                                  inlet = inlet_label,
                                                  version = version)

                    print("Writing... " + nc_filename)
                    ds_sp.to_netcdf(nc_filename)
                    print("... written.")



def crds_data_read(data_file):

    print("Reading " + data_file)

    # Read file header
    df_header = pd.read_csv(data_file,
                         skiprows=1,
                         nrows = 2,
                         header = None,
                         sep=r"\s+")

    header = []
    species = []

    # Create header list
    for i in df_header.columns:
        if df_header[i][0] != '-':
            header.append(df_header[i][0].upper() + \
                          crds_header_string_interpret[df_header[i][1]])
            if df_header[i][1] == "C":
                species.append(df_header[i][0].upper())
        else:
            header.append(df_header[i][1].upper())

    # Read data
    df = pd.read_csv(data_file,
                     skiprows=4,
                     header = None,
                     sep=r"\s+",
                     names = header,
                     dtype = {"DATE": str, "TIME": str})

    # Interpret time
    time = [dt(2000 + int(date[0:2]),
                      int(date[2:4]),
                      int(date[4:]),
                      int(time[0:2]),
                      int(time[2:4]),
                      int(time[4:])) \
            for date, time in zip(df["DATE"].values, df["TIME"].values)]
    df.index = time

    # Remove duplicate indices
    df = df.reset_index().drop_duplicates(subset='index').set_index('index')

    # Convert to Dataset
    df.index.name = "time"
    ds = xray.Dataset.from_dataframe(df.sort_index())

    return ds, species


def crds(site, network,
         input_directory = None,
         output_directory = None,
         date_range = None,
         version = None):
    """
    Process CRDS data

    site : Three letter site code
    network : Network string only for output
    """
    params_crds = params["CRDS"]

    site_string = params_crds[site]["gcwerks_site_name"]

    data_folder, output_folder = \
            get_directories(params_crds["directory"].replace("%site", site_string),
                            params["CRDS"]["directory_output"],
                            user_specified_input_directory = input_directory,
                            user_specified_output_directory = output_directory)

    # Search for species and inlets from file names
    data_file_search = join(data_folder, site.lower() + ".*.1minute.*.dat")
    data_files = glob.glob(data_file_search)
    inlets = [f.split(".")[-2] for f in data_files]

    for i, inlet in enumerate(inlets):

        # Create Pandas dataframe
        ds, species = crds_data_read(data_files[i])

        # Write netCDF file for each species
        for sp in species:

            # Species-specific dataset
            ds_sp = ds[[sp, sp + " variability", sp + " number_of_observations"]]
            
            ds_sp = ds_sp.dropna("time")

            global_attributes = params_crds[site]["global_attributes"]
            global_attributes["inlet_height_magl"] = float(inlet[0:-1])
            global_attributes["comment"] = params_crds["comment"]

            ds_sp = attributes(ds_sp, sp, site.upper(),
                               global_attributes = global_attributes,
                               scale = scales[sp],
                               sampling_period=60,
                               date_range = date_range)

            if len(ds_sp.time.values) == 0:
                
                # Then must have not passed date_range filter?
                print(" ... no data in range")
                # then do nothing

            else:

                # Write file
                nc_filename = output_filename(output_folder,
                                              network,
                                              "CRDS",
                                              site.upper(),
                                              ds_sp.time.to_pandas().index.to_pydatetime()[0],
                                              ds_sp.species,
                                              inlet = inlet,
                                              version = version)
                
                print("Writing " + nc_filename)
                ds_sp.to_netcdf(nc_filename)
                print("... written.")


def ale_gage(site, network):
    """
    Process Georgia Tech ALE or GAGE observations
    
    Args:
        site (str): ADR, RPB, ORG, SMO, CGO or MHD
        network (str): ALE or GAGE

    """
    
    import fortranformat as ff

    ale_directory = "/dagage2/agage/summary/git/ale_new/complete/"
    gage_directory = "/dagage2/agage/summary/git/gage_new/complete/"

    output_directory = "/data/shared/obs_2018/"

    site_translate = {"ADR": "adrigole",
                      "RPB": "barbados",
                      "ORG": "oregon",
                      "SMO": "samoa",
                      "CGO": "tasmania",
                      "MHD": "macehead"}

    if network == "ALE":
        data_directory = ale_directory
    elif network == "GAGE":
        data_directory = gage_directory
    else:
        print("Network needs to be ALE or GAGE")
        return None


    fnames = sorted(glob.glob(join(data_directory,
                                   site_translate[site] + "/" + site + "*.dap")))


    formatter = ff.FortranRecordReader('(F10.5, 2I4,I6, 2I4,I6,1X,10(F10.3,a1))')


    dfs = []
    for fname in fnames:

        print("Reading... " + fname)

        header = []

        with open(fname) as f:
            for i in range(6):
                header.append(f.readline())

            lines = f.readlines()

        scales = header[-3].split()
        units = header[-2].split()
        species = header[-1].split()

        dayi = species.index("DD")
        monthi = species.index("MM")
        yeari = species.index("YYYY")
        houri = species.index("hh")
        mini = species.index("min")

        data = []
        time = []

        for line in lines:
            data_line = formatter.read(line)

            if data_line[mini] < 60 and data_line[houri] < 24:
                data.append([d for d in data_line if d != " " and \
                                                     d != None and \
                                                     d != "P"])
                time.append(dt(data_line[yeari],
                               data_line[monthi],
                               data_line[dayi],
                               data_line[houri],
                               data_line[mini]))

        data = np.vstack(data)
        data = data[:, 7:]

        df = pd.DataFrame(data = data, columns = species[7:], index = time)
        df.replace(to_replace = 0., value=np.NaN, inplace = True)
        dfs.append(df)

    scales = scales[7:]
    units = units[7:]
    species = species[7:]

    df = pd.concat(dfs)

    # Write netCDF file for each species
    for si, sp in enumerate(species):

        # Remove duplicate indices
        df_sp = df[sp].reset_index().drop_duplicates(subset='index').set_index('index')

        # Convert to Dataset
        df_sp.index.name = "time"
        ds = xray.Dataset.from_dataframe(df_sp.sort_index())
        ds = ds.dropna("time")

        ds = attributes(ds, sp, site.upper(),
                       scale = scales[si],
                       sampling_period=60,
                       units = units[si])

        # Write file
        nc_filename = output_filename(output_directory,
                                      network,
                                      "GCECD",
                                      site.upper(),
                                      ds.time.to_pandas().index.to_pydatetime()[0],
                                      ds.species)
        print("Writing " + nc_filename)
        ds.to_netcdf(nc_filename)
        print("... written.")


def mhd_o3():

    channels = ["channel1", "channel0", "channel2"]
    base_directory = "/dagage2/agage/macehead-ozone/results/export/"

    df = []

    for channel in channels:

        files_channel = sorted(glob.glob(join(base_directory, channel, "*.csv")))

        for f in files_channel:

            df.append(pd.read_csv(f, sep=",",
                                  names = ["datetime",
                                           "ozone",
                                           "ozone_variability",
                                           "ozone_number_samples"],
                                  na_values = "NA",
                                  index_col = "datetime",
                                  parse_dates = ["datetime"]))
            df[-1].dropna(inplace = True)

    df = pd.concat(df)
    df.index.name = "index"
    df = df.reset_index().drop_duplicates(subset='index').set_index('index')
    df.sort_index(inplace = True)

    # Convert to Dataset
    df.index.name = "time"
    ds = xray.Dataset.from_dataframe(df)

    ds = attributes(ds,
                    "ozone",
                    "MHD",
                    scale = "SCALE",
                    sampling_period=60*60,
                    units = "ppb")

    # Write file
    nc_filename = output_filename("/dagage2/agage/metoffice/processed_observations_2018",
                                  "AURN",
                                  "thermo",
                                  "MHD",
                                  str(ds.time.to_pandas().index.to_pydatetime()[0].year),
                                  ds.species,
                                  site_params["MHD"]["height"][0])
    print("Writing " + nc_filename)
    ds.to_netcdf(nc_filename)
    print("... written.")


def data_freeze(version,
                end_date,
                input_directory = "/dagage2/agage/summary/gccompare-net/snapshot/current-frozendata/data-net/",
                output_directory = "/dagage2/agage/summary/gccompare-net/snapshot/current-frozendata/data-net/processed/"):

    date_range = ["19000101", end_date]

    # ICOS
    icos("MHD", network = "ICOS", input_directory = input_directory,
         output_directory = output_directory, version = version, date_range = date_range)

    # GAUGE CRDS data
    crds("HFD", "GAUGE", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)

    crds("BSD", "GAUGE", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)


    # GAUGE GC data
    gc("BSD", "GCMD", "GAUGE", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)
    gc("HFD", "GCMD", "GAUGE", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)

    # DECC CRDS data
    crds("TTA", "DECC", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)
    crds("RGL", "DECC", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)
    crds("TAC", "DECC", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)

    # DECC GC data
    gc("TAC", "GCMD", "DECC", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)
    gc("RGL", "GCMD", "DECC", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)

    # DECC Medusa
    gc("TAC", "medusa", "DECC", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)

    # AGAGE GC data
    gc("MHD", "GCMD", "AGAGE", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)

    # AGAGE GCMS data
    gc("MHD", "GCMS", "AGAGE", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)

    # AGAGE Medusa
    gc("MHD", "medusa", "AGAGE", input_directory = input_directory, output_directory = output_directory, version = version, date_range = date_range)




if __name__ == "__main__":

    # AGAGE Medusa
    gc("MHD", "medusa", "AGAGE")
    gc("CGO", "medusa", "AGAGE")
    gc("GSN", "medusa", "AGAGE")
    gc("SDZ", "medusa", "AGAGE")
    gc("THD", "medusa", "AGAGE")
    gc("RPB", "medusa", "AGAGE")
    gc("SMO", "medusa", "AGAGE")
    gc("SIO", "medusa", "AGAGE")
    gc("JFJ", "medusa", "AGAGE")
    gc("CMN", "medusa", "AGAGE")
    gc("ZEP", "medusa", "AGAGE")

    # AGAGE GC data
    gc("RPB", "GCMD", "AGAGE")
    gc("CGO", "GCMD", "AGAGE")
    gc("MHD", "GCMD", "AGAGE")
    gc("SMO", "GCMD", "AGAGE")
    gc("THD", "GCMD", "AGAGE")

    # AGAGE GCMS data
    gc("CGO", "GCMS", "AGAGE")
    gc("MHD", "GCMS", "AGAGE")
    gc("RPB", "GCMS", "AGAGE")
    gc("SMO", "GCMS", "AGAGE")
    gc("THD", "GCMS", "AGAGE")
    gc("JFJ", "GCMS", "AGAGE")
    gc("CMN", "GCMS", "AGAGE")
    gc("ZEP", "GCMS", "AGAGE")

    # AGAGE CRDS data
    crds("RPB", "AGAGE")

    # ICOS
#    icos("TTA", network = "DECC")
    icos("MHD", network = "ICOS")

    # GAUGE CRDS data
    crds("HFD", "DECC")
    crds("BSD", "DECC")

    # GAUGE GC data
    gc("BSD", "GCMD", "DECC")
    gc("HFD", "GCMD", "DECC")

    # DECC CRDS data
    crds("TTA", "DECC")
    crds("RGL", "DECC")
    crds("TAC", "DECC")

    # DECC GC data
    gc("TAC", "GCMD", "DECC")
    gc("RGL", "GCMD", "DECC")

    # DECC Medusa
    gc("TAC", "medusa", "DECC")

    cleanup("CGO")
    cleanup("MHD")
    cleanup("RPB")
    cleanup("THD")
    cleanup("SMO")
    cleanup("GSN")
    cleanup("SDZ")
    cleanup("JFJ")
    cleanup("CMN")
    cleanup("ZEP")

    cleanup("TAC")
    cleanup("RGL")
    cleanup("HFD")
    cleanup("BSD")
    cleanup("TTA")
    

#    # Copy files
#    networks = ["AGAGE", "GAUGE", "DECC", "ICOS"]
#    src_dir = "/dagage2/agage/metoffice/processed_observations_2018"
#    dest_dir = "/data/shared/obs_2018"
#
#    for network in networks:
#        files = glob.glob(join(src_dir, network, "*.nc"))
#        for f in files:
#            print("Copying %s..." % (split(f)[-1]))
#            shutil.copy(f, join(dest_dir, network))
