# import hugs
import os
import sys
import json
import xarray
import glob
import datetime
from data_processing import utils
import pandas as pd

def read_data_file(data_file):
    """ This function reads in a single CRDS datafile
        and creates a Pandas dataframe to store the data

        Args:
            data_file (str): Path of file to read
        Returns:
            tuple (xarray.Dataset, list): Dataset of processed data,
            list of species if found
    
    """

    # Lists to hold the header and species data
    header = []
    species = []

    df_header = pd.read_csv(data_file, skiprows=1, nrows=2,
                            header=None, sep=r"\s+")

    # Translate header strings
    crds_header_string_interpret = {"C": "",
                                    "stdev": " variability",
                                    "N": " number_of_observations"}
    
    # Create header list
    for i in df_header.columns:
        if df_header[i][0] != '-':
            header.append(df_header[i][0].upper() +
                        crds_header_string_interpret[df_header[i][1]])

            if df_header[i][1] == "C":
                species.append(df_header[i][0].upper())
        else:
            header.append(df_header[i][1].upper())

    # Read in the data
    df = pd.read_csv(data_file, skiprows=4, header=None, sep=r"\s+", 
                    names=header, dtype={"DATE": str, "TIME": str})
                    

    # TODO - GJ - I don't like this - tidy it up somehow?
    # Interpret time
    time = [datetime.datetime(2000 + int(date[0:2]), int(date[2:4]), 
            int(date[4:]), int(time[0:2]), int(time[2:4]), int(time[4:]))
            for date, time in zip(df["DATE"].values, df["TIME"].values)]
    
    df.index = time

    df = df.reset_index().drop_duplicates(subset='index').set_index('index')

    df.index.name = "time"
    ds = xarray.Dataset.from_dataframe(df.sort_index())

    return ds, species


def search_data_files(data_folder, site, search_string):
    """ Searches for data files in data_folder for the site
        using search_string

        Args:
            data_folder (str): Data folder to search
            site (str): Site to use
            search_string (str): Search string to find files
            Example: ".*.1minute.*.dat"
        Returns
            list: List of matching files in data_folder

    """

    file_search = os.path.join(data_folder, site.lower() + search_string)

    data_files = glob.glob(file_search)

    return data_files
    

def find_inlets(data_files):
    """ Creates a list of inlets from the current data files

        Args:
            data_files (list): A list of data files found
        Returns:
            list: List of inlets
    """
    # Find the inlets
    return [f.split(".")[-2] for f in data_files]


def load_from_JSON(path, filename):
    """ Reads in from a JSON file

        Args:
            filename (str): Name of file to load
            path (str): Location of file
        Returns:
            dict: Dictionary created from JSON data

    """

    params_file = os.path.join(os.path.dirname(__file__),
                               path, filename)

    # Load the JSON file in as a dictionary
    with open(params_file, "r") as f:
        d = json.load(f)

    return d


def process_data(data_files, inlets, site):
    """ Process the list of data files in data_files
        and create a list of Pandas dataframes

        Args:
            data_files (list): List of found data files
            inlets (list): List of found inlets
        Returns:
            list: A list of xarray.Datasets

    """
    # Read in the parameters
    gcwerks_param_file = "process_gcwerks_parameters.json"
    metadata_folder = "metadata"

    params = load_from_JSON(path=metadata_folder, filename=gcwerks_param_file)

    params = params["CRDS"]

    # GJ - comments below come from original file
    # Default calibration scales
    # TODO: Remove this? seems dangerous
    scales = {"CO2": "NOAA-2007",
              "CH4": "NOAA-2004A",
              "N2O": "SIO-98",
              "CO": "Unknown"}

    # A dataset for each species
    species_datasets = []

    for i, inlet in enumerate(inlets):

        # Create the pandas dataframe
        ds, species = read_data_file(data_files[i])

        # Create a dataset for each species
        for sp in species:
            print("Processing species : " + sp)
            # Species specific dataset
            species_ds = ds[[sp, sp + " variability", sp + " number_of_observations"]]
            species_ds = species_ds.dropna("time")

            global_attributes = params[site]["global_attributes"]
            global_attributes["inlet_height_magl"] = float(inlet[0:-1])
            global_attributes["comment"] = params["comment"]

            species_ds = utils.attributes(species_ds, sp, site.upper(),
                            global_attributes=global_attributes,
                            scale=scales[sp],
                            sampling_period=60,
                            date_range=None)

            # TODO - sort this out
            if len(species_ds.time.values) == 0:
                print("Error - no data in range")
                raise
            else:
                species_datasets.append(species_ds)

    return species_datasets

def parse_filenames(file_list):
    """ Returns a prettified list of filenames from the path

        Not sure if this is necessarily needed but may be handy for
        processing and feedback to the user

        Include some kind of MD5 for saving and recording 
        of already uploaded data?
    """

    pretty_filenames = [x.split("/")[-1] for x in file_list]

    return pretty_filenames


def process_raw_data(folder_path, site, search_string):
    """ Process data in folder_path into
        xarray.Datasets 

        Args:
            Example: ".*.1minute.*.dat"
            folder_path (str): Path for folder containing data
            site (str): Site of data
            Example: ".*.1minute.*.dat"
            search_string (str): Search string to find files
            Example: ".*.1minute.*.dat"
        Returns:
            list: List of xarray.Datasets for each species found in
            the folder
    """

    file_list = search_data_files(data_folder=folder_path, site=site,
                                           search_string=search_string)
    
    print("\nFiles found : ", parse_filenames(file_list))

    inlets = find_inlets(file_list)

    print("\nInlets found : ", inlets)

    species_data = process_data(data_files=file_list, 
                                        inlets=inlets, site="BSD")

    return species_data



def write_files(data_list, output_folder):
    """ Writes the data in data_list to file

        Args:
            data_list (list): A list of xarray datasets
            output_folder (str): Folder to write NetCDFs
        Returns:
            None
        Raises:
            TypeError: If the list contains an object that
            is not an xarray.Datset

    """

    network = "test_network"
    site = "test"

    for d in data_list:
        # Create the filename
        nc_filename = utils.output_filename(output_folder, network, "CRDS", site.upper(),
                                        d.time.to_pandas().index.to_pydatetime()[0],
                                        d.species, inlet=inlet, version=version)
        # Write to file
        d.to_netcdf(nc_filename)
