import hugs
import os
import sys
import json
import xarray
import glob
import pandas as np

class proc_CRDS:
    def __init__(self):
        # Load in parameters data
        self.metadata_folder = "metadata"
        self.gcwerks_param_file = "process_gcwerks_parameters.json"
        self.site_info_filename = "acrg_site_info.json"

        params_file = os.path.join(self.metadata_folder, self.gcwerks_param_file)
        site_info_file = os.path.join(self.metadata_folder, self.site_info_file)
        
        try:
            with open(params_file, "r") as f:
                self.params = json.load(f)
        except FileNotFoundError as err:
            print("Cannot open ", self.params_file, err)

        try:   
            with open(site_info_file, "r") as f:
                self.site_params = json.load(f)
        except FileNotFoundError as err:
            print("Cannot open ", self.site_params, err)

    # def crds_data_read(data_file) 
    # From process_gcwerks.py
    def read_data_file(self, data_file):
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
        df_header = pandas.read_csv(data_file, skiprows=1, nrows=2, sep=r"\s+")

        # Translate header strings
        crds_header_string_interpret = {"C": "",
                                        "stdev": " variability",
                                        "N": " number_of_observations"}
        
        for x in df_header.columns:
            if df_header[i][0] != "-":
                header.append(df_header[i][0].upper() + crds_header_string_interpret[df_header[i][1]])
                if df_header[i][1] == "C":
                    species.append(df_header[i][0].upper())
        else:
            header.append(df_header[i][1].upper())

        # Read in the data
        df = pd.read_csv(data_file, skiprows=4, header = None, sep=r"\s+", 
                        names = header, dtype = {"DATE": str, "TIME": str})
                        

        # TODO - GJ - I don't like this - tidy it up somehow?
        # Interpret time
        time = [dt(2000 + int(date[0:2]), int(date[2:4]), int(date[4:]), int(time[0:2]), int(time[2:4]), int(time[4:]))

                for date, time in zip(df["DATE"].values, df["TIME"].values)]
        
        df.index = time

        df = df.reset_index().drop_duplicates(subset='index').set_index('index')

        df.index.name = "time"
        ds = xarray.Dataset.from_dataframe(df.sort_index())

        return ds, species


    def search_data_files(self, data_folder, site, search_string):
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

    def populate_data_list(self, data_folder, site, search_string):
        """ This function populates a list of data files to use and
            a list of inlets

            Args:
                data_folder (str): The directory holding data files
                search_string (str): Search string to find files
                Example: ".*.1minute.*.dat"
            Returns:
                None           
        """

        params_crds = self.params["CRDS"]

        site_string = params_crds[site]["gcwerks_site_name"]

        # 2019-04-24
        # I'll get rid of this for now, I don't like this being hard coded in
        # data_dir = params_crds["directory"].replace("%site, site_string")

        # search_string = ".*.1minute.*.dat"
        self.data_files = self.search_data_files(data_folder, site, search_string)

        # Find the inlets
        self.inlets = [f.split(".")[-2] for f in data_files]

    def process_data(self):
        """ Process the list of data files in data_files
            and create a list of Pandas dataframes

            Returns:
                None
        """
        # A dataset for each species
        species_datasets = []

        for i, inlet in enumerate(self.inlets):

            # Create the pandas dataframe
            ds, species = self.read_data_file(data_file[i])

            # Write a NetCDF file for each species
            for sp in species:
                # Species specific dataset
                species_ds = ds[[sp, sp + " variability", sp + " number_of_observations"]]
                species_ds = species_ds.dropna("time")

            global_attributes = params_crds[site]["global_attributes"]
            global_attributes["inlet_height_magl"] = float(inlet[0:-1])
            global_attributes["comment"] = params_crds["comment"]

            species_ds = attributes(species_ds, sp, site.upper(),
                               global_attributes=global_attributes,
                               scale=scales[sp],
                               sampling_period=60,
                               date_range=date_range)

            
            if len(species_ds.time.values) == 0:
                # Then must have not passed date_range filter?
                print(" ... no data in range")
                # then do nothing
            else:
                species_datasets.append(species_ds)

        
        return species_datasets

        
    def write_files(self, data_list, output_folder):
        """ Writes the data in data_list to file

            Args:
                data_list (list): A list of xarray datasets
                output_folder (str): Folder to write NetCDFs
            Returns:
                None
            Raises:


                            
        """
        for d in data_list:
            if not isinstance(d, xarray.Dataset):
                raise TypeError("Passed list must contain only xarray.Dataset objects")

        

        # Write file
        nc_filename = output_filename(output_folder,
                                        network,
                                        "CRDS",
                                        site.upper(),
                                        ds_sp.time.to_pandas().index.to_pydatetime()[
                                            0],
                                        ds_sp.species,
                                        inlet=inlet,
                                        version=version)

        print("Writing " + nc_filename)
        ds_sp.to_netcdf(nc_filename)
        print("... written.")


            
                

        






        # Loop over the inlets and create Pandas dataframes

        




