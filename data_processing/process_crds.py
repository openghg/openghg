import hugs
import os
import sys
import json
import xarray
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
        df = pd.read_csv(data_file
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
                        int(time[4:]))
                for date, time in zip(df["DATE"].values, df["TIME"].values)]
        
        df.index = time

        df = df.reset_index().drop_duplicates(subset='index').set_index('index')

        df.index.name = "time"
        ds = xarray.Dataset.from_dataframe(df.sort_index())

        return ds, species


    def read_data(self, data_folder):
        """ This function collects raw 
            Args:
                data_folder (str): The directory holding data files
            Returns:
                

        """
        # TODO - get this properly
        site = "yahyah"
        params_crds = self.params["CRDS"]
        site_string = params_crds[site]["gcwerks_site_name"]

        # String type to search folder for
        data_file_search = join(data_folder, site.lower() + ".*.1minute.*.dat")
        # Glob the files 
        data_files = glob.glob(data_file_search)
        inlets = [f.split(".")[-2] for f in data_files]

        for i, inlet in enumerate(inlets):
            # 

        






        # Loop over the inlets and create Pandas dataframes

        




