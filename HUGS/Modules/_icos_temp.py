# ICOS
########################################################

def icos_data_read(data_file, species):

    print("Reading " + data_file)

    # Find out how many header lines there are
    nheader = 0
    with open(data_file, "r") as f:
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

    def find_species_inlet_model(filenames):
        out = []
        for f in filenames:
            f_elements = f.split(".")
            if len(f_elements) == 6:
                out.append((f_elements[1],
                            f_elements[4],
                            "picarro" + f_elements[3].upper()))
            else:
                out.append((f_elements[1],
                            f_elements[3],
                            "picarro"))
        return(out)

    # Get directories and site strings
    params_icos = params["ICOS"]
    site_string = params_icos[site]["gcwerks_site_name"]

    data_folder, output_folder = \
            get_directories(params_icos["directory"].replace("%site", site_string),
                            params_icos["directory_output"],
                            user_specified_input_directory = input_directory,
                            user_specified_output_directory = output_directory)

    # Search for species, inlets and model from file names
    data_file_search = join(data_folder, site.lower() + ".*.1minute.*.dat")
    data_files = glob.glob(data_file_search)
    data_file_names = [split(f)[1] for f in data_files]
    species_inlet_model = find_species_inlet_model(data_file_names)

    inlets = set([i for (s, i, m) in species_inlet_model])

    for i, (species, inlet, model) in enumerate(species_inlet_model):

        if stat(data_files[i]).st_size > 0:

            # Create Pandas dataframe
            ds = icos_data_read(data_files[i], species.upper())

            # Sort out attributes
            global_attributes = params_icos[site.upper()]["global_attributes"]
            global_attributes["inlet_height_magl"] = float(params_icos[site]["inlet_rename"][inlet][:-1])

            ds = attributes(ds,
                            species.upper(),
                            site.upper(),
                            network = network,
                            global_attributes = global_attributes,
                            sampling_period = 60,
                            date_range = date_range)

            if len(ds.time.values) == 0:

                # Then must have not passed date_range filter?
                print(" ... no data in range")
                # then do nothing

            else:

                inlet_label = params_icos[site]["inlet_rename"][inlet]

                # Write file
                nc_filename = output_filename(output_folder,
                                              network,
                                              model,
                                              site.upper(),
                                              ds.time.to_pandas().index.to_pydatetime()[0],
                                              ds.species,
                                              inlet = [None, inlet_label][len(inlets) > 1],
                                              version = version)

                ds.to_netcdf(nc_filename)

                print("Written " + nc_filename)

        else:
            print("Skipping empty file: %s" % data_files[i])