# GC FUNCTIONS
###############################################################

def gc_data_read(dotC_file, scale = {}, units = {}):

    species = []

    # Read header
    header = pd.read_csv(dotC_file, skiprows=2,nrows=2,header = None,sep=r"\s+")

    # Read data
    df = pd.read_csv(dotC_file, skiprows=4, sep=r"\s+")

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
    dfs["new_time"] = dfs.index - \
            pd.Timedelta(seconds = params["GC"]["sampling_period"][instrument]/2.)
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

        global_attributes = params["GC"][site.upper()]["global_attributes"]
        global_attributes["comment"] = params["GC"]["comment"][instrument]

        # Now go through each inlet (if required)
        for inleti, inlet in enumerate(inlets):

            # There is only one inlet, just use all data, and don't label inlet in filename
            if (inlet == "any") or (inlet == "air"):
                
                print("Processing %s, assuming single inlet..." %sp)
                
                ds_sp = ds[[sp,
                            sp + " repeatability",
                            sp + " status_flag",
                            sp + " integration_flag",
#                            "analysis_time",
                            "Inlet"]]
                
                # No inlet label in file name
                inlet_label = None
                
            else:
                # Get specific inlet
                
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
