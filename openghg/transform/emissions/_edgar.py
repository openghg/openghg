import re
import numpy as np
from numpy import ndarray
import xarray as xr
from pathlib import Path
import zipfile
from zipfile import ZipFile
from typing import Dict, Tuple, Optional, Union, cast


ArrayType = Optional[Union[ndarray, xr.DataArray]]


def parse_edgar(
    datapath: Path,
    date: str,
    species: Optional[str] = None,
    domain: Optional[str] = None,
    lat_out: ArrayType = None,
    lon_out: ArrayType = None,
    # sector: Optional[str] = None,
    # period: Optional[Union[str, tuple]] = None,
    edgar_version: Optional[str] = None,
) -> Dict:
    """
    Read and parse input EDGAR data.
    Notes: Only accepts annual 2D grid maps in netcdf (.nc) format for now.
           Does not accept monthly data yet.

    EDGAR data is global on a 0.1 x 0.1 grid. This function allows products
    to be created for a given year which cover specific regions (and matches
    to the OpenGHG data schema, including units and coordinate names).

    Region information can be specified as follows:
     - To use a pre-defined domain use the domain keyword only.
     - To define a new domain use the domain, lat_out, lon_out keywords
     - If no domain or lat_out, lon_out data is supplied, the global EDGAR
    data will be added labelled as "globaledgar" domain.

    Pre-exisiting domains are defined within the 'domain_info.json' file.

    Metadata will also be added to the stored data including:
     - "domain": domain (e.g. "europe") OR "globaledgar"
     - "source": "anthro" (for "TOTAL"), source name from file otherwise
     - "database": "EDGAR"
     - "database_version": edgar_version (e.g. "v60", "v50", "v432")

    Args:
        datapath: Path to data folder or zip archive for EDGAR data
        date: Year to extract. Expect a string of the form "YYYY"
        species: Species name being extracted
        domain: Domain name for new or pre-existing domain
        lat_out: Latitude values for new domain
        lon_out: Longitude values for new domain
        edgar_version: EDGAR version in file. Will be inferred otherwise.

    Returns:
        dict: Dictionary of data

    TODO: Add monthly parsing and sector stacking options
    """
    import tempfile
    from openghg.util import synonyms, molar_mass, timestamp_now, clean_string, convert_longitude
    from openghg.store import infer_date_range
    from openghg.standardise.meta import define_species_label, assign_flux_attributes

    # Currently based on acrg.name.emissions_helperfuncs.getedgarannualtotals()
    # Additional edgar functions which could be incorporated.
    # - getedgarv5annualsectors
    # - getedgarv432annualsectors
    # - (monthly sectors?)

    # TODO: Work out how to select frequency
    # - could try and relate to period e.g. "monthly" versus "yearly" etc.
    period = None

    raw_edgar_domain = "globaledgar"

    lat_out, lon_out = _check_lat_lon(domain, lat_out, lon_out)

    if domain is None:
        domain = raw_edgar_domain

    # TODO: Add check for period? Only monthly or yearly (or equivalent inputs)

    # Check if input is a zip file
    if zipfile.is_zipfile(datapath):
        zipped = True
        zip_folder = zipfile.ZipFile(datapath)
    else:
        zipped = False

    known_version = _edgar_known_versions()

    # Check readme file for edgar version (if not specified)
    if zipped and edgar_version is None:
        edgar_version = _check_readme_version(zippath=zip_folder)
    elif edgar_version is None:
        edgar_version = _check_readme_version(datapath=datapath)

    # Extract list of data files
    if zipped:
        zip_filelist = zip_folder.infolist()
        # folder_filelist = list(zip_folder.namelist())
        folder_filelist = [Path(filename.filename) for filename in zip_filelist]
    else:
        folder_filelist = list(datapath.glob("*"))

    # Extract netcdf files (only, for now) - ".txt" is also an option (not implemented)
    suffix = ".nc"
    data_files = [file for file in folder_filelist if file.suffix == suffix]

    if not data_files:
        raise ValueError("Expect EDGAR '.nc' files." f"No suitable files found within datapath: {datapath}")

    for file in data_files:
        try:
            db_info = _extract_file_info(file)
        except ValueError:
            db_info = {}
            continue

    # Extract species from filename if not specified
    try:
        species_from_file: Optional[str] = db_info["species"]
    except KeyError:
        species_from_file = None

    if species is None:
        species = species_from_file

    # Check synonyms and compare against filename value
    if species is not None:
        species_label = define_species_label(species)[0]
        # species_label = synonyms(species).lower()
    else:
        raise ValueError("Unable to retrieve species from database filenames." " Please specify")

    if species_from_file is not None and species_label != synonyms(species_from_file):
        print(
            "WARNING: Input species does not match species extracted from",
            " database filenames. Please check.",
        )

    # If version not yet found, extract version from file naming scheme
    if edgar_version is None:
        possible_version = db_info["version"]
        if possible_version in known_version:
            edgar_version = possible_version

    if edgar_version not in known_version:
        raise ValueError(f"Unable to infer EDGAR version ({edgar_version})." " Please pass as an argument")

    # TODO: May want to split out into a separate function, so we can use this for
    # - yearly - "v6.0_CH4_2015_TOTALS.0.1x0.1.nc"
    # - sectoral - "v6.0_CH4_2015_ENE.0.1x0.1.nc"
    # - monthly sectoral - "v6.0_CH4_2015_1_ENE.0.1x0.1.nc", "v6.0_CH4_2015_2_ENE.0.1x0.1.nc", ...

    if len(date) == 4:
        year = int(date)
    else:
        raise ValueError(f"Do no accept date which does not represent a year yet: {date}")

    files_by_year = {}
    for file in data_files:
        try:
            file_info = _extract_file_info(file)
        except ValueError:
            continue

        # Check if data is actually monthly "...2015_1" etc. - can't parse yet
        if "month" in file_info:
            raise NotImplementedError("Unable to parse monthly EDGAR data at present.")

        year_from_file = file_info["year"]

        files_by_year[year_from_file] = file

        if year_from_file == year:
            edgar_file = file
            edgar_file_info = file_info
            break
    else:
        all_years = list(files_by_year.keys())
        all_years.sort()
        start_year, end_year = all_years[0], all_years[-1]
        if year < start_year:
            raise ValueError(
                f"EDGAR {edgar_version} range: {start_year}-{end_year}." f" {year} is before this period."
            )
        elif year > end_year:
            print(f"Using last available year from EDGAR {edgar_version} range:" f"{start_year}-{end_year}.")
            edgar_file = files_by_year[end_year]
            edgar_file_info = _extract_file_info(edgar_file)

    # For a zipped archive need to unzip the netcdf file and place in a
    # temporary directory.
    if zipped:
        temp_extract_folder = tempfile.TemporaryDirectory()

        for zipinfo in zip_filelist:
            if zipinfo.filename == edgar_file.name:
                zip_folder.extract(zipinfo, path=temp_extract_folder.name)
                edgar_file = temp_extract_folder.name / edgar_file
                break

    # Dimension - (lat, lon) - no time dimension
    # time is not included in the file just in the filename *sigh*!

    # v432_CH4_1978.0.1x0.1.nc (or .zip)
    # v50_CH4_1978.0.1x0.1.nc (or .zip)
    # v6.0_CH4_1978_TOTALS.0.1x0.1.nc

    # v50_CO2_excl_short-cycle_org_C_1978.0.1x0.1.nc (or .zip)
    # v50_CO2_org_short-cycle_C_1978.0.1x0.1.nc (or .zip)
    # v50_N2O_1978.0.1x0.1.zip (or .zip)

    with xr.open_dataset(edgar_file) as temp:
        edgar_ds = temp

    # Expected name e.g. "emi_ch4", "emi_co2"
    name = f"emi_{species_label}"

    # For reference, from "_readme.html" from v6.0 data:
    # 'Yearly Emissions gridmaps in ton substance / 0.1degree x 0.1degree / year
    #  for the .txt files with longitude and latitude coordinates referring to
    #  the low-left corner of each grid-cell.'
    # 'Monthly Emissions gridmaps in ton substance / 0.1degree x 0.1degree / month
    #  for the .txt files with longitude and latitude coordinates referring to
    #  the low-left corner of each grid-cell.'
    # 'Emissions gridmaps in kg substance /m2 /s for the .nc files with longitude
    # and latitude coordinates referring to the cell center of each grid-cell.'

    # Convert from kg/m2/s to mol/m2/s
    species_molar_mass = molar_mass(species_label)
    kg_to_g = 1e3

    flux_da = edgar_ds[name]
    flux_values = flux_da.values * kg_to_g / species_molar_mass
    units = "mol/m2/s"

    lat_name = "lat"
    lon_name = "lon"
    try:
        lat_in = edgar_ds[lat_name].values
        lon_in = edgar_ds[lon_name].values
    except KeyError:
        raise ValueError(
            f"Could not find '{lat_name}' or '{lon_name}' in EDGAR file.\n"
            " Please check this is a 2D grid map."
        )

    # Check range of longitude values and convert to -180 - +180
    lon_in, ordinds = convert_longitude(lon_in, return_index=True)
    flux_values = flux_values[:, ordinds]

    if lat_out is not None and lon_out is not None:
        # Will produce import error if xesmf has not been installed.
        from openghg.transform import regrid_uniform_cc

        # regrid2d() used within acrg code for equivalent regrid function
        # but switched to using xesmf (rather than iris) here instead.
        flux_values = regrid_uniform_cc(flux_values, lat_out, lon_out, lat_in, lon_in)
    else:
        lat_out = lat_in
        lon_out = lon_in

    edgar_attrs = edgar_ds.attrs

    # After the data has been extracted and used from the unzipped netcdf
    # file clean up and remove temporary directory and file.
    if zipped:
        temp_extract_folder.cleanup()

    # Check for "time" dimension and add if missing.
    flux_ndim = flux_values.ndim
    time_name = "time"
    if time_name in flux_da:
        time = flux_da[time_name].values
    elif time_name not in flux_da and flux_ndim == 2:
        time = np.array([f"{year}-01-01"], dtype="datetime64[ns]")
        flux = flux_values[np.newaxis, ...]
    elif flux_ndim != 3:
        raise ValueError(f"Expected '{name}' to contain 2 or 3 dimensions. Actually: {flux_ndim}")

    dims = ("time", "lat", "lon")

    em_data = xr.Dataset(
        {"flux": (dims, flux)}, coords={"time": time, "lat": lat_out, "lon": lon_out}, attrs=edgar_attrs
    )

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

    source_from_file = edgar_file_info["source"]
    if source_from_file in ("TOTALS", ""):
        source = "anthro"
    elif species_label == "co2" and "TOTALS" in source_from_file:
        co2_source = "_".join(source_from_file.split("_")[:-1])
        source = clean_string(f"{co2_source}_anthro")
    else:
        source = clean_string(source_from_file)
    database = "EDGAR"
    database_version = clean_string(edgar_version)

    metadata = {}
    metadata.update(attrs)

    metadata["species"] = species_label
    metadata["domain"] = domain
    metadata["source"] = source
    metadata["date"] = date
    metadata["database"] = database
    metadata["database_version"] = database_version
    metadata["author"] = author_name
    metadata["processed"] = str(timestamp_now())

    attrs = {"author": metadata["author"], "processed": metadata["processed"]}

    # Infer the date range associated with the flux data
    em_time = em_data.time
    start_date, end_date, period_str = infer_date_range(em_time, filepath=edgar_file.name, period=period)

    prior_info_dict = {
        "EDGAR": {
            "version": f"EDGAR {edgar_version}",
            "filename": edgar_file.name,
            "raw_resolution": "0.1 degrees x 0.1 degrees",
            "reference": edgar_ds.attrs["source"],
        }
    }

    metadata["start_date"] = str(start_date)
    metadata["end_date"] = str(end_date)

    metadata["min_longitude"] = round(float(em_data["lon"].min()), 5)
    metadata["max_longitude"] = round(float(em_data["lon"].max()), 5)
    metadata["min_latitude"] = round(float(em_data["lat"].min()), 5)
    metadata["max_latitude"] = round(float(em_data["lat"].max()), 5)

    metadata["time_resolution"] = "standard"
    metadata["time_period"] = period_str

    key = "_".join((species_label, source, domain, date))

    emissions_data: Dict[str, dict] = {}
    emissions_data[key] = {}
    emissions_data[key]["data"] = em_data
    emissions_data[key]["metadata"] = metadata
    emissions_data[key]["attributes"] = attrs

    emissions_data = assign_flux_attributes(emissions_data, units=units, prior_info_dict=prior_info_dict)

    return emissions_data


def _check_lat_lon(
    domain: Optional[str] = None, lat_out: ArrayType = None, lon_out: ArrayType = None
) -> Tuple[Optional[ndarray], Optional[ndarray]]:
    """
    Define and check latitude and longitude values for a domain.

    The domain can be used in one of two ways:
        1. To specify a pre-exisiting lat, lon extent which can be extracted
        2. To supply a name for a new lat, lon extent which must be specified

    For case 1, only domain needs to be specified (lat_out and lon_out can
    be specified but they must already exactly match the domain definition).
    The details will be extracted from 'domain_info.json'.

    For case 2, the domain, lat_out and lon_out must all be specified.

    If none of these values are specified, (None, None) will be returned. This
    is valid behaviour.

    Args:
        domain: Domain name for a pre-existing domain or a new domain
        lat_out: Latitude values (only needed if domain is new)
        lon_out: Longitude values (only needed if domain is new)

    Returns:
        ndarray, ndarray: Latitude and longitude arrays
        None, None: if all inputs are None, a tuple of Nones will be returned.
    """
    from openghg.util import find_domain, convert_longitude

    if lat_out is not None or lon_out is not None:
        if domain is None:
            raise ValueError(
                "Please specify new 'domain' name if selecting new" " latitude, longitude values"
            )

    if isinstance(lat_out, xr.DataArray):
        lat_out = cast(ndarray, lat_out.values)

    if isinstance(lon_out, xr.DataArray):
        lon_out = cast(ndarray, lon_out.values)

    if domain is not None:
        # If domain is specified, attempt to extract lat/lon values from
        # pre-defined definitions.
        try:
            lat_domain, lon_domain = find_domain(domain)
        except ValueError:
            # If domain cannot be found and lat, lon values have not been
            # defined raise an error.
            if lat_out is None or lon_out is None:
                raise ValueError("To create new domain please input" " 'lat_out' and 'lon_out' values.")
        else:
            # Check domain latitude and longitude values against any
            # lat_out and lon_out values specified to check they match.
            if lat_out is not None:
                if not np.array_equal(lat_domain, lat_out):
                    raise ValueError(
                        "Latitude values should not be specified"
                        f" when using pre-defined domain {domain}"
                        " (values don't match)."
                    )
            else:
                lat_out = lat_domain

            if lon_out is not None:
                if not np.array_equal(lon_domain, lon_out):
                    raise ValueError(
                        "Longitude values should not be specified"
                        f" when using pre-defined domain {domain}"
                        " (values don't match)."
                    )
            else:
                lon_out = lon_domain

            if lon_out.max() > 180 or lon_out.min() < -180:
                raise ValueError(
                    "Invalid domain definition."
                    " Expected longitude in range: -180 - 180."
                    f"Current longitude: {lon_out.min()} - {lon_out.max()}"
                )

    if lon_out is not None and (lon_out.max() > 180 or lon_out.min() < -180):
        print("Converting longitude to stay within -180 - 180 bounds")
        lon_converted = convert_longitude(lon_out)
        lon_out = cast(Optional[ndarray], lon_converted)

    return lat_out, lon_out


def _edgar_known_versions() -> list:
    """Define list of known versions for the EDGAR database"""
    known_version = ["v432", "v50", "v6.0"]
    return known_version


def _check_readme_version(
    datapath: Optional[Path] = None, zippath: Optional[ZipFile] = None
) -> Optional[str]:
    """
    Attempts to extract the edgar version from the associated "_readme.html"
    file, if present.

    Args:
        datapath : Path to the folder containing the downloaded EDGAR files
        zippath: Path to zipped archive file (direct from EDGAR)

    Returns:
        str : edgar version if found (None otherwise)
    """

    # Work out version if possible from readme
    # All database versions so far may contain "_readme.html" file
    # "v6.0"
    #  - "TOTALS_nc.zip" is what is downloaded from website
    #  - "_readme.html" title line: "<title>EDGAR v6.0_GHG (2021)</title>"
    # "v5.0"
    #  - "v50_CH4_1970_2015.zip" can be downloaded
    #  - "_readme.html" title line: "<title>EDGAR v5.0 (2019)</title>"
    # "v4.3.2"
    #  - "v432_CH4_1970_2012.zip" can be downloaded
    #  - "_readme.html" title line: "<title>EDGAR v4.3.2 (2017)</title>"

    # Check for readme html file and, if present, extract version
    readme_filename = "_readme.html"
    if zippath is not None:
        try:
            # Cast extracted bytes to a str object
            readme_data: Optional[str] = str(zippath.read(readme_filename))
        except ValueError:
            readme_data = None
    elif datapath is not None:
        readme_filepath = datapath.joinpath(readme_filename)
        if readme_filepath.exists():
            readme_data = readme_filepath.read_text()
        else:
            readme_data = None
    else:
        raise ValueError("One of datapath or zippath must be specified.")

    if readme_data is not None:
        try:
            # Ignoring types as issues caught by try-except statement
            # Find and extract title line from html file
            title_line = re.search("<title.*?>(.+?)</title>", readme_data).group()  # type: ignore
            # Extract version e.g. "v6.0" or "v4.3.2"
            edgar_version = re.search(r"v\d[.]\d[.]?\d*", title_line).group()  # type: ignore
        except ValueError:
            pass
        else:
            # Check against known versions and remove '.' if these don't match.
            known_version = _edgar_known_versions()
            if edgar_version not in known_version:
                edgar_version = edgar_version.replace(".", "")
    else:
        edgar_version = None

    return edgar_version


def _extract_file_info(edgar_file: Union[Path, str]) -> Dict:
    """
    Extract details from EDGAR filename.

    Expected filenames roughly of the form:
        - {version}_{species}_{year}.{resolution}.nc
        - {version}_{species}_{year}_{source}.{resolution}.nc
        - {version}_{species}_{year}_{month}.._{source..}.{resolution}.nc

    Args:
        edgar_file: Filename for an EDGAR database file

    Returns:
        Dict : Elements of the filename as a dictionary

        >>> _extract_file_info("v6.0_CH4_2015_TOTALS.0.1x0.1.nc")
            {"version": "v6.0", "species": "CH4", "year": 2015,
            "source": "TOTALS", "resolution": "0.1x0.1"}
        >>> _extract_file_info("v50_CH4_2015.0.1x0.1.nc")
            {"version": "v50", "species": "CH4", "year": 2015,
            "source": "", "resolution": "0.1x0.1"}
        >>> _extract_file_info("v432_CH4_2010_9_IPCC_6A_6D.0.1x0.1.nc")
            {"version": "v432", "species": "CH4", "year": 2010, "month": 9,
            "source": "IPCC-6A-6D", "resolution": "0.1x0.1"}
    """
    # Can extract details about files from the filename itself.
    # e.g. "v6.0_CH4_2015_TOTALS.0.1x0.1.nc"

    # Extract filename stem (name without extension) and split
    edgar_file = Path(edgar_file)
    filename = edgar_file.stem
    filename_split = filename.split("_")

    # Check if we can extract the known components from the filename
    # - version, species (upper), year
    try:
        version = filename_split[0]
        species = filename_split[1]
    except IndexError:
        raise ValueError(f"Did not recognise input file format: {filename}")
    else:
        index_remaining = 2

    # CO2 input has 2 options e.g.
    # - "v6.0_CO2_excl_short-cycle_org_C_2015_TOTALS.0.1x0.1.nc"
    # - "v6.0_CO2_org_short-cycle_C_1970_TOTALS.0.1x0.1.nc"
    if species.lower() == "co2":
        co2_options = ["excl_short-cycle_org_C", "org_short-cycle_C"]
        for option in co2_options:
            if option in filename:
                option_split = option.split("_")
                extra_sections = len(option_split)
                index_remaining += extra_sections

                source = "_".join(option_split[0:2]) + "_"
                break
        else:
            source = ""
    else:
        source = ""

    # Check if year can be cast to integer to check this is a valid value
    try:
        year_str = filename_split[index_remaining]
        year = int(year_str)
    except IndexError:
        raise ValueError(f"Unable to cast year extracted from file format to an integer: {year_str}")
    except ValueError:
        # In some files there is no source specified so
        # filename_split[2] contains the year and resolution
        # e.g. "v50_CH4_2015.0.1x0.1.nc" --> "2015.0.1x0.1"
        try:
            year = int(year_str.split(".")[0])
        except ValueError:
            raise ValueError(f"Could not find valid year value from file: {filename}")
    else:
        index_remaining += 1

    # Check whether month is included in filename
    # e.g. "v6.0_CH4_2015_1_ENE.0.1x0.1.nc"
    try:
        month: Optional[int] = int(filename_split[3])
    except (IndexError, ValueError):
        month = None
    else:
        index_remaining += 1

    # Attempt to extract source(s) and resolution from filename stem
    # e.g. "v6.0_CH4_2015_TOTALS.0.1x0.1.nc" --> "TOTALS.0.1x0.1"
    # e.g. "v50_CH4_2015.0.1x0.1.nc" --> "2015.0.1x0.1" (note no source in filename)
    # e.g. "v432_CH4_2010_9_IPCC_6A_6D.0.1x0.1.nc" --> "IPCC_6A_6D.0.1x0.1"
    try:
        source_resolution = "-".join(filename_split[index_remaining:])
    except (IndexError, ValueError):
        raise ValueError(f"Unable to extract source: {filename}")
    else:
        # e.g. "TOTALS.0.1x0.1" --> "TOTALS", "0.1x0.1"
        # e.g. "2015.0.1x0.1" --> "2015", "0.1x0.1" --> "", "0.1x0.1"
        # e.g. "IPCC_6A_6D.0.1x0.1" --> "IPCC-6A-6D", "0.1x0.1"
        em_source = source_resolution.split(".")[0]
        resolution = source_resolution.lstrip(em_source).lstrip(".")
        # Check source was actually contained in filename and not just the year
        # If so, set source to contain empty string
        if em_source == str(year):
            source += ""
        else:
            source += em_source

    file_info = {
        "version": version,
        "species": species,
        "year": year,
        "source": source,
        "resolution": resolution,
    }

    if month is not None:
        file_info["month"] = month

    return file_info


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
