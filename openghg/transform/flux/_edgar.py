"""
Parse EDGAR flux files and perform area-conservative regridding
of fluxes.

Currently based on acrg.name.emissions_helperfuncs.getedgarannualtotals()

Additional edgar functions which could be incorporated.
- getedgarv5annualsectors
- getedgarv432annualsectors
- (monthly sectors?)

TODO: Work out how to select frequency
- could try and relate to period e.g. "monthly" versus "yearly" etc.

EDGAR flux files tend to have dimensions (lat, lon), and no time dimension;
however, time is included in the filename, so we try to extract this, along
with version, species, sector, and resolution.

Typical file names:

 v432_CH4_1978.0.1x0.1.nc (or .zip)
 v50_CH4_1978.0.1x0.1.nc (or .zip)
 v6.0_CH4_1978_TOTALS.0.1x0.1.nc

 v50_CO2_excl_short-cycle_org_C_1978.0.1x0.1.nc (or .zip)
 v50_CO2_org_short-cycle_C_1978.0.1x0.1.nc (or .zip)
 v50_N2O_1978.0.1x0.1.zip (or .zip)

Futher information is contained in the EDGAR readme files.
For instance, from "_readme.html" from v6.0 data:

'Yearly Emissions gridmaps in ton substance / 0.1degree x 0.1degree / year
 for the .txt files with longitude and latitude coordinates referring to
 the low-left corner of each grid-cell.'

'Monthly Emissions gridmaps in ton substance / 0.1degree x 0.1degree / month
 for the .txt files with longitude and latitude coordinates referring to
 the low-left corner of each grid-cell.'

'Emissions gridmaps in kg substance /m2 /s for the .nc files with longitude
 and latitude coordinates referring to the cell center of each grid-cell.'

For EDGAR v8.0,

"""

import pathlib
import re
import zipfile
from collections import namedtuple
from typing import Any, Optional, cast
import logging
import numpy as np
import xarray as xr
from numpy import ndarray

from openghg.standardise.meta import assign_flux_attributes, define_species_label
from openghg.store import infer_date_range
from openghg.util import (
    clean_string,
    molar_mass,
    synonyms,
    find_coord_name,
    convert_internal_longitude,
    timestamp_now,
)


logger = logging.getLogger("openghg.transform.flux")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


ArrayType = Optional[ndarray | xr.DataArray]


_edgar_known_versions = ("v432", "v50", "v60", "v70", "v80")


# TODO: make this work for...
# - yearly - "v6.0_CH4_2015_TOTALS.0.1x0.1.nc"
# - sectoral - "v6.0_CH4_2015_ENE.0.1x0.1.nc"
# - monthly sectoral - "v6.0_CH4_2015_1_ENE.0.1x0.1.nc", "v6.0_CH4_2015_2_ENE.0.1x0.1.nc", ...
def assemble_edgar_metadata(
    filepath: pathlib.Path | zipfile.Path,
    species: str | None = None,
    version: str | None = None,
) -> dict[str, Any]:
    """Combine given metadata with metadata extracted from filename.

    Args:
        filepath: pathlib.Path or zipfile.Path to EDGAR datafile
        species: species of flux
        version: EDGAR database version

    Returns:
        dictionary containing keys: version, species, year, source, resolution
    """
    known_versions = _edgar_known_versions

    if species is not None:
        species = define_species_label(species)[0]

    valid_version = False
    if version is not None:
        version = clean_string(version)
        if version in known_versions:
            valid_version = True

    try:
        metadata = _extract_file_info(filepath)
    except ValueError as e:
        # NOTE: if species is not None and valid_version is True, then we have everything we
        # need except resolution at this point...
        raise ValueError(f"Could extract EDGAR metadata from file f{filepath.name}") from e
    else:
        if species:
            if species != synonyms(metadata["species"]):
                logger.warning(
                    "Input species does not match species extracted from database filenames. Please check."
                )
            metadata["species"] = species
        else:
            metadata["species"] = clean_string(metadata["species"])

        if valid_version:
            metadata["version"] = version
        else:
            if clean_string(metadata["version"]) not in known_versions:
                if version is not None:
                    raise ValueError(
                        f"Unable to infer EDGAR version ({version})."
                        f" Please pass as an argument (one of {known_versions})"
                    )
                else:
                    raise ValueError(
                        f"Unable to infer EDGAR version."
                        f" Please pass as an argument (one of {known_versions})"
                    )
            else:
                metadata["version"] = clean_string(metadata["version"])

        source_from_file = metadata["source"]
        if source_from_file in ("TOTALS", ""):
            source = "anthro"
        elif metadata["species"] == "co2" and "TOTALS" in source_from_file:
            co2_source = "_".join(source_from_file.split("_")[:-1])
            source = clean_string(f"{co2_source}_anthro")
        else:
            source = clean_string(source_from_file)

        metadata["source"] = source

    return metadata


def parse_edgar(
    datapath: pathlib.Path,
    date: str,
    species: str | None = None,
    domain: str | None = None,
    lat_out: ArrayType = None,
    lon_out: ArrayType = None,
    source: str | None = None,
    edgar_version: str | None = None,
) -> dict:
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

    Pre-exisiting domains are defined within the openghg_defs "domain_info.json" file.

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
        source: Flux source to use; overrides the source extracted from the filename.
        edgar_version: EDGAR version in file. Will be inferred otherwise.

    Returns:
        dict: Dictionary of data

    TODO: Allow date range to be extracted rather than year?
    TODO: Add monthly parsing and sector stacking options
    """
    period = None

    # TODO: Add check for period? Only monthly or yearly (or equivalent inputs)

    if zipfile.is_zipfile(datapath):
        datapath = zipfile.Path(datapath)  # type: ignore

    # NOTE: the built-in `open` method doesn't work with zipfile.Path
    # but there is a zipfile.Path.open method that works the same as
    # pathlib.Path.open

    folder_filelist = [x for x in datapath.iterdir() if x.is_file()]

    # Extract netcdf files (only, for now) - ".txt" is also an option (not implemented)
    # Path(file.name).suffix workaround because zipfile.Path.suffix is Python 3.11+
    data_files = [file for file in folder_filelist if pathlib.Path(file.name).suffix == ".nc"]

    if not data_files:
        raise ValueError(f"No '.nc' files found in datapath: {datapath}")

    if edgar_version is None:
        # check if _readme.html is in folder_filelist, and if so, try to extract version
        readme = datapath / "_readme.html"
        if readme.exists():
            with readme.open("r") as f:  # pathlib.Path and zipfile.Path have .open method
                edgar_version = _check_readme_data(f.read())

    if len(date) == 4:
        year = int(date)
    else:
        raise ValueError(
            f"Date {date} does not represent a year;" " only annual EDGAR data can be processed currently."
        )

    FileInfo = namedtuple("FileInfo", "path metadata")
    files_by_year: dict[int, FileInfo] = {}
    for data_file in data_files:
        try:
            metadata = assemble_edgar_metadata(data_file, species, edgar_version)
        except ValueError:
            continue
        else:
            # Check if data is actually monthly "...2015_1" etc. - can't parse yet
            if "month" in metadata:
                raise NotImplementedError("Unable to parse monthly EDGAR data at present.")

            files_by_year[metadata["year"]] = FileInfo(data_file, metadata)

    if not files_by_year:
        raise ValueError(f"Unable to extract EDGAR file info from any files in {datapath}.")

    try:
        edgar_file, edgar_file_info = files_by_year[year]
    except KeyError:
        all_years = sorted(list(files_by_year.keys()))
        start_year, end_year = all_years[0], all_years[-1]

        if year < start_year:
            raise ValueError(f"Files span range: {start_year}-{end_year}." f" {year} is before this period.")
        elif year > end_year:
            logger.info(f"Using last available year from range:" f"{start_year}-{end_year}.")
        edgar_file, edgar_file_info = files_by_year[end_year]

    species_label = edgar_file_info["species"]
    version = edgar_file_info["version"]

    # get dataset
    # using .open("rb") for pathlib.Path and zipfile.Path compatibility
    edgar_ds = xr.open_dataset(edgar_file.open("rb"))

    # Expected name e.g. "emi_ch4", "emi_co2" for version <= 7; "fluxes" for version 8
    name = "fluxes" if version.startswith("v8") else f"emi_{species_label}"

    # check that data variable `name` is in `edgar_ds`
    if name not in edgar_ds.data_vars:
        if version.startswith("v8"):
            raise ValueError(
                f"Data variable {name} not present. We only support 'flx_nc' files, not 'emi_nc' files, for EDGAR v8.0"
            )
        else:
            raise ValueError(f"Data variable {name} not present.")

    # Convert from kg/m2/s to mol/m2/s
    species_molar_mass = molar_mass(species_label)
    kg_to_g = 1e3

    flux_da = edgar_ds[name]
    flux_da = flux_da * kg_to_g / species_molar_mass
    units = "mol/m2/s"

    # TODO: some options for f-gases (.emi files) have different units...
    # need to catch this

    lat_name = find_coord_name(flux_da, options=["lat", "latitude"])
    lon_name = find_coord_name(flux_da, options=["lon", "longitude"])
    if lat_name is None or lon_name is None:
        raise ValueError(
            f"Could not find '{lat_name}' or '{lon_name}' in EDGAR file.\n"
            " Please check this is a 2D grid map."
        )

    # Check range of longitude values and convert to -180 - +180
    flux_da = convert_internal_longitude(
        flux_da, lon_name=lon_name
    )  # TODO is this creating NaNs for East Asia domain?

    lat_out, lon_out = _check_lat_lon(domain, lat_out, lon_out)

    if lat_out is not None and lon_out is not None:
        # Will produce import error if xesmf has not been installed.
        from openghg.transform import regrid_uniform_cc
        from openghg.util import cut_data_extent

        # To improve performance of regridding algorithm cut down the data
        # to match the output grid (with buffer).
        flux_da_cut = cut_data_extent(flux_da, lat_out, lon_out)
        flux_values = flux_da_cut.values

        lat_in_cut = flux_da_cut[lat_name]
        lon_in_cut = flux_da_cut[lon_name]

        # area conservative regrid
        flux_values = regrid_uniform_cc(flux_values, lat_out, lon_out, lat_in_cut, lon_in_cut)
    else:
        lat_out = flux_da[lat_name]
        lon_out = flux_da[lon_name]
        flux_values = flux_da.values

    edgar_attrs = edgar_ds.attrs

    # Check for "time" dimension and add if missing.
    flux_ndim = flux_values.ndim
    time_name = "time"
    if time_name in flux_da:
        time = flux_da[time_name].values
        flux = flux_values  # TODO: this was missing... is this correct?? otherwise 'flux' might be undefined
    elif time_name not in flux_da and flux_ndim == 2:
        time = np.array([f"{year}-01-01"], dtype="datetime64[ns]")
        flux = flux_values[np.newaxis, ...]
    else:
        raise ValueError(
            f"Expected data variable '{name}' to contain 2 or 3 dimensions (including time),"
            f" but '{name}' has {flux_ndim} dimensions: {flux_da.dims}."
        )

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

    metadata = {}
    metadata.update(attrs)

    raw_edgar_domain = "globaledgar"
    if domain is None:
        domain = raw_edgar_domain

    source = source if source is not None else edgar_file_info["source"]
    metadata["species"] = species_label
    metadata["domain"] = domain
    metadata["source"] = source
    metadata["database"] = "EDGAR"
    metadata["database_version"] = edgar_file_info["version"]
    metadata["author"] = author_name
    metadata["processed"] = str(timestamp_now())
    metadata["data_type"] = "flux"

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

    emissions_data: dict[str, dict] = {}
    emissions_data[key] = {}
    emissions_data[key]["data"] = em_data
    emissions_data[key]["metadata"] = metadata
    emissions_data[key]["attributes"] = attrs

    emissions_data = assign_flux_attributes(emissions_data, units=units, prior_info_dict=prior_info_dict)

    return emissions_data


def _check_lat_lon(
    domain: str | None = None, lat_out: ArrayType = None, lon_out: ArrayType = None
) -> tuple[ndarray | None, ndarray | None]:
    """
    Define and check latitude and longitude values for a domain.

    The domain can be used in one of two ways:
        1. To specify a pre-exisiting lat, lon extent which can be extracted
        2. To supply a name for a new lat, lon extent which must be specified

    For case 1, only domain needs to be specified (lat_out and lon_out can
    be specified but they must already exactly match the domain definition).
    The details will be extracted from openghg_defs "domain_info.json" file.

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
    from openghg.util import convert_lon_to_360, find_domain

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

            if lon_out.max() - lon_out.min() >= 360:
                raise ValueError(
                    "Invalid domain definition."
                    " Expected longitude in range: 0 to 360."
                    f"Current longitude: {lon_out.min()} - {lon_out.max()}"
                )

    if lon_out is not None and (lon_out.max() > 180 or lon_out.min() < -180):
        logger.info("Converting longitude to stay within 0 to 360 bounds")
        lon_converted = convert_lon_to_360(lon_out)
        lon_out = cast(ndarray | None, lon_converted)

    return lat_out, lon_out


def _check_readme_data(readme_data: str) -> str | None:
    """Parse EDGAR _readme.html to find version.

    Args:
        readme_data: string containing contents of _readme.html

    Returns:
        EDGAR version, if found, otherwise None
    """
    try:
        # Ignoring types as issues caught by try-except statement
        # Find and extract title line from html file
        title_line = re.search("<title.*?>(.+?)</title>", readme_data).group()  # type: ignore
        # Extract version e.g. "v6.0" or "v4.3.2"
        edgar_version = re.search(r"v\d[.]\d[.]?\d*", title_line).group()  # type: ignore
    except (ValueError, AttributeError):
        return None

        # Check against known versions and remove '.' if these don't match.
    if edgar_version not in _edgar_known_versions:
        edgar_version = edgar_version.replace(".", "")

    return edgar_version


def _extract_file_info(edgar_file: pathlib.Path | zipfile.Path | str) -> dict:
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
    if isinstance(edgar_file, str):
        edgar_file = pathlib.Path(edgar_file)
    if isinstance(edgar_file, zipfile.Path):
        edgar_file = pathlib.Path(edgar_file.name)  # zipfile.Path.stem is Python 3.11+
    filename = edgar_file.stem

    # remove unwanted parts like:
    # FT2021, FT2022, etc
    # GHG
    # emi_nc, flx_nc
    cleaning_regexps = [r"FT(19|20)\d{2}_", r"GHG_", r"_(emi|flx)(_nc)?"]
    cleaning_regexps_joined = "|".join(cleaning_regexps)
    cleaning_pat = re.compile(rf"({cleaning_regexps_joined})")

    filename_clean = cleaning_pat.sub("", filename)

    # parse string of form:
    # {version}_{species}_{optional co2 info}_{year}_{optional month}_{optional sector}_{optional resolution}
    info_pat = re.compile(
        r"^(?P<version>v\d[\.\d]*)_"  # capture version e.g. v432, v50, v8.0
        r"(?P<species>[a-zA-Z\d-]+)_"  # capture species, e.g. CH4, c-C4F8, HFC-43-10-mee
        r"((?P<co2_options>(excl|org)_short-cycle)(_org)?(_C)?_)?"  # optional, capture CO2 info
        r"(?P<year>(19|20)\d{2})"  # capture year (might not end in _)
        r"(_(?P<month>\d{1,2}))?"  # optionally capture month
        r"(_(?P<sector>(\w+)))?"  # optionally capture sector, e.g. TOTALS, TNR_Ship, N2O, IPCC_4C_4D1_4D4
        r"(\.(?P<resolution>\d\.\dx\d\.\d))?"  # optionally capture resolution, e.g. 0.1x0.1
    )

    if m := info_pat.search(filename_clean):
        file_info = m.groupdict()
    else:
        # info_pat matches all files in known EDGAR versions
        # (verified by searching all file names for these versions
        # 18 March 2024)
        raise ValueError(f"Did not recognise input file format: {filename}")

    # make "source" string
    co2_options = file_info.pop("co2_options") or ""

    sector = file_info.pop("sector") or ""
    sector = sector.replace("_", "-")

    source = "_".join([co2_options, sector])
    source = source.strip("_")  # in case co2_options or sector is ""

    # if not source:
    #     raise ValueError(f"Unable to extract source: {filename}")

    file_info["source"] = source

    # year and month should be integers
    file_info["year"] = int(file_info["year"])

    if file_info["month"] is not None:
        file_info["month"] = int(file_info["month"])
    else:
        del file_info["month"]

    # file_info["version"] = clean_string(file_info["version"])

    # if not file_info["version"] in _edgar_known_versions:
    #     raise ValueError(f"Version {file_info['version']} inferred from {filename} not know known "
    #                      f"EDGAR versions: {_edgar_known_versions}.")

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
