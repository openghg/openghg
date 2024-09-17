import pandas as pd
import xarray as xr
import shutil
from pathlib import Path
import pkg_resources
from openghg.retrieve import get_obs_surface, get_obs_column
from typing import Union, Optional, Sequence, TextIO


def define_obs_types() -> list:
    """
    Define names of obs type folders to be included in an ObsPack.
    """
    obs_types = ["surface-insitu", "surface-flask", "column"]
    return obs_types


def define_get_functions() -> dict:
    """
    Define relationship between obs type names (for folders) and get functions.
    """
    get_functions = {
        "surface-insitu": get_obs_surface,
        "surface-flask": get_obs_surface,
        "column": get_obs_column,
    }

    return get_functions


def define_obspack_filename(
    site: str, species: str, inlet: str, version: str, obs_type: str, obspack_folder: Union[Path, str]
) -> Path:
    """
    Create file name with the correct naming convention.
    """
    obs_types = define_obs_types()
    obspack_folder = Path(obspack_folder)
    if obs_type in obs_types:
        filename = obspack_folder / obs_type / f"{species}_{site}_{inlet}_{obs_type}_{version}.nc"
    else:
        raise ValueError(f"Did not recognise obs_type {obs_type}. Should be one of: {obs_types}")
    return filename


def create_obspack_structure(
    output_path: Union[Path, str],
    obspack_name: str,
    obs_types: Sequence = define_obs_types(),
    release_files: Optional[Sequence] = None,
) -> Path:
    """
    Create the structure for the new obspack
    """

    # output_path = Path("~/work/creating_obspack").expanduser()
    obspack_folder = Path(output_path) / obspack_name

    if release_files is None:
        release_file_readme = pkg_resources.resource_filename("openghg", "data/obspack/obspack_README.md")
        release_files = [release_file_readme]

    # TODO: Update this to use logger
    print(f"Creating obspack folder: {obspack_folder} and subfolder(s)")
    for subfolder in obs_types:
        subfolder = obspack_folder / subfolder
        subfolder.mkdir(parents=True)

    for file in release_files:
        shutil.copy(file, obspack_folder)

    return obspack_folder


def read_input_file(filename: Union[Path, str]) -> tuple[list, list]:
    """
    Read input file containing search parameters and use to get data from object store.
    """
    search_df = pd.read_csv(filename)

    obs_type_name = "obs_type"
    get_functions = define_get_functions()

    data_object_all = []
    unique_obs_types = []
    for i, row in search_df.iterrows():
        obs_type = row[obs_type_name]
        get_fn = get_functions[obs_type]

        kwargs = row.to_dict()
        kwargs.pop(obs_type_name)

        # TODO: Update to a more robust check (may be code to already do this?)
        if "-" in kwargs["inlet"]:
            start, end = kwargs["inlet"].split("-")
            kwargs["inlet"] = slice(start, end)

        # TODO: Decide details around catching errors for no files/multi files found.
        surface_data = get_fn(**kwargs)
        # search_results = search_surface(**kwargs)
        # data_retrieved = search_results.retrieve()
        data_object_all.append(surface_data)

        if obs_type not in unique_obs_types:
            unique_obs_types.append(obs_type)

    return data_object_all, unique_obs_types


def collate_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each unique entry in a column, collate this as a single string separated by a semi-colon.
    """
    df_new = pd.DataFrame()
    for name, series in df.items():
        unique_values = series.unique()
        collated_value = ",".join([str(value) for value in unique_values])

        df_new[name] = [collated_value]

    return df_new


def define_site_details(ds: xr.Dataset, obs_type: str, strict: bool = False) -> dict:
    """
    Define each row containing the site details.
    """
    attrs = ds.attrs

    key_names = {
        "site": "Site code",
        "station_long_name": "Name",
        "inlet": "Inlet height",
        "station_latitude": "Latitude",
        "station_longitude": "Longitude",
        "data_owner": "Data owner",
        "data_owner_email": "Email",
    }

    params = {}
    for key, new_name in key_names.items():
        if key in attrs:
            params[new_name] = attrs[key]
        else:
            if strict:
                raise ValueError("Unable to find '{}' key in site data attributes")
            else:
                pass
                # include warning

    params["Observation type"] = obs_type

    return params


def create_site_index(df: pd.DataFrame, output_file: TextIO) -> None:
    """
    Expects a DataFrame object.
    Expects file object input (not just filename) at the moment.
    """

    # Site index should include key metadata for each file (enough to be distinguishable but not too much?)
    # Want to create a DataFrame and pass a file object to this - then can add comments before and after the table as needed

    site_details = df.groupby("Site code").apply(collate_strings)
    site_details = site_details.rename(columns={"Inlet height": "Inlet heights"})

    # index_output_name = open(obspack_folder / f"site_index_details_{version}.txt", "w")

    # Add header to file
    output_file.write("++++++++++++\n")
    output_file.write("Site details\n")
    output_file.write("++++++++++++\n")
    output_file.write("\n")

    site_details.to_csv(output_file, index=False, sep="\t")

    output_file.close()


def create_obspack(
    filename: Union[Path, str],
    output_path: Union[Path, str],
    obspack_name: str,
    release_files: Optional[Sequence] = None,
) -> None:
    """
    Create ObsPack of obspack_name at output_path from input search file.
    """

    retrieved_data, obs_types = read_input_file(filename)

    obspack_folder = create_obspack_structure(
        output_path, obspack_name, obs_types=obs_types, release_files=release_files
    )

    # Put things in obspack and build structure

    # TODO: May need to be formalised and split out (need more flexibility for column etc.)
    naming_parameters = ["site", "species", "inlet"]
    # TODO: TEMPORARY - To be updated and generalised
    version = "v1"
    obs_type = "surface-insitu"
    ####

    site_detail_rows = []
    for data in retrieved_data:
        metadata = data.metadata
        metadata_param = {param: metadata[param] for param in naming_parameters}

        output_name = define_obspack_filename(
            version=version, obs_type=obs_type, obspack_folder=obspack_folder, **metadata_param
        )
        ds = data.data
        ds.to_netcdf(output_name)

        site_details = define_site_details(ds, obs_type)
        site_detail_rows.append(site_details)

    index_output_filename = obspack_folder / f"site_index_details_{version}.txt"

    site_details_df = pd.DataFrame(site_detail_rows)
    print(f"Writing site details: {index_output_filename}")
    index_output_name = open(index_output_filename, "w")
    create_site_index(site_details_df, index_output_name)
