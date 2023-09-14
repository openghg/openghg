from pathlib import Path
import xarray as xr
from openghg.standardise.meta import define_species_label
from openghg.store import ObsSurface
from openghg.util import remove_punctuation, hash_file
from typing import Dict, Union
import logging
import numpy as np

from openghg_defs import site_info_file
from openghg.util import load_json

logger = logging.getLogger("standardise")
logger.setLevel(logging.DEBUG)


def parse_agage(data_folder: Union[Path, str], drop_duplicate_timestamps: bool = True) -> Dict:
    data_folder = Path(data_folder)
    species_folders = [Path(f) for f in data_folder.iterdir() if f.is_dir()]
    print(
        "Warning: for now this function is not intelligent and will try and process all subfolders of this directory "
        "and try to read the contained .nc files. It will report any files it can't process or it doesn't understand"
    )

    skip_keys = ["comment", "file_created", "file_created_by", "github_url"]
    n_key = 0
    to_store = {}
    for species in species_folders:
        species_files = species.glob("*.nc")

        for site_file in species_files:
            ds = xr.open_dataset(site_file)

            lookup_inlet_height = False
            try:
                _ = str(int(float(ds.attrs["inlet_base_elevation_masl"])))
            except ValueError:
                lookup_inlet_height = True
                logger.warning(f"Can't read inlet height for {site_file.name}, we'll try looking it up.")

            if lookup_inlet_height:
                site_info = load_json(path=site_info_file)
                site_code = ds.attrs["site_code"].upper()
                inlet_heights = site_info[site_code]["AGAGE"]["height"]
                if len(inlet_heights) > 1:
                    logger.warning(
                        f"Multiple inlet heights found for {site_code}, skipping {site_file.name}."
                    )
                    continue

                inlet_height = inlet_heights[0]
            else:
                inlet_height = f"{int(float(ds.attrs['inlet_base_elevation_masl']))}m"

            # Do a very quick check to make sure the species is what we expect it to be
            attrs_species = ds.attrs["species"]
            folder_species = species.name
            if remove_punctuation(folder_species) != remove_punctuation(attrs_species):
                raise ValueError(
                    f"Mismatch between species foldername: {folder_species} and species in attributes {attrs_species}"
                )

            species_label_lower, _ = define_species_label(attrs_species)
            # Update the variable names to inlcude the species
            rename_dict = {
                "mf": species_label_lower,
                "mf_repeatability": f"{species_label_lower}_repeatability",
            }

            ds = ds.rename(rename_dict)

            unique, index, count = np.unique(ds.time, return_counts=True, return_index=True)
            n_dupes = unique[count > 1].size

            if n_dupes > 0:
                logger.warning(f"Dropping duplicate timestamps in {site_file.name}")
                ds = ds.drop_duplicates("time", keep="first")

            # Check the file attributes and pull out for use as metadata, maybe just keep a subset of these
            ObsSurface.validate_data(data=ds, species=attrs_species)

            metadata = {str(k): str(v) for k, v in ds.attrs.items() if k not in skip_keys}
            # Clean any empty strings
            metadata = {k: v for k, v in metadata.items() if v}

            metadata["filename_original"] = site_file.name
            metadata["file_hash"] = hash_file(site_file)
            metadata["network"] = "AGAGE"
            metadata["site"] = metadata["site_code"]
            # We'll store these as integer strings, if that's a term
            metadata["inlet"] = inlet_height
            metadata["species_label"] = species_label_lower

            to_store[str(n_key)] = {"data": ds, "metadata": metadata}
            n_key += 1

    return to_store
