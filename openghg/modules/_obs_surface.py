from openghg.modules import BaseModule
from pathlib import Path
from typing import Dict, Optional, Union

__all__ = ["ObsSurface"]


class ObsSurface(BaseModule):
    """This class is used to process surface observation data"""

    _root = "ObsSurface"
    _uuid = "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"

    def __init__(self):
        from Acquire.ObjectStore import get_datetime_now
        from collections import defaultdict

        self._creation_datetime = get_datetime_now()
        self._stored = False
        # Keyed by name - allows retrieval of UUID from name
        self._datasource_names = {}
        # Keyed by UUID - allows retrieval of name by UUID
        self._datasource_uuids = {}
        # Hashes of previously uploaded files
        self._file_hashes = {}
        # Keyed by UUID
        self._rank_data = defaultdict(dict)

    def to_data(self) -> Dict:
        """ Return a JSON-serialisable dictionary of object
        for storage in object store

        Returns:
            dict: Dictionary version of object
        """
        from Acquire.ObjectStore import datetime_to_string

        data = {}
        data["creation_datetime"] = datetime_to_string(self._creation_datetime)
        data["stored"] = self._stored
        data["datasource_uuids"] = self._datasource_uuids
        data["datasource_names"] = self._datasource_names
        data["file_hashes"] = self._file_hashes
        data["rank_data"] = self._rank_data

        return data

    def save(self, bucket: Optional[dict] = None) -> None:
        """ Save the object to the object store

        Args:
            bucket: Bucket for data
        Returns:
            None
        """
        from openghg.objectstore import get_bucket, set_object_from_json

        if bucket is None:
            bucket = get_bucket()

        obs_key = f"{ObsSurface._root}/uuid/{ObsSurface._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    @staticmethod
    def read_file(
        filepath: Union[str, Path, list],
        data_type: str,
        site: Optional[str] = None,
        network: Optional[str] = None,
        instrument: Optional[str] = None,
        overwrite: Optional[bool] = False,
    ) -> Dict:
        """ Process files and store in the object store. This function
            utilises the process functions of the other classes in this submodule
            to handle each data type.

        Args:
            filepath: Filepath(s)
            data_type: Data type
            site: Site code/name
            network: Network name
            overwrite: Overwrite previously uploaded data
        Returns:
            dict: Dictionary of Datasource UUIDs
        """
        from collections import defaultdict
        import logging
        from pathlib import Path
        import sys
        from tqdm import tqdm
        from openghg.util import load_object, hash_file
        from openghg.processing import assign_data, DataTypes

        # Suppress numexpr thread count info info warnings
        logging.getLogger("numexpr").setLevel(logging.WARNING)

        if not isinstance(filepath, list):
            filepath = [filepath]

        data_type = DataTypes[data_type.upper()].name
        data_obj = load_object(class_name=data_type)

        obs = ObsSurface.load()

        # Create a progress bar object using the filepaths, iterate over this below
        results = {}
        with tqdm(total=len(filepath), file=sys.stdout) as progress_bar:
            for fp in filepath:
                try:
                    progress_bar.set_description(f"Processing: {fp}")

                    if data_type == "GCWERKS":
                        if not isinstance(fp, tuple):
                            raise TypeError("To process GCWERKS data a data filepath and a precision filepath must be suppled as a tuple")

                        data_filepath = Path(fp[0])
                        precision_filepath = Path(fp[1])

                        data = data_obj.read_file(
                            data_filepath=data_filepath, precision_filepath=precision_filepath, site=site, network=network
                        )
                    else:
                        if isinstance(fp, tuple):
                            raise TypeError(
                                "Only a single data file may be passed for this data type. Please check you have the correct type selected."
                            )

                        data_filepath = Path(fp)
                        data = data_obj.read_file(data_filepath=data_filepath, site=site, network=network)

                    # TODO - need a new way of creating the source name
                    source_name = data_filepath.stem

                    # Hash the file and if we've seen this file before raise an error
                    file_hash = hash_file(filepath=data_filepath)
                    if file_hash in obs._file_hashes and not overwrite:
                        raise ValueError(f"This file has been uploaded previously with the filename : {obs._file_hashes[file_hash]}.")

                    datasource_table = defaultdict(dict)
                    # For each species check if we have a Datasource
                    for species in data:
                        name = "_".join([source_name, species])
                        datasource_table[species]["uuid"] = obs._datasource_names.get(name, False)
                        datasource_table[species]["name"] = name

                    # Create Datasources, save them to the object store and get their UUIDs
                    datasource_uuids = assign_data(gas_data=data, lookup_results=datasource_table, overwrite=overwrite)

                    # results[data_filepath.name] = datasource_uuids

                    # Record the Datasources we've created / appended to
                    obs.add_datasources(datasource_uuids)

                    # Store the hash as the key for easy searching, store the filename as well for
                    # ease of checking by user
                    obs._file_hashes[file_hash] = data_filepath.name
                except:
                    import traceback
                    results[data_filepath.stem] = traceback.format_exc()
            
                progress_bar.update(1)

        # Save this object back to the object store
        obs.save()

        return results

    @staticmethod
    def read_folder(folder_path: Union[str, Path], data_type: str, network: str, extension: Optional[str] = "dat") -> Dict:
        """Find files with the given extension (by default dat) in the passed folder

        Args:
            folder_path: Path of folder
            data_type: Data type
            extension: File extension
        Returns:
            dict: Dictionary of the Datasources created for each file
        """
        from pathlib import Path

        if extension.startswith("."):
            extension = extension.strip(".")

        filepaths = [f.resolve() for f in Path(folder_path).glob(f"**/*.{extension}")]

        if not filepaths:
            raise FileNotFoundError("No data files found")

        return ObsSurface.read_file(filepath=filepaths, data_type=data_type, network=network)

    def delete(self, uuid: str) -> None:
        """Delete a Datasource with the given UUID

        This function deletes both the record of the object store in he

        Args:
            uuid (str): UUID of Datasource
        Returns:
            None
        """
        from openghg.objectstore import delete_object, get_bucket
        from openghg.modules import Datasource

        bucket = get_bucket()
        # Load the Datasource and get all its keys
        # iterate over these keys and delete them
        datasource = Datasource.load(uuid=uuid)

        data_keys = datasource.data_keys(return_all=True)

        for version in data_keys:
            key_data = data_keys[version]["keys"]

            for daterange in key_data:
                key = key_data[daterange]
                delete_object(bucket=bucket, key=key)

        # Then delete the Datasource itself
        key = f"{Datasource._datasource_root}/uuid/{uuid}"
        delete_object(bucket=bucket, key=key)

        # First remove from our dictionary of Datasources
        name = self._datasource_uuids[uuid]

        del self._datasource_names[name]
        del self._datasource_uuids[uuid]
