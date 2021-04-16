from openghg.modules import BaseModule
from pathlib import Path
from typing import Dict, Optional, Union
import traceback

__all__ = ["ObsSurface"]


class ObsSurface(BaseModule):
    """This class is used to process surface observation data"""

    _root = "ObsSurface"
    _uuid = "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"

    # We don't currently need to add anything here
    # def __init__(self):
    #     super().__init__()

    def save(self, bucket: Optional[Dict] = None) -> None:
        """Save the object to the object store

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
        site: str,
        network: str,
        inlet: Optional[str] = None,
        instrument: Optional[str] = None,
        sampling_period: Optional[str] = None,
        measurement_type: Optional[str] = "insitu",
        overwrite: Optional[bool] = False,
    ) -> Dict:
        """Process files and store in the object store. This function
            utilises the process functions of the other classes in this submodule
            to handle each data type.

        Args:
            filepath: Filepath(s)
            data_type: Data type, for example CRDS, GCWERKS, ICOS
            site: Site code/name
            network: Network name
            inlet: Inlet height. If processing multiple files pass None, OpenGHG will attempt to
            read inlets from data.
            instrument: Instrument name
            sampling_period: Sampling period in pandas style (e.g. 2H for 2 hour period, 2m for 2 minute period)
            measurement_type: Type of measurement e.g. insitu, flask
            overwrite: Overwrite previously uploaded data
        Returns:
            dict: Dictionary of Datasource UUIDs
        """
        from collections import defaultdict
        import logging
        from pathlib import Path
        import sys
        from tqdm import tqdm
        from openghg.util import load_object, hash_file, clean_string
        from openghg.processing import assign_data, DataTypes

        # Suppress numexpr thread count info info warnings
        logging.getLogger("numexpr").setLevel(logging.WARNING)

        if not isinstance(filepath, list):
            filepath = [filepath]

        try:
            data_type = DataTypes[data_type.upper()].name
        except KeyError:
            raise ValueError(f"Incorrect data type {data_type} selected.")

        # Test that the passed values are valid
        # Check validity of site, instrument, inlet etc in acrg_site_info.json
        # Clean the strings
        site = clean_string(site)
        network = clean_string(network)
        inlet = clean_string(inlet)
        instrument = clean_string(instrument)
        sampling_period = clean_string(sampling_period)

        # Load the data processing object
        data_obj = load_object(class_name=data_type)

        obs = ObsSurface.load()

        # Create a progress bar object using the filepaths, iterate over this below
        results = defaultdict(dict)

        with tqdm(total=len(filepath), file=sys.stdout) as progress_bar:
            for fp in filepath:
                if data_type == "GCWERKS":
                    try:
                        data_filepath = Path(fp[0])
                        precision_filepath = Path(fp[1])
                    except ValueError:
                        raise ValueError("For GCWERKS data both data and precision filepaths must be given.")
                else:
                    data_filepath = Path(fp)

                try:
                    file_hash = hash_file(filepath=data_filepath)
                    if file_hash in obs._file_hashes and overwrite is False:
                        raise ValueError(
                            f"This file has been uploaded previously with the filename : {obs._file_hashes[file_hash]}."
                        )

                    progress_bar.set_description(f"Processing: {data_filepath.name}")

                    if data_type == "GCWERKS":
                        data = data_obj.read_file(
                            data_filepath=data_filepath,
                            precision_filepath=precision_filepath,
                            site=site,
                            network=network,
                            inlet=inlet,
                            instrument=instrument,
                            sampling_period=sampling_period,
                            measurement_type=measurement_type
                        )
                    else:
                        data = data_obj.read_file(
                            data_filepath=data_filepath,
                            site=site,
                            network=network,
                            inlet=inlet,
                            instrument=instrument,
                            sampling_period=sampling_period,
                            measurement_type=measurement_type
                        )

                    # Extract the metadata for each set of measurements to perform a Datasource lookup
                    metadata = {key: data["metadata"] for key, data in data.items()}

                    lookup_results = obs.datasource_lookup(metadata=metadata)

                    # Create Datasources, save them to the object store and get their UUIDs
                    datasource_uuids = assign_data(gas_data=data, lookup_results=lookup_results, overwrite=overwrite)

                    results["processed"][data_filepath.name] = datasource_uuids

                    # Record the Datasources we've created / appended to
                    obs.add_datasources(datasource_uuids, metadata)

                    # Store the hash as the key for easy searching, store the filename as well for
                    # ease of checking by user
                    obs._file_hashes[file_hash] = data_filepath.name
                except Exception:
                    results["error"][data_filepath.name] = traceback.format_exc()

                progress_bar.update(1)

        # Save this object back to the object store
        obs.save()

        return results

    def datasource_lookup(self, metadata: Dict) -> Dict:
        """Find the Datasource we should assign the data to

        Args:
            metadata: Dictionary of metadata returned from the data_obj.read_file function
        Returns:
            dict: Dictionary of datasource information
        """
        lookup_results = {}
        for key, data in metadata.items():
            site = data["site"]
            network = data["network"]
            inlet = data["inlet"]

            # TODO - remove this once further checks for inlet processing
            # are in place
            if inlet is None:
                raise ValueError("No valid inlet height.")

            species = data["species"]

            result = self._datasource_table[site][network][inlet][species]

            if not result:
                result = False

            lookup_results[key] = result

        return lookup_results

    def save_datsource_info(self, datasource_data: Dict) -> None:
        """Save the datasource information to

        Args:
            datasource_data: Dictionary of datasource data to add
            to the Datasource table
        Returns:
            None

        """
        raise NotImplementedError()

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

        del self._datasource_uuids[uuid]
