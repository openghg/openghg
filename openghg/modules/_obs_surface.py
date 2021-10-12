from openghg.modules import BaseModule
from pathlib import Path
from typing import DefaultDict, Dict, Union

# import traceback

__all__ = ["ObsSurface"]


class ObsSurface(BaseModule):
    """This class is used to process surface observation data"""

    _root = "ObsSurface"
    _uuid = "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"

    @staticmethod
    def read_file(
        filepath: Union[str, Path, list],
        data_type: str,
        site: str,
        network: str,
        inlet: str = None,
        instrument: str = None,
        sampling_period: str = None,
        measurement_type: str = "insitu",
        overwrite: bool = False,
    ) -> Dict:
        """Process files and store in the object store. This function
            utilises the process functions of the other classes in this submodule
            to handle each data type.

        Args:
            filepath: Filepath(s)
            data_type: Data type, for example CRDS, GCWERKS
            site: Site code/name
            network: Network name
            inlet: Inlet height. If processing multiple files pass None, OpenGHG will attempt to
            read inlets from data.
            instrument: Instrument name
            sampling_period: Sampling period in pandas style (e.g. 2H for 2 hour period, 2m for 2 minute period).
            measurement_type: Type of measurement e.g. insitu, flask
            overwrite: Overwrite previously uploaded data
        Returns:
            dict: Dictionary of Datasource UUIDs
        """
        from collections import defaultdict
        from pathlib import Path
        from pandas import Timedelta
        import sys
        from tqdm import tqdm
        from openghg.util import load_object, hash_file, clean_string
        from openghg.processing import assign_data, DataTypes

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

        sampling_period_seconds: Union[str, None] = None
        # If we have a sampling period passed we want the number of seconds
        if sampling_period is not None:
            sampling_period_seconds = str(Timedelta(sampling_period).total_seconds())

        # Load the data processing object
        data_obj = load_object(class_name=data_type)

        obs = ObsSurface.load()

        # Create a progress bar object using the filepaths, iterate over this below
        results: DefaultDict[str, Dict] = defaultdict(dict)

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

                # try:
                file_hash = hash_file(filepath=data_filepath)
                if file_hash in obs._file_hashes and overwrite is False:
                    raise ValueError(f"This file has been uploaded previously with the filename : {obs._file_hashes[file_hash]}.")

                progress_bar.set_description(f"Processing: {data_filepath.name}")

                if data_type == "GCWERKS":
                    data = data_obj.read_file(
                        data_filepath=data_filepath,
                        precision_filepath=precision_filepath,
                        site=site,
                        network=network,
                        inlet=inlet,
                        instrument=instrument,
                        sampling_period=sampling_period_seconds,
                        measurement_type=measurement_type,
                    )
                else:
                    data = data_obj.read_file(
                        data_filepath=data_filepath,
                        site=site,
                        network=network,
                        inlet=inlet,
                        instrument=instrument,
                        sampling_period=sampling_period_seconds,
                        measurement_type=measurement_type,
                    )

                # Extract the metadata for each set of measurements to perform a Datasource lookup
                metadata = {key: data["metadata"] for key, data in data.items()}

                lookup_results = obs.datasource_lookup(metadata=metadata)

                # Create Datasources, save them to the object store and get their UUIDs
                datasource_uuids = assign_data(data_dict=data, lookup_results=lookup_results, overwrite=overwrite)

                results["processed"][data_filepath.name] = datasource_uuids

                # Record the Datasources we've created / appended to
                obs.add_datasources(datasource_uuids, metadata)

                # Store the hash as the key for easy searching, store the filename as well for
                # ease of checking by user
                obs._file_hashes[file_hash] = data_filepath.name
                # except Exception:
                #     results["error"][data_filepath.name] = traceback.format_exc()

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
            sampling_period = data["sampling_period"]

            # TODO - remove these once further checks for metadata inputs are in place
            if inlet is None:
                raise ValueError("No valid inlet height.")

            if sampling_period is None:
                raise ValueError("No valid sampling period.")

            species = data["species"]

            lookup_results[key] = self.lookup_uuid(
                site=site, network=network, inlet=inlet, species=species, sampling_period=sampling_period
            )

        return lookup_results

    def add_datasources(self, datasource_uuids: Dict, metadata: Dict) -> None:
        """Add the passed list of Datasources to the current list

        Args:
            datasource_uuids: Datasource UUIDs
            metadata: Metadata for each species
        Returns:
            None
        """
        for key, uid in datasource_uuids.items():
            md = metadata[key]
            site = md["site"]
            network = md["network"]
            inlet = md["inlet"]
            species = md["species"]
            sampling_period = md["sampling_period"]

            # TODO - remove this check when improved input sanitisation is in place
            if not any((site, network, inlet, species, sampling_period)):
                raise ValueError("Please ensure site, network, inlet, species and sampling_period are not None")

            result = self.lookup_uuid(site=site, network=network, inlet=inlet, species=species, sampling_period=sampling_period)

            if result and result != uid:
                raise ValueError("Mismatch between assigned uuid and stored Datasource uuid.")

            self.set_uuid(site=site, network=network, inlet=inlet, species=species, sampling_period=sampling_period, uuid=uid)
            self._datasource_uuids[uid] = key

    def lookup_uuid(self, site: str, network: str, inlet: str, species: str, sampling_period: int) -> Union[str, bool]:
        """Perform a lookup for the UUID of a Datasource

        Args:
            site: Site code
            network: Network name
            inlet: Inlet height
            species: Species name
            sampling_period: Sampling period in seconds
        Returns:
            str or bool: UUID if exists else None
        """
        uuid = self._datasource_table[site][network][species][inlet][sampling_period]

        return uuid if uuid else False

    def set_uuid(self, site: str, network: str, inlet: str, species: str, sampling_period: int, uuid: str) -> None:
        """Record a UUID of a Datasource in the datasource table

        Args:
            site: Site code
            network: Network name
            inlet: Inlet height
            species: Species name
            sampling_period: Sampling period in seconds
            uuid: UUID of Datasource
        Returns:
            None
        """
        self._datasource_table[site][network][species][inlet][sampling_period] = uuid

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

        data_keys = datasource.raw_keys()

        for version in data_keys:
            key_data = data_keys[version]["keys"]

            for daterange in key_data:
                key = key_data[daterange]
                delete_object(bucket=bucket, key=key)

        # Then delete the Datasource itself
        key = f"{Datasource._datasource_root}/uuid/{uuid}"
        delete_object(bucket=bucket, key=key)

        del self._datasource_uuids[uuid]
