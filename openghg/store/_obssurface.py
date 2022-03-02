from openghg.store.base import BaseStore
from openghg.types import pathType, multiPathType, resultsType
from pathlib import Path
from typing import DefaultDict, Dict, Optional, Union


__all__ = ["ObsSurface"]


class ObsSurface(BaseStore):
    """This class is used to process surface observation data"""

    _root = "ObsSurface"
    _uuid = "da0b8b44-6f85-4d3c-b6a3-3dde34f6dea1"

    @staticmethod
    def read_file(
        filepath: multiPathType,
        data_type: str,
        network: str,
        site: str,
        inlet: Optional[str] = None,
        instrument: Optional[str] = None,
        sampling_period: Optional[str] = None,
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
            inlet: Inlet height. If retrieve multiple files pass None, OpenGHG will attempt to
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
        from openghg.util import load_surface_parser, hash_file, clean_string, verify_site
        from openghg.types import SurfaceTypes
        from openghg.store import assign_data

        if not isinstance(filepath, list):
            filepath = [filepath]

        try:
            data_type = SurfaceTypes[data_type.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {data_type} selected.")

        # Test that the passed values are valid
        # Check validity of site, instrument, inlet etc in acrg_site_info.json
        # Clean the strings
        site = verify_site(site=site)

        network = clean_string(network)
        inlet = clean_string(inlet)
        instrument = clean_string(instrument)
        sampling_period = clean_string(sampling_period)

        sampling_period_seconds: Union[str, None] = None
        # If we have a sampling period passed we want the number of seconds
        if sampling_period is not None:
            sampling_period_seconds = str(Timedelta(sampling_period).total_seconds())

        # Load the data retrieve object
        parser_fn = load_surface_parser(data_type=data_type)

        obs = ObsSurface.load()

        results: resultsType = defaultdict(dict)

        # Create a progress bar object using the filepaths, iterate over this below
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
                    print(
                        f"This file has been uploaded previously with the filename : {obs._file_hashes[file_hash]} - skipping."
                    )

                progress_bar.set_description(f"Processing: {data_filepath.name}")

                if data_type == "GCWERKS":
                    data = parser_fn(
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
                    data = parser_fn(
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
                datasource_uuids = assign_data(
                    data_dict=data, lookup_results=lookup_results, overwrite=overwrite
                )

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

    @staticmethod
    def read_multisite_aqmesh(
        data_filepath: pathType,
        metadata_filepath: pathType,
        network: str = "aqmesh_glasgow",
        instrument: str = "aqmesh",
        sampling_period: int = 60,
        measurement_type: str = "insitu",
        overwrite: bool = False,
    ) -> DefaultDict:
        """Read AQMesh data for the Glasgow network

        NOTE - temporary function until we know what kind of AQMesh data
        we'll be retrieve in the future.

        This data is different in that it contains multiple sites in the same file.
        """
        from openghg.standardise.surface import parse_aqmesh
        from openghg.store import assign_data
        from openghg.util import hash_file
        from collections import defaultdict
        from tqdm import tqdm

        data_filepath = Path(data_filepath)
        metadata_filepath = Path(metadata_filepath)

        # Load the ObsSurface object for retrieve
        obs = ObsSurface.load()
        # Get a dict of data and metadata
        processed_data = parse_aqmesh(data_filepath=data_filepath, metadata_filepath=metadata_filepath)

        results: resultsType = defaultdict(dict)
        for site, site_data in tqdm(processed_data.items()):
            metadata = site_data["metadata"]
            measurement_data = site_data["data"]

            inlet = metadata["inlet"]
            species = metadata["species"]

            file_hash = hash_file(filepath=data_filepath)

            if obs.seen_hash(file_hash=file_hash) and overwrite is False:
                raise ValueError(
                    f"This file has been uploaded previously with the filename : {obs._file_hashes[file_hash]}."
                )

            uuid = obs.lookup_uuid(
                site=site,
                network=network,
                inlet=inlet,
                species=species,
                sampling_period=sampling_period,
            )

            # Jump through these hoops until we can rework the data assignment functionality to split it out
            # into more sensible functions
            # TODO - fix the assign data function to avoid this kind of hoop jumping
            combined = {site: {"data": measurement_data, "metadata": metadata}}
            lookup_result = {site: uuid}

            # Create Datasources, save them to the object store and get their UUIDs
            datasource_uuids = assign_data(
                data_dict=combined, lookup_results=lookup_result, overwrite=overwrite
            )

            results[site] = datasource_uuids

            # TODO - fix add_datasources as well
            _metadata = {site: metadata}

            # Record the Datasources we've created / appended to
            obs.add_datasources(datasource_uuids=datasource_uuids, metadata=_metadata)

            # Store the hash as the key for easy searching, store the filename as well for
            # ease of checking by user
            obs.set_hash(file_hash=file_hash, filename=data_filepath.name)

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
                site=site,
                network=network,
                inlet=inlet,
                species=species,
                sampling_period=sampling_period,
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
                raise ValueError(
                    "Please ensure site, network, inlet, species and sampling_period are not None"
                )

            result = self.lookup_uuid(
                site=site,
                network=network,
                inlet=inlet,
                species=species,
                sampling_period=sampling_period,
            )

            if result and result != uid:
                raise ValueError("Mismatch between assigned uuid and stored Datasource uuid.")

            self.set_uuid(
                site=site,
                network=network,
                inlet=inlet,
                species=species,
                sampling_period=sampling_period,
                uuid=uid,
            )
            self._datasource_uuids[uid] = key

    def lookup_uuid(
        self, site: str, network: str, inlet: str, species: str, sampling_period: int
    ) -> Union[str, bool]:
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

    def set_uuid(
        self,
        site: str,
        network: str,
        inlet: str,
        species: str,
        sampling_period: int,
        uuid: str,
    ) -> None:
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
        from openghg.store.base import Datasource

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

    def seen_hash(self, file_hash: str) -> bool:
        return file_hash in self._file_hashes

    def set_hash(self, file_hash: str, filename: str) -> None:
        self._file_hashes[file_hash] = filename
