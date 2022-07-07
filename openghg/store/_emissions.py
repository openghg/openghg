from pathlib import Path
from typing import DefaultDict, Dict, Optional, Union
from xarray import Dataset
from tempfile import TemporaryDirectory

from openghg.store.base import BaseStore

__all__ = ["Emissions"]


class Emissions(BaseStore):
    """This class is used to process emissions / flux data"""

    _root = "Emissions"
    _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    @staticmethod
    def read_data(binary_data: bytes, metadata: Dict, file_metadata: Dict) -> Dict:
        """Ready a footprint from binary data

        Args:
            binary_data: Footprint data
            metadata: Dictionary of metadata
            file_metadat: File metadata
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            try:
                filename = file_metadata["filename"]
            except KeyError:
                raise KeyError("We require a filename key for metadata read.")

            filepath = tmpdir_path.joinpath(filename)
            filepath.write_bytes(binary_data)

            return Emissions.read_file(filepath=filepath, **metadata)

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        species: str,
        source: str,
        domain: str,
        date: Optional[str] = None,
        high_time_resolution: Optional[bool] = False,
        period: Optional[Union[str, tuple]] = None,
        continuous: bool = True,
        overwrite: bool = False,
    ) -> Dict:
        """Read emissions file

        Args:
            filepath: Path of emissions file
            species: Species name
            domain: Emissions domain
            source: Emissions source
            high_time_resolution: If this is a high resolution file
            period: Period of measurements. Only needed if this can not be inferred from the time coords
                    If specified, should be one of:
                     - "yearly", "monthly"
                     - suitable pandas Offset Alias
                     - tuple of (value, unit) as would be passed to pandas.Timedelta function
            continuous: Whether time stamps have to be continuous.
            overwrite: Should this data overwrite currently stored data.
        Returns:
            dict: Dictionary of datasource UUIDs data assigned to
        """
        from collections import defaultdict
        from xarray import open_dataset
        from openghg.store import assign_data, load_metastore, datasource_lookup
        from openghg.util import (
            clean_string,
            hash_file,
            timestamp_now,
        )
        from openghg.store import infer_date_range

        species = clean_string(species)
        source = clean_string(source)
        domain = clean_string(domain)
        date = clean_string(date)

        filepath = Path(filepath)

        em_store = Emissions.load()

        # Load in the metadata store
        metastore = load_metastore(key=em_store._metakey)

        file_hash = hash_file(filepath=filepath)
        if file_hash in em_store._file_hashes and not overwrite:
            print(
                f"This file has been uploaded previously with the filename : {em_store._file_hashes[file_hash]} - skipping."
            )

        em_data = open_dataset(filepath)

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

        metadata["species"] = species
        metadata["domain"] = domain
        metadata["source"] = source
        metadata["date"] = date
        metadata["author"] = author_name
        metadata["processed"] = str(timestamp_now())

        # As emissions files handle things slightly differently we need to check the time values
        # more carefully.
        # e.g. a flux / emissions file could contain e.g. monthly data and be labelled as 2012 but
        # contain 12 time points labelled as 2012-01-01, 2012-02-01, etc.

        em_time = em_data.time

        start_date, end_date, period_str = infer_date_range(
            em_time, filepath=filepath, period=period, continuous=continuous
        )

        if date is None:
            # Check for how granular we should make the date label
            if "year" in period_str:
                date = f"{start_date.year}"
            elif "month" in period_str:
                date = f"{start_date.year}{start_date.month:02}"
            else:
                date = start_date.astype("datetime64[s]").astype(str)

        metadata["start_date"] = str(start_date)
        metadata["end_date"] = str(end_date)

        metadata["max_longitude"] = round(float(em_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(em_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(em_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(em_data["lat"].min()), 5)

        metadata["time_resolution"] = "high" if high_time_resolution else "standard"
        metadata["time_period"] = period_str

        key = "_".join((species, source, domain, date))

        emissions_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
        emissions_data[key]["data"] = em_data
        emissions_data[key]["metadata"] = metadata

        required = ("species", "source", "domain", "date")
        lookup_results = datasource_lookup(metastore=metastore, data=emissions_data, required_keys=required)

        data_type = "emissions"
        datasource_uuids = assign_data(
            data_dict=emissions_data,
            lookup_results=lookup_results,
            overwrite=overwrite,
            data_type=data_type,
        )

        em_store.add_datasources(uuids=datasource_uuids, data=emissions_data, metastore=metastore)

        # Record the file hash in case we see this file again
        em_store._file_hashes[file_hash] = filepath.name

        em_store.save()
        metastore.close()

        return datasource_uuids
