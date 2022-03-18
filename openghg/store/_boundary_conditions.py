from openghg.store.base import BaseStore
from pathlib import Path
from typing import DefaultDict, Dict, Optional, Union
from xarray import Dataset
import pandas as pd
from pandas import Timestamp, DateOffset
from dateutil.relativedelta import relativedelta

__all__ = ["BoundaryConditions"]


class BoundaryConditions(BaseStore):
    """This class is used to process boundary condition data"""

    _root = "BoundaryConditions"
    # _uuid = "c5c88168-0498-40ac-9ad3-949e91a30872"

    def save(self) -> None:
        """Save the object to the object store

        Returns:
            None
        """
        from openghg.objectstore import get_bucket, set_object_from_json

        bucket = get_bucket()
        obs_key = f"{BoundaryConditions._root}/uuid/{BoundaryConditions._uuid}"

        self._stored = True
        set_object_from_json(bucket=bucket, key=obs_key, data=self.to_data())

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        species: str,
        bc_input: str,
        domain: str,
        # date: Union[str, Timestamp],
        period: Optional[Union[str, tuple]] = None,
        continuous: bool = True,
        overwrite: bool = False,
    ) -> Dict:
        """Read boundary conditions file

        Args:
            filepath: Path of boundary conditions file
            species: Species name
            bc_input: Input used to create boundary conditions. For example:
              - a model name such as "MOZART" or "CAMS"
              - a description such as "UniformAGAGE" (uniform values based on AGAGE average)
            domain: Region for boundary conditions
            date: 
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
        import re
        from collections import defaultdict
        from xarray import open_dataset
        from openghg.store import assign_data
        from openghg.util import (
            clean_string,
            hash_file,
            pairwise,
            timestamp_tzaware,
            timestamp_now,
            parse_period,
            create_frequency_str,
            relative_time_offset,

        )

        species = clean_string(species)
        bc_input = clean_string(bc_input)
        domain = clean_string(domain)
        # date = clean_string(date)

        filepath = Path(filepath)

        bc_store = BoundaryConditions.load()

        file_hash = hash_file(filepath=filepath)
        if file_hash in bc_store._file_hashes and not overwrite:
            print(
                f"This file has been uploaded previously with the filename : {bc_store._file_hashes[file_hash]} - skipping."
            )

        bc_data = open_dataset(filepath)

        # Some attributes are numpy types we can't serialise to JSON so convert them
        # to their native types here
        attrs = {}
        for key, value in bc_data.attrs.items():
            try:
                attrs[key] = value.item()
            except AttributeError:
                attrs[key] = value

        author_name = "OpenGHG Cloud"
        bc_data.attrs["author"] = author_name

        metadata = {}
        metadata.update(attrs)

        metadata["species"] = species
        metadata["domain"] = domain
        metadata["boundary_condition_input"] = bc_input
        # metadata["date"] = date
        metadata["author"] = author_name
        metadata["processed"] = str(timestamp_now())

        # TODO: Decide what format (if any) we want the "date" input as?
        # Surely this can be inferred from the time axis within the data?
        # It's the period which would be useful as an input here

        # Currently ACRG boundary conditions are split by month or year
        # Need to add in code to handle this below.
        #  - Could do this based on filename? This is linked to stored data for current ACRG data.
        bc_time = bc_data.time
        n_dates = bc_time.size
        start_date = timestamp_tzaware(bc_time.values[0])

        # Usual filename format: "species"_"domain"_"date".nc

        # Find frequency from period, if specified
        if period is not None:
            freq = parse_period(period)
        else:
            freq = None

        if n_dates == 1:
            
            filename = filepath.stem  # Filename without the extension
            filename_identifiers = filename.split("_")
            filename_identifiers.reverse()  # Date identifier usually at the end

            for id in filename_identifiers:
                try:
                    # Check if filename contains 6 ("yyyymm") or 4 ("yyyy") digit section
                    date_match = re.search("^(\d{6}|\d{4})$", id).group()
                except AttributeError:
                    continue
                else:
                    break
            else:
                date_match = ""

            if len(date_match) == 6:
                # "yyyymm" format indicates monthly data
                inferred_freq = "months"
            elif len(date_match) == 4:
                # "yyyy" format indicates yearly data
                inferred_freq = "years"
            else:
                # Set as default as annual if this cannot be inferred from filename
                inferred_freq = "years"

            # Because frequency cannot be inferred from the data and only the filename,
            # use the user specified input in preference of the inferred value
            if freq is not None:
                time_value = freq[0]
                time_unit = freq[1]
            else:
                print(f"Only one time point, inferring frequency of {inferred_freq}")
                time_value = 1
                time_unit = inferred_freq

            # Check input period against inferred period
            if inferred_freq != time_unit:
                print(f"Warning: Input period of {period} did not map to frequency inferred from filename: {inferred_freq} (date extracted: {date_match})")

            # Create time offset and use to create start and end datetime
            time_delta = relative_time_offset(unit=time_unit, value=time_value)
            start_date = timestamp_tzaware(bc_data.time[0].values)
            end_date = start_date + time_delta

            # TODO: Aim to remove if date isn't needed as a key
            if time_unit == "years":
                date = str(start_date.year)
            elif time_unit == "months":
                date = str(start_date.year) + f"{start_date.month:02}"
            else:
                date = start_date.astype("datetime64[s]").astype(str)

            period_str = create_frequency_str(time_value, time_unit)

        else:
            timestamps = pd.to_datetime([timestamp_tzaware(t) for t in bc_time.values])
            timestamps = timestamps.sort_values()

            inferred_period = pd.infer_freq(timestamps)
            if inferred_period is None:
                if continuous:
                    raise ValueError("Continuous data with no gaps is expected but no time period can be inferred. Run with continous=False to remove this constraint.")
                else:
                    inferred_freq = ()
                    time_value, time_unit = None, None
            else:
                inferred_freq = parse_period(inferred_period)
                time_value, time_unit = inferred_freq

            # Because frequency will be inferred from the data, use the inferred
            # value in preference to any user specified input.
            # Note: this is opposite to the other part of this branch.
            if freq is not None:
                if inferred_freq and freq != inferred_freq:
                    print(f"Warning: Input period: {period} does not map to inferred frequency {inferred_freq}")
                    freq = inferred_freq

            # Create time offset, using inferred offset
            start_date = timestamp_tzaware(bc_data.time[0].values)
            if time_value is not None:
                time_delta = DateOffset(**{time_unit:time_value})
                end_date = timestamp_tzaware(bc_data.time[-1].values) + time_delta
            else:
                end_date = timestamp_tzaware(bc_data.time[-1].values)

            # TODO: Aim to remove if date isn't needed as a key
            date = str(start_date.year)
            # date = start_date.astype("datetime64[s]").astype(str)

            if inferred_period is not None:
                period_str = create_frequency_str(time_value, time_unit)
            else:
                period_str = "varies"

        # TODO: Add checking against expected format for boundary conditions
        # Will probably want to do this for Emissions, Footprints as well
        # - develop and use check_format() method

        metadata["start_date"] = str(start_date)
        metadata["end_date"] = str(end_date)

        metadata["max_longitude"] = round(float(bc_data["lon"].max()), 5)
        metadata["min_longitude"] = round(float(bc_data["lon"].min()), 5)
        metadata["max_latitude"] = round(float(bc_data["lat"].max()), 5)
        metadata["min_latitude"] = round(float(bc_data["lat"].min()), 5)
        metadata["min_height"] = round(float(bc_data["height"].min()), 5)
        metadata["max_height"] = round(float(bc_data["height"].max()), 5)

        metadata["time_period"] = period_str
        metadata["date"] = date

        # TODO: Remove reliance on date in the key - this should be dynamically
        # split out as the obs data is.
        key = "_".join((species, bc_input, domain, date))

        boundary_conditions_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
        boundary_conditions_data[key]["data"] = bc_data
        boundary_conditions_data[key]["metadata"] = metadata

        keyed_metadata = {key: metadata}

        lookup_results = bc_store.datasource_lookup(metadata=keyed_metadata)

        data_type = "boundary_conditions"
        datasource_uuids = assign_data(
            data_dict=boundary_conditions_data,
            lookup_results=lookup_results,
            overwrite=overwrite,
            data_type=data_type,
        )

        bc_store.add_datasources(datasource_uuids=datasource_uuids, metadata=keyed_metadata)

        # Record the file hash in case we see this file again
        bc_store._file_hashes[file_hash] = filepath.name

        bc_store.save()

        return datasource_uuids

    def lookup_uuid(self, species: str, bc_input: str, domain: str, date: str) -> Union[str, bool]:
        """Perform a lookup for the UUID of a Datasource

        Args:
            species: Site code
            bc_input: Input identifier for boundary conditions
            domain: Domain
            date: Date of original file
        Returns:
            str or dict: UUID or False if no entry
        """
        uuid = self._datasource_table[species][bc_input][domain][date]

        return uuid if uuid else False

    def set_uuid(self, species: str, bc_input: str, domain: str, date: str, uuid: str) -> None:
        """Record a UUID of a Datasource in the datasource table

        Args:
            species: Site code
            bc_input: Input identifier for boundary conditions
            domain: Domain
            date: Date of original file
            uuid: UUID of Datasource
        Returns:
            None
        """
        self._datasource_table[species][bc_input][domain][date] = uuid

    def datasource_lookup(self, metadata: Dict) -> Dict[str, Union[str, bool]]:
        """Find the Datasource we should assign the data to

        Args:
            metadata: Dictionary of metadata
        Returns:
            dict: Dictionary of datasource information
        """
        # TODO - I'll leave this as a function for now as the way we read emissions may
        # change in the near future
        # GJ - 2021-04-20
        lookup_results = {}

        for key, data in metadata.items():
            species = data["species"]
            bc_input = data["boundary_condition_input"]
            domain = data["domain"]
            date = data["date"]

            lookup_results[key] = self.lookup_uuid(species=species, bc_input=bc_input, domain=domain, date=date)

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
            species = md["species"]
            bc_input = md["boundary_condition_input"]
            domain = md["domain"]
            date = md["date"]

            result = self.lookup_uuid(species=species, bc_input=bc_input, domain=domain, date=date)

            if result and result != uid:
                raise ValueError("Mismatch between assigned uuid and stored Datasource uuid.")
            else:
                self.set_uuid(species=species, bc_input=bc_input, domain=domain, date=date, uuid=uid)
                self._datasource_uuids[uid] = key

    def check_format(self):
        # TODO: Create check_format() function to define and align format to
        # expected values within database
        pass