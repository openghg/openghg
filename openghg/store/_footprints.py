from __future__ import annotations
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, DefaultDict, Dict, Literal, List, Optional, Tuple, Union, cast
import numpy as np
from openghg.store import DataSchema
from openghg.store.base import BaseStore
from xarray import Dataset

__all__ = ["Footprints"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class Footprints(BaseStore):
    """This class is used to process footprints model output"""

    _data_type = "footprints"
    _root = "Footprints"
    _uuid = "62db5bdf-c88d-4e56-97f4-40336d37f18c"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def read_data(self, binary_data: bytes, metadata: Dict, file_metadata: Dict) -> Optional[Dict]:
        """Ready a footprint from binary data

        Args:
            binary_data: Footprint data
            metadata: Dictionary of metadata
            file_metadat: File metadata
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        raise NotImplementedError("This branch doesn't currently support cloud.")
        # with TemporaryDirectory() as tmpdir:
        #     tmpdir_path = Path(tmpdir)

        #     try:
        #         filename = file_metadata["filename"]
        #     except KeyError:
        #         raise KeyError("We require a filename key for metadata read.")

        #     filepath = tmpdir_path.joinpath(filename)
        #     filepath.write_bytes(binary_data)

        #     return Footprints.read_file(filepath=filepath, **metadata)

    # @staticmethod
    # def read_data(binary_data: bytes, metadata: Dict, file_metadata: Dict) -> Dict:
    #     """Ready a footprint from binary data

    #     Args:
    #         binary_data: Footprint data
    #         metadata: Dictionary of metadata
    #         file_metadat: File metadata
    #     Returns:
    #         dict: UUIDs of Datasources data has been assigned to
    #     """
    #     from openghg.store import assign_data, infer_date_range, load_metastore, datasource_lookup

    #     fp = Footprints.load()

    #     # Load in the metadata store
    #     metastore = load_metastore(key=fp._metakey)

    #     sha1_hash = file_metadata["sha1_hash"]
    #     overwrite = metadata.get("overwrite", False)

    #     if sha1_hash in fp._file_hashes and not overwrite:
    #         print(
    #             f"This file has been uploaded previously with the filename : {fp._file_hashes[sha1_hash]} - skipping."
    #         )

    #     data_buf = BytesIO(binary_data)
    #     fp_data = open_dataset(data_buf)

    #     fp_time = fp_data.time
    #     period = metadata.get("period")
    #     continuous = metadata["continous"]
    #     high_spatial_res = metadata["high_spatial_res"]
    #     species = metadata["species"]
    #     filename = file_metadata["filename"]

    #     site = metadata["site"]
    #     domain = metadata["domain"]
    #     model = metadata["model"]
    #     height = metadata["height"]

    #     start_date, end_date, period_str = infer_date_range(
    #         fp_time, filepath=filename, period=period, continuous=continuous
    #     )

    #     metadata["start_date"] = str(start_date)
    #     metadata["end_date"] = str(end_date)
    #     metadata["time_period"] = period_str

    #     metadata["max_longitude"] = round(float(fp_data["lon"].max()), 5)
    #     metadata["min_longitude"] = round(float(fp_data["lon"].min()), 5)
    #     metadata["max_latitude"] = round(float(fp_data["lat"].max()), 5)
    #     metadata["min_latitude"] = round(float(fp_data["lat"].min()), 5)

    #     # TODO: Pull out links to underlying data format into a separate format function
    #     #  - high_spatial_res - data vars - "fp_low", "fp_high", coords - "lat_high", "lon_high"
    #     #  - high_time_res - data vars - "fp_HiTRes", coords - "H_back"

    #     metadata["spatial_resolution"] = "standard_spatial_resolution"

    #     if high_spatial_res:
    #         try:
    #             metadata["max_longitude_high"] = round(float(fp_data["lon_high"].max()), 5)
    #             metadata["min_longitude_high"] = round(float(fp_data["lon_high"].min()), 5)
    #             metadata["max_latitude_high"] = round(float(fp_data["lat_high"].max()), 5)
    #             metadata["min_latitude_high"] = round(float(fp_data["lat_high"].min()), 5)

    #             metadata["spatial_resolution"] = "high_spatial_resolution"
    #         except KeyError:
    #             raise KeyError("Expected high spatial resolution. Unable to find lat_high or lon_high data.")

    #     if species == "co2":
    #         # Expect co2 data to have high time resolution
    #         high_time_res = True

    #     metadata["time_resolution"] = "standard_time_resolution"

    #     if high_time_res:
    #         if "fp_HiTRes" in fp_data:
    #             metadata["time_resolution"] = "high_time_resolution"
    #         else:
    #             raise KeyError("Expected high time resolution. Unable to find fp_HiTRes data.")

    #     metadata["heights"] = [float(h) for h in fp_data.height.values]
    #     # Do we also need to save all the variables we have available in this footprints?
    #     metadata["variables"] = list(fp_data.keys())

    #     # if model_params is not None:
    #     #     metadata["model_parameters"] = model_params

    #     # Set the attributes of this Dataset
    #     fp_data.attrs = {"author": "OpenGHG Cloud", "processed": str(timestamp_now())}

    #     # This might seem longwinded now but will help when we want to read
    #     # more than one footprints at a time
    #     key = "_".join((site, domain, model, height))

    #     footprint_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
    #     footprint_data[key]["data"] = fp_data
    #     footprint_data[key]["metadata"] = metadata

    #     # These are the keys we will take from the metadata to search the
    #     # metadata store for a Datasource, they should provide as much detail as possible
    #     # to uniquely identify a Datasource
    #     required = ("site", "model", "height", "domain")
    #     lookup_results = datasource_lookup(metastore=metastore, data=footprint_data, required_keys=required)

    #     data_type = "footprints"
    #     datasource_uuids: Dict[str, Dict] = assign_data(
    #         data_dict=footprint_data,
    #         lookup_results=lookup_results,
    #         overwrite=overwrite,
    #         data_type=data_type,
    #     )

    #     fp.add_datasources(uuids=datasource_uuids, data=footprint_data, metastore=metastore)

    #     # Record the file hash in case we see this file again
    #     fp._file_hashes[sha1_hash] = filename

    #     fp.save()

    #     metastore.close()

    #     return datasource_uuids

    # def _store_data(data: Dataset, metadata: Dict):
    #     """ Takes an xarray Dataset

    #     Args:
    #         data: xarray Dataset
    #         metadata: Metadata dict
    #     Returns:

    #     """

    def read_file(
        self,
        filepath: Union[List, str, Path],
        site: str,
        domain: str,
        model: str,
        inlet: Optional[str] = None,
        height: Optional[str] = None,
        metmodel: Optional[str] = None,
        species: Optional[str] = None,
        network: Optional[str] = None,
        period: Optional[Union[str, tuple]] = None,
        chunks: Union[int, Dict, Literal["auto"], None] = None,
        continuous: bool = True,
        retrieve_met: bool = False,
        high_spatial_resolution: bool = False,
        high_time_resolution: bool = False,
        short_lifetime: bool = False,
        if_exists: str = "auto",
        save_current: str = "auto",
        overwrite: bool = False,
        force: bool = False,
        sort: bool = False,
        drop_duplicates: bool = False,
        compression: bool = True,
        compressor: Optional[Any] = None,
        filters: Optional[Any] = None,
    ) -> dict:
        """Reads footprints data files and returns the UUIDS of the Datasources
        the processed data has been assigned to

        Args:
            filepath: Path(s) of file(s) to standardise
            site: Site name
            domain: Domain of footprints
            model: Model used to create footprint (e.g. NAME or FLEXPART)
            inlet: Height above ground level in metres. Format 'NUMUNIT' e.g. "10m"
            height: Alias for inlet. One of height or inlet MUST be included.
            metmodel: Underlying meteorlogical model used (e.g. UKV)
            species: Species name. Only needed if footprint is for a specific species e.g. co2 (and not inert)
            network: Network name
            period: Period of measurements. Only needed if this can not be inferred from the time coords
            continuous: Whether time stamps have to be continuous.
            retrieve_met: Whether to also download meterological data for this footprints area
            high_spatial_resolution : Indicate footprints include both a low and high spatial resolution.
            high_time_resolution: Indicate footprints are high time resolution (include H_back dimension)
                           Note this will be set to True automatically if species="co2" (Carbon Dioxide).
            short_lifetime: Indicate footprint is for a short-lived species. Needs species input.
                            Note this will be set to True if species has an associated lifetime.
            if_exists: What to do if existing data is present.
                - "auto" - checks new and current data for timeseries overlap
                   - adds data if no overlap
                   - raises DataOverlapError if there is an overlap
                - "new" - just include new data and ignore previous
                - "combine" - replace and insert new data into current timeseries
            save_current: Whether to save data in current form and create a new version.
                - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
                - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
                - "n" / "no" - Allow current data to updated / deleted
            overwrite: Deprecated. This will use options for if_exists="new".
            force: Force adding of data even if this is identical to data stored.
            sort: Sort data in time dimension. We recommend NOT sorting footprint data unless necessary.
            drop_duplicates: Drop duplicate timestamps, keeping the first value
            compression: Enable compression, we recommend enabling compression
            compressor: A custom compressor to use. If None, this will default to
            `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
            See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
            filters: Filters to apply to the data on storage, this defaults to no filtering. See
            https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        import xarray as xr
        from openghg.store import (
            infer_date_range,
            update_zero_dim,
        )

        from openghg.util import (
            clean_string,
            format_inlet,
            hash_file,
            species_lifetime,
            timestamp_now,
            check_if_need_new_version,
        )

        if not isinstance(filepath, list):
            filepath = [filepath]

        # We wanted sorted Path objects
        filepath = sorted([Path(f) for f in filepath])

        site = clean_string(site)
        network = clean_string(network)
        domain = clean_string(domain)

        # Make sure `inlet` OR the alias `height` is included
        # Note: from this point only `inlet` variable should be used.
        if inlet is None and height is None:
            raise ValueError("One of inlet (or height) must be specified as an input")
        elif inlet is None:
            inlet = height

        # Try to ensure inlet is 'NUM''UNIT' e.g. "10m"
        inlet = clean_string(inlet)
        inlet = format_inlet(inlet)
        inlet = cast(str, inlet)

        if overwrite and if_exists == "auto":
            logger.warning(
                "Overwrite flag is deprecated in preference to `if_exists` (and `save_current`) inputs."
                "See documentation for details of these inputs and options."
            )
            if_exists = "new"

        # Making sure new version will be created by default if force keyword is included.
        if force and if_exists == "auto":
            if_exists = "new"

        new_version = check_if_need_new_version(if_exists, save_current)

        # Save the hashes so we don't have to compute them again
        new_files_hashes = {}
        for f in filepath:
            file_hash = hash_file(filepath=f)
            if file_hash in self._file_hashes and not force:
                logger.warning(
                    f"This file has been uploaded previously with the filename : {self._file_hashes[file_hash]} - skipping.\n"
                    "If necessary, use force=True to bypass this to add this data."
                )
                return {}

            new_files_hashes[file_hash] = f.name

        if chunks is None:
            chunks = {}

        # TODO - this needs some tidying once we decide on how to chunk things without errors constantly
        if len(filepath) > 1:
            xr_open_fn: Callable = xr.open_mfdataset
            logger.warning(
                "Opening a number of footprints as a single Dataset is currently an experimental feature "
                + "and may result in chunking errors, slow operation or high memory usage."
            )
        else:
            xr_open_fn = xr.open_dataset
            filepath = filepath[0]

        # This accepts both single and multiple files
        # Using open_mfdataset handles chunks different so we have this setup
        with xr_open_fn(filepath).reset_encoding().chunk(chunks) as fp_data:
            if chunks:
                logger.info(f"Rechunking with chunks={chunks}")

            if species == "co2":
                # Expect co2 data to have high time resolution
                if not high_time_resolution:
                    logger.info("Updating high_time_resolution to True for co2 data")
                    high_time_resolution = True

                if sort:
                    logger.info(
                        "Sorting high time resolution data is very memory intensive, we recommend not sorting."
                    )

            if short_lifetime and not species:
                raise ValueError(
                    "When indicating footprint is for short lived species, 'species' input must be included"
                )
            elif not short_lifetime and species:
                lifetime = species_lifetime(species)
                if lifetime is not None:
                    # TODO: May want to add a check on length of lifetime here
                    logger.info("Updating short_lifetime to True since species has an associated lifetime")
                    short_lifetime = True

            # Checking against expected format for footprints
            # Based on configuration (some user defined, some inferred)
            Footprints.validate_data(
                fp_data,
                high_spatial_resolution=high_spatial_resolution,
                high_time_resolution=high_time_resolution,
                short_lifetime=short_lifetime,
            )

            # Need to read the metadata from the footprints and then store it
            # Do we need to chunk the footprints / will a Datasource store it correctly?
            metadata: Dict[str, Union[str, float, List[float]]] = {}

            metadata["data_type"] = "footprints"
            metadata["site"] = site
            metadata["domain"] = domain
            metadata["model"] = model

            # Include both inlet and height keywords for backwards compatability
            metadata["inlet"] = inlet
            metadata["height"] = inlet

            if species is not None:
                metadata["species"] = clean_string(species)

            if network is not None:
                metadata["network"] = clean_string(network)

            if metmodel is not None:
                metadata["metmodel"] = clean_string(metmodel)

            # Check if time has 0-dimensions and, if so, expand this so time is 1D
            if "time" in fp_data.coords:
                fp_data = update_zero_dim(fp_data, dim="time")

            fp_time = fp_data["time"]

            # TODO - fix this setup
            filepath = cast(Path, filepath)
            start_date, end_date, period_str = infer_date_range(
                fp_time, filepath=filepath, period=period, continuous=continuous
            )

            metadata["start_date"] = str(start_date)
            metadata["end_date"] = str(end_date)
            metadata["time_period"] = period_str

            metadata["max_longitude"] = round(float(fp_data["lon"].max()), 5)
            metadata["min_longitude"] = round(float(fp_data["lon"].min()), 5)
            metadata["max_latitude"] = round(float(fp_data["lat"].max()), 5)
            metadata["min_latitude"] = round(float(fp_data["lat"].min()), 5)

            if high_spatial_resolution:
                try:
                    metadata["max_longitude_high"] = round(float(fp_data["lon_high"].max()), 5)
                    metadata["min_longitude_high"] = round(float(fp_data["lon_high"].min()), 5)
                    metadata["max_latitude_high"] = round(float(fp_data["lat_high"].max()), 5)
                    metadata["min_latitude_high"] = round(float(fp_data["lat_high"].min()), 5)

                except KeyError:
                    raise KeyError(
                        "Expected high spatial resolution. Unable to find lat_high or lon_high data."
                    )

            metadata["high_time_resolution"] = high_time_resolution
            metadata["high_spatial_resolution"] = high_spatial_resolution
            metadata["short_lifetime"] = short_lifetime

            metadata["heights"] = [float(h) for h in fp_data.height.values]
            # Do we also need to save all the variables we have available in this footprints?
            metadata["variables"] = list(fp_data.data_vars)

            # if model_params is not None:
            #     metadata["model_parameters"] = model_params

            # Set the attributes of this Dataset
            fp_data.attrs = {"author": "OpenGHG Cloud", "processed": str(timestamp_now())}

            # This might seem longwinded now but will help when we want to read
            # more than one footprints at a time
            # TODO - remove this once assign_attributes has been refactored
            key = "_".join((site, domain, model, inlet))

            footprint_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
            footprint_data[key]["data"] = fp_data
            footprint_data[key]["metadata"] = metadata

            # These are the keys we will take from the metadata to search the
            # metadata store for a Datasource, they should provide as much detail as possible
            # to uniquely identify a Datasource
            required = (
                "site",
                "model",
                "inlet",
                "domain",
                "high_time_resolution",
                "high_spatial_resolution",
                "short_lifetime",
            )

            data_type = "footprints"
            # TODO - filter options
            datasource_uuids = self.assign_data(
                data=footprint_data,
                if_exists=if_exists,
                new_version=new_version,
                data_type=data_type,
                required_keys=required,
                sort=sort,
                drop_duplicates=drop_duplicates,
                compressor=compressor,
                filters=filters,
            )

            # TODO: MAY NEED TO ADD BACK IN OR CAN DELETE
            # update_keys = ["start_date", "end_date", "latest_version"]
            # footprint_data = update_metadata(
            #     data_dict=footprint_data, uuid_dict=datasource_uuids, update_keys=update_keys
            # )

            # Record the file hash in case we see the file(s) again
            self._file_hashes.update(new_files_hashes)

            return datasource_uuids

    @staticmethod
    def schema(
        particle_locations: bool = True,
        high_spatial_resolution: bool = False,
        high_time_resolution: bool = False,
        short_lifetime: bool = False,
    ) -> DataSchema:
        """
        Define schema for footprint Dataset.

        The returned schema depends on what the footprint represents,
        indicated using the keywords.
        By default, this will include "fp" variable but this will be superceded
        if high_spatial_resolution or high_time_resolution are specified.

        Args:
            particle_locations: Include 4-directional particle location variables:
                - "particle_location_[nesw]"
                and include associated additional dimensions ("height")
            high_spatial_resolution : Set footprint variables include high and low resolution options:
                - "fp_low"
                - "fp_high"
                and include associated additional dimensions ("lat_high", "lon_high").
            high_time_resolution : Set footprint variable to be high time resolution
                - "fp_HiTRes"
                and include associated dimensions ("H_back").
            short_lifetime: Include additional particle age parameters for short lived species:
                - "mean_age_particles_[nesw]"
        """

        # Names of data variables and associated dimensions (as a tuple)
        data_vars: Dict[str, Tuple[str, ...]] = {}
        # Internal data types of data variables and coordinates
        dtypes = {
            "lat": np.floating,  # Covers np.float16, np.float32, np.float64 types
            "lon": np.floating,
            "time": np.datetime64,
        }

        if not high_time_resolution and not high_spatial_resolution:
            # Includes standard footprint variable
            data_vars["fp"] = ("time", "lat", "lon")
            dtypes["fp"] = np.floating

        if high_spatial_resolution:
            # Include options for high spatial resolution footprint
            # This includes footprint data on multiple resolutions

            data_vars["fp_low"] = ("time", "lat", "lon")
            data_vars["fp_high"] = ("time", "lat_high", "lon_high")

            dtypes["fp_low"] = np.floating
            dtypes["fp_high"] = np.floating

        if high_time_resolution:
            # Include options for high time resolution footprint (usually co2)
            # This includes a footprint data with an additional hourly back dimension
            data_vars["fp_HiTRes"] = ("time", "lat", "lon", "H_back")
            dtypes["fp_HiTRes"] = np.floating
            dtypes["H_back"] = np.number  # float or integer

        # Includes particle location directions - one for each regional boundary
        if particle_locations:
            data_vars["particle_locations_n"] = ("time", "lon", "height")
            data_vars["particle_locations_e"] = ("time", "lat", "height")
            data_vars["particle_locations_s"] = ("time", "lon", "height")
            data_vars["particle_locations_w"] = ("time", "lat", "height")

            dtypes["height"] = np.floating
            dtypes["particle_locations_n"] = np.floating
            dtypes["particle_locations_e"] = np.floating
            dtypes["particle_locations_s"] = np.floating
            dtypes["particle_locations_w"] = np.floating

        # TODO: Could also add check for meteorological + other data
        # "pressure", "wind_speed", "wind_direction", "PBLH"
        # "release_lon", "release_lat"

        # Include options for short lifetime footprints (short-lived species)
        # This includes additional particle ages (allow calculation of decay based on particle lifetimes)
        if short_lifetime:
            data_vars["mean_age_particles_n"] = ("time", "lon", "height")
            data_vars["mean_age_particles_e"] = ("time", "lat", "height")
            data_vars["mean_age_particles_s"] = ("time", "lon", "height")
            data_vars["mean_age_particles_w"] = ("time", "lat", "height")

            dtypes["mean_age_particles_n"] = np.floating
            dtypes["mean_age_particles_e"] = np.floating
            dtypes["mean_age_particles_s"] = np.floating
            dtypes["mean_age_particles_w"] = np.floating

        data_format = DataSchema(data_vars=data_vars, dtypes=dtypes)

        return data_format

    @staticmethod
    def validate_data(
        data: Dataset,
        particle_locations: bool = True,
        high_spatial_resolution: bool = False,
        high_time_resolution: bool = False,
        short_lifetime: bool = False,
    ) -> None:
        """
        Validate data against Footprint schema - definition from
        Footprints.schema(...) method.

        Args:
            data : xarray Dataset in expected format

            See Footprints.schema() method for details on optional inputs.

        Returns:
            None

            Raises a ValueError with details if the input data does not adhere
            to the Footprints schema.
        """
        data_schema = Footprints.schema(
            particle_locations=particle_locations,
            high_spatial_resolution=high_spatial_resolution,
            high_time_resolution=high_time_resolution,
            short_lifetime=short_lifetime,
        )
        data_schema.validate_data(data)
