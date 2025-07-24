from __future__ import annotations
import logging
from typing import cast
import warnings
import numpy as np

from openghg.store import DataSchema
from openghg.store.base import BaseStore
from openghg.store.storage import ChunkingSchema
from openghg.util import check_species_lifetime, check_species_time_resolved, synonyms

__all__ = ["Footprints"]

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


class Footprints(BaseStore):
    """This class is used to process footprints model output"""

    _data_type = "footprints"
    _root = "Footprints"
    _uuid = "62db5bdf-c88d-4e56-97f4-40336d37f18c"
    _metakey = f"{_root}/uuid/{_uuid}/metastore"

    def read_data(self, binary_data: bytes, metadata: dict, file_metadata: dict) -> list[dict] | None:
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

    def format_inputs(self, **kwargs) -> tuple[dict, dict]:
        """ """
        from openghg.util import (
            clean_string,
            format_inlet,
            check_and_set_null_variable,
        )

        # Apply clean_string first and then any specifics?
        # How do we check the keys we're expecting for this? Rely on required keys?

        # Specify any additional metadata to be added
        additional_metadata = {}

        params = kwargs.copy()

        if params.get("high_time_resolution") is not None:
            warnings.warn(
                "This argument is deprecated and will be replaced in future versions with time_resolved.",
                DeprecationWarning,
            )
            params["time_resolved"] = params["high_time_resolution"]
            params.pop("high_time_resolution")

        if params.get("site") is not None:
            params["site"] = clean_string(params["site"])
        elif params.get("satellite") is not None and params.get("obs_region") is not None:
            params["satellite"] = clean_string(params["satellite"])
            params["obs_region"] = clean_string(params["obs_region"])
            params["continuous"] = False
            logger.info("For satellite data, 'continuous' is set to `False`")
        else:
            raise ValueError("Please pass either site or satellite and obs_region values")

        params["network"] = clean_string(params["network"])
        params["domain"] = clean_string(params["domain"])

        # Make sure `inlet` OR the alias `height` is included
        # Note: from this point only `inlet` variable should be used.
        inlet = params.get("inlet")
        if inlet is None and params.get("height") is None:
            raise ValueError("One of inlet (or height) must be specified as an input")
        elif inlet is None:
            inlet = params["height"]
            params.pop("height")

        # Try to ensure inlet is 'NUM''UNIT' e.g. "10m"
        inlet = clean_string(inlet)
        params["inlet"] = format_inlet(inlet)
        params["inlet"] = cast(str, params["inlet"])

        # Ensure we have a value for species
        if params.get("species") is None:
            species = "inert"
        else:
            species = clean_string(params["species"])
            species = synonyms(species)
        params["species"] = species

        # Ensure we have a clear missing value for met_model
        met_model = params.get("met_model")
        params["met_model"] = check_and_set_null_variable(met_model)
        params["met_model"] = clean_string(params["met_model"])

        if params.get("network") is not None:
            params["network"] = clean_string(params["network"])

        # Do some housekeeping on the inputs
        time_resolved = params.get("time_resolved", False)
        short_lifetime = params.get("short_lifetime", False)
        params["time_resolved"] = check_species_time_resolved(species, time_resolved)
        params["short_lifetime"] = check_species_lifetime(species, short_lifetime)

        if params["time_resolved"] and params.get("sort") is True:
            logger.info(
                "Sorting high time resolution data is very memory intensive, we recommend not sorting."
            )

        return params, additional_metadata

    @staticmethod
    def schema(
        particle_locations: bool = True,
        high_spatial_resolution: bool = False,
        time_resolved: bool = False,
        high_time_resolution: bool = False,
        short_lifetime: bool = False,
        source_format: str | None = None,
    ) -> DataSchema:
        """
        Define schema for footprint Dataset.

        The returned schema depends on what the footprint represents,
        indicated using the keywords.
        By default, this will include "fp" variable but this will be superceded
        if high_spatial_resolution or time_resolved are specified.

        Args:
            particle_locations: Include 4-directional particle location variables:
                - "particle_location_[nesw]"
                and include associated additional dimensions ("height")
            high_spatial_resolution : Set footprint variables include high and low resolution options:
                - "fp_low"
                - "fp_high"
                and include associated additional dimensions ("lat_high", "lon_high").
            time_resolved: Set footprint variable to be high time resolution
                - "fp_HiTRes"
                and include associated dimensions ("H_back").
            high_time_resolution: This argument is deprecated and will be replaced in future versions with time_resolved.
            short_lifetime: Include additional particle age parameters for short lived species:
                - "mean_age_particles_[nesw]"
            source_format: optional string containing source format; necessary for "time resolved" footprints since the
                the schema is different for PARIS/FLEXPART and ACRG formats.

        Returns:
            DataSchema object describing this format.

        Note: In PARIS format the coordinate dimensions are ("latitude", "longitude") rather than ("lat", "lon")
            but given that all other openghg internal formats are ("lat", "lon"), we are currently keeping all
            footprint internal formats consistent with this.
        """

        # # Note: In PARIS format the coordinate dimensions are ("latitude", "longitude") rather than ("lat", "lon")
        # # but given that all other openghg internal formats are ("lat", "lon"), we are currently keeping the
        # # footprint internal format consistent with this.

        # Names of data variables and associated dimensions (as a tuple)
        data_vars: dict[str, tuple[str, ...]] = {}
        # Internal data types of data variables and coordinates
        dtypes = {
            "lat": np.floating,  # Covers np.float16, np.float32, np.float64 types
            "lon": np.floating,
            "time": np.datetime64,
        }

        if high_time_resolution:
            warnings.warn(
                "This argument is deprecated and will be replaced in future versions with time_resolved.",
                DeprecationWarning,
            )
            time_resolved = high_time_resolution

        if not time_resolved and not high_spatial_resolution:
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

        if time_resolved:
            # Include options for high time resolution footprint (usually co2)
            # This includes a footprint data with an additional hourly back dimension
            if source_format in ("PARIS", "FLEXPART"):
                data_vars["fp_time_resolved"] = ("time", "lat", "lon", "H_back")
                data_vars["fp_residual"] = ("time", "lat", "lon")
                dtypes["fp_time_resolved"] = np.floating
                dtypes["fp_residual"] = np.floating
            else:
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
        # "air_temperature", "air_pressure", "wind_speed", "wind_from_direction",
        # "atmosphere_boundary_layer_thickness", "release_lon", "release_lat"

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

    def chunking_schema(
        self,
        time_resolved: bool = False,
        high_time_resolution: bool = False,
        high_spatial_resolution: bool = False,
        short_lifetime: bool = False,
        source_format: str = "",
    ) -> ChunkingSchema:
        """
        Get chunking schema for footprint data.

        Args:
            time_resolved : Set footprint variable to be high time resolution.
            high_time_resolution: This argument is deprecated and will be replaced in future versions with time_resolved.
            high_spatial_resolution : Set footprint variables include high and low resolution options.
            short_lifetime: Include additional particle age parameters for short lived species.
        Returns:
            dict: Chunking schema for footprint data.
        """
        if high_spatial_resolution or short_lifetime:
            raise NotImplementedError(
                "Chunking schema for footprints with high spatial resolution or short lifetime is not currently set.\n"
                + "Using the default chunking schema."
            )

        # TODO - could these defaults be changed in the object store config maybe?

        if high_time_resolution:
            warnings.warn(
                "This argument is deprecated and will be replaced in future versions with time_resolved.",
                DeprecationWarning,
            )
            time_resolved = high_time_resolution
        if time_resolved:
            var = "fp_HiTRes" if source_format.upper() not in ("PARIS", "FLEXPART") else "fp_time_resolved"
            time_chunk_size = 24
            secondary_vars = ["lat", "lon", "H_back"]
        else:
            var = "fp"
            time_chunk_size = 480
            secondary_vars = ["lat", "lon"]

        return ChunkingSchema(variable=var, chunks={"time": time_chunk_size}, secondary_dims=secondary_vars)
