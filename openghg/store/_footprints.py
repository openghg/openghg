from __future__ import annotations
import logging
from pathlib import Path
from typing import Dict, Literal, Optional, Tuple, Union, Any, cast
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
        filepath: Union[str, Path],
        site: str,
        domain: str,
        model: str,
        inlet: Optional[str] = None,
        height: Optional[str] = None,
        metmodel: Optional[str] = None,
        species: Optional[str] = None,
        network: Optional[str] = None,
        period: Optional[Union[str, tuple]] = None,
        continuous: bool = True,
        chunks: Union[int, Dict, Literal["auto"], None] = None,
        source_format: str = "acrg_org",
        retrieve_met: bool = False,
        high_spatial_resolution: bool = False,
        high_time_resolution: bool = False,
        short_lifetime: bool = False,
        overwrite: bool = False,
        # model_params: Optional[Dict] = None,
    ) -> dict:
        """Reads footprints data files and returns the UUIDS of the Datasources
        the processed data has been assigned to

        Args:
            filepath: Path of file to load
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
            chunks: Chunk size to use when parsing NetCDF, useful for large datasets.
                Passing "auto" will ask xarray to calculate a chunk size.
            source_format : Type of data being input e.g. acrg_org
            retrieve_met: Whether to also download meterological data for this footprints area
            high_spatial_resolution : Indicate footprints include both a low and high spatial resolution.
            high_time_resolution: Indicate footprints are high time resolution (include H_back dimension)
                           Note this will be set to True automatically if species="co2" (Carbon Dioxide).
            short_lifetime: Indicate footprint is for a short-lived species. Needs species input.
                            Note this will be set to True if species has an associated lifetime.
            overwrite: Overwrite any currently stored data
        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        from openghg.types import FootprintTypes
        from openghg.util import clean_string, format_inlet, hash_file, load_footprint_parser

        filepath = Path(filepath)

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

        if species is not None:
            species = clean_string(species)

        if network is not None:
            network = clean_string(network)

        if metmodel is not None:
            metmodel = clean_string(metmodel)

        try:
            source_format = FootprintTypes[source_format.upper()].value
        except KeyError:
            raise ValueError(f"Unknown data type {source_format} selected.")

        # Load the data retrieve object
        parser_fn = load_footprint_parser(source_format=source_format)

        file_hash = hash_file(filepath=filepath)
        if file_hash in self._file_hashes and not overwrite:
            logger.warning(
                f"This file has been uploaded previously with the filename : {self._file_hashes[file_hash]} - skipping."
            )
            return {}

        # Define parameters to pass to the parser function
        # TODO: Update this to match against inputs for parser function.
        param = {
            "filepath": filepath,
            "site": site,
            "domain": domain,
            "model": model,
            "inlet": inlet,
            "metmodel": metmodel,
            "species": species,
            "network": network,
            "high_time_resolution": high_time_resolution,
            "high_spatial_resolution": high_spatial_resolution,
            "short_lifetime": short_lifetime,
            "period": period,
            "continuous": continuous,
            "chunks": chunks,
        }

        input_parameters: dict[Any, Any] = param.copy()

        # # TODO: Decide if we want to include details below / switch any parameters to be optional.
        # optional_keywords: dict[Any, Any] = {}

        # signature = inspect.signature(parser_fn)
        # fn_accepted_parameters = [param.name for param in signature.parameters.values()]

        # # Checks if optional parameters are present in function call and includes them else ignores its inclusion in input_parameters.
        # for param, param_value in optional_keywords.items():
        #     if param in fn_accepted_parameters:
        #         input_parameters[param] = param_value
        #     else:
        #         logger.warning(
        #             f"Input: '{param}' (value: {param_value}) is not being used as part of the standardisation process."
        #             f"This is not accepted by the current standardisation function: {parser_fn}"
        #         )

        footprint_data = parser_fn(**input_parameters)

        # Checking against expected format for footprints
        # Based on configuration (some user defined, some inferred)
        for split_data in footprint_data.values():
            fp_data = split_data["data"]
            Footprints.validate_data(
                fp_data,
                high_spatial_resolution=high_spatial_resolution,
                high_time_resolution=high_time_resolution,
                short_lifetime=short_lifetime,
            )

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
        datasource_uuids = self.assign_data(
            data=footprint_data, overwrite=overwrite, data_type=data_type, required_keys=required
        )

        # Record the file hash in case we see this file again
        self._file_hashes[file_hash] = filepath.name

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
            "latitude": np.floating,  # Covers np.float16, np.float32, np.float64 types
            "longitude": np.floating,
            "time": np.datetime64,
        }

        if not high_time_resolution and not high_spatial_resolution:
            # Includes standard footprint variable
            # data_vars["fp"] = ("time", "lat", "lon")
            # dtypes["fp"] = np.floating
            data_vars["srr"] = ("time", "latitude", "longitude")
            dtypes["srr"] = np.floating

        if high_spatial_resolution:
            # Include options for high spatial resolution footprint
            # This includes footprint data on multiple resolutions

            data_vars["fp_low"] = ("time", "latitude", "longitude")
            data_vars["fp_high"] = ("time", "lat_high", "lon_high")

            dtypes["fp_low"] = np.floating
            dtypes["fp_high"] = np.floating

        if high_time_resolution:
            # Include options for high time resolution footprint (usually co2)
            # This includes a footprint data with an additional hourly back dimension
            data_vars["fp_HiTRes"] = ("time", "latitude", "longitude", "H_back")
            dtypes["fp_HiTRes"] = np.floating
            dtypes["H_back"] = np.number  # float or integer

        # Includes particle location directions - one for each regional boundary
        if particle_locations:
            data_vars["particle_locations_n"] = ("time", "longitude", "height")
            data_vars["particle_locations_e"] = ("time", "latitude", "height")
            data_vars["particle_locations_s"] = ("time", "longitude", "height")
            data_vars["particle_locations_w"] = ("time", "latitude", "height")

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
            data_vars["mean_age_particles_n"] = ("time", "longitude", "height")
            data_vars["mean_age_particles_e"] = ("time", "latitude", "height")
            data_vars["mean_age_particles_s"] = ("time", "longitude", "height")
            data_vars["mean_age_particles_w"] = ("time", "latitude", "height")

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
