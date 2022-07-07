from typing import DefaultDict, Dict, List, Optional, Union, NoReturn
from pathlib import Path
from pandas import Timestamp
from xarray import Dataset
from openghg.store.base import BaseStore
from collections import defaultdict
from tempfile import TemporaryDirectory

__all__ = ["Footprints"]


class Footprints(BaseStore):
    """This class is used to process footprints model output"""

    _root = "Footprints"
    _uuid = "62db5bdf-c88d-4e56-97f4-40336d37f18c"
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

            return Footprints.read_file(filepath=filepath, **metadata)

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

    @staticmethod
    def read_file(
        filepath: Union[str, Path],
        site: str,
        height: str,
        domain: str,
        model: str,
        metmodel: Optional[str] = None,
        species: Optional[str] = None,
        network: Optional[str] = None,
        period: Optional[Union[str, tuple]] = None,
        continuous: bool = True,
        retrieve_met: bool = False,
        high_spatial_res: bool = False,
        high_time_res: bool = False,
        overwrite: bool = False,
        # model_params: Optional[Dict] = None,
    ) -> Dict[str, Dict]:
        """Reads footprints data files and returns the UUIDS of the Datasources
        the processed data has been assigned to

        Args:
            filepath: Path of file to load
            site: Site name
            height: Height above ground level in metres
            domain: Domain of footprints
            model: Model used to create footprint (e.g. NAME or FLEXPART)
            metmodel: Underlying meteorlogical model used (e.g. UKV)
            species: Species name. Only needed if footprint is for a specific species e.g. co2 (and not inert)
            network: Network name
            period: Period of measurements. Only needed if this can not be inferred from the time coords
            continuous: Whether time stamps have to be continuous.
            retrieve_met: Whether to also download meterological data for this footprints area
            high_spatial_res : Indicate footprints include both a low and high spatial resolution.
            high_time_res: Indicate footprints are high time resolution (include H_back dimension)
                           Note this will be set to True automatically for Carbon Dioxide data.
            overwrite: Overwrite any currently stored data
            high_res: ***

        Returns:
            dict: UUIDs of Datasources data has been assigned to
        """
        from xarray import open_dataset
        from openghg.util import (
            hash_file,
            timestamp_now,
            clean_string,
        )
        from openghg.store import assign_data, infer_date_range, load_metastore, datasource_lookup

        filepath = Path(filepath)

        site = clean_string(site)
        network = clean_string(network)
        height = clean_string(height)
        domain = clean_string(domain)

        fp = Footprints.load()

        # Load in the metadata store
        metastore = load_metastore(key=fp._metakey)

        file_hash = hash_file(filepath=filepath)
        if file_hash in fp._file_hashes and not overwrite:
            print(
                f"This file has been uploaded previously with the filename : {fp._file_hashes[file_hash]} - skipping."
            )

        fp_data = open_dataset(filepath)

        # Need to read the metadata from the footprints and then store it
        # Do we need to chunk the footprints / will a Datasource store it correctly?
        metadata: Dict[str, Union[str, float, List[float]]] = {}

        metadata["data_type"] = "footprints"
        metadata["site"] = site
        metadata["height"] = height
        metadata["domain"] = domain
        metadata["model"] = model

        if species is not None:
            metadata["species"] = clean_string(species)

        if network is not None:
            metadata["network"] = clean_string(network)

        if metmodel is not None:
            metadata["metmodel"] = clean_string(metmodel)

        fp_time = fp_data.time

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

        # TODO: Pull out links to underlying data format into a separate format function
        #  - high_spatial_res - data vars - "fp_low", "fp_high", coords - "lat_high", "lon_high"
        #  - high_time_res - data vars - "fp_HiTRes", coords - "H_back"

        metadata["spatial_resolution"] = "standard_spatial_resolution"

        if high_spatial_res:
            try:
                metadata["max_longitude_high"] = round(float(fp_data["lon_high"].max()), 5)
                metadata["min_longitude_high"] = round(float(fp_data["lon_high"].min()), 5)
                metadata["max_latitude_high"] = round(float(fp_data["lat_high"].max()), 5)
                metadata["min_latitude_high"] = round(float(fp_data["lat_high"].min()), 5)

                metadata["spatial_resolution"] = "high_spatial_resolution"
            except KeyError:
                raise KeyError("Expected high spatial resolution. Unable to find lat_high or lon_high data.")

        if species == "co2":
            # Expect co2 data to have high time resolution
            high_time_res = True

        metadata["time_resolution"] = "standard_time_resolution"

        if high_time_res:
            if "fp_HiTRes" in fp_data:
                metadata["time_resolution"] = "high_time_resolution"
            else:
                raise KeyError("Expected high time resolution. Unable to find fp_HiTRes data.")

        metadata["heights"] = [float(h) for h in fp_data.height.values]
        # Do we also need to save all the variables we have available in this footprints?
        metadata["variables"] = list(fp_data.keys())

        # if model_params is not None:
        #     metadata["model_parameters"] = model_params

        # Set the attributes of this Dataset
        fp_data.attrs = {"author": "OpenGHG Cloud", "processed": str(timestamp_now())}

        # This might seem longwinded now but will help when we want to read
        # more than one footprints at a time
        key = "_".join((site, domain, model, height))

        footprint_data: DefaultDict[str, Dict[str, Union[Dict, Dataset]]] = defaultdict(dict)
        footprint_data[key]["data"] = fp_data
        footprint_data[key]["metadata"] = metadata

        # These are the keys we will take from the metadata to search the
        # metadata store for a Datasource, they should provide as much detail as possible
        # to uniquely identify a Datasource
        required = ("site", "model", "height", "domain")
        lookup_results = datasource_lookup(metastore=metastore, data=footprint_data, required_keys=required)

        data_type = "footprints"
        datasource_uuids: Dict[str, Dict] = assign_data(
            data_dict=footprint_data,
            lookup_results=lookup_results,
            overwrite=overwrite,
            data_type=data_type,
        )

        fp.add_datasources(uuids=datasource_uuids, data=footprint_data, metastore=metastore)

        # Record the file hash in case we see this file again
        fp._file_hashes[file_hash] = filepath.name

        fp.save()

        metastore.close()

        return datasource_uuids

    def search(
        self,
        site: str,
        network: str,
        start_date: Optional[Union[str, Timestamp]],
        end_date: Optional[Union[str, Timestamp]],
    ) -> NoReturn:
        """Search for a footprints from a specific site and network, return a dictionary of data
        so the user can choose
        """
        raise NotImplementedError()

    def retrieve(self, uuid: str, dates: str) -> NoReturn:
        """"""
        raise NotImplementedError()

    def _get_metdata(self) -> NoReturn:
        """This retrieves the metadata for this footprints"""
        raise NotImplementedError()
