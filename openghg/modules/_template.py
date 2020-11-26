from openghg.modules import BaseModule
from typing import Dict, Optional, Union
from pathlib import Path

# flake8: noqa

__all__ = ["TEMPLATE"]

# To use this template replace:
# - TEMPLATE with new data name in all upper case e.g. CRDS
# - template with new data name in all lower case e.g. crds
# - CHANGEME with a new fixed uuid (at the moment)


class TEMPLATE(BaseModule):
    """ Class for processing TEMPLATE data

    """

    _root = "TEMPLATE"
    # Use uuid.uuid4() to create a unique fixed UUID for this object
    _uuid = "CHANGEME"

    def __init__(self):
        self._sampling_period = 60

    def read_file(self, data_filepath: Union[str, Path], site: Optional[str] = None) -> Dict:
        """ Reads TEMPLATE data files and returns the UUIDS of the Datasources
            the processed data has been assigned to

            Args:
                filepath: Path of file to load
                site: Site code
            Returns:
                dict: UUIDs of Datasources data has been assigned to
        """
        from openghg.processing import assign_attributes
        from pathlib import Path

        data_filepath = Path(data_filepath)
        filename = data_filepath.name

        if not site:
            site = filename.stem.split(".")[0]

        # This should return xarray Datasets
        gas_data = template.read_data(data_filepath=data_filepath, site=site)

        # Assign attributes to the xarray Datasets here data here makes it a lot easier to test
        gas_data = assign_attributes(data=gas_data, site=site)

        return gas_data

    def read_data(self, data_filepath: Path, site: str) -> Dict:
        """ Separates the gases stored in the dataframe in
            separate dataframes and returns a dictionary of gases
            with an assigned UUID as gas:UUID and a list of the processed
            dataframes

            Args:
                data_filepath: Path of datafile
            Returns:
                dict: Dictionary containing attributes, data and metadata keys
        """
        from pandas import RangeIndex, concat, read_csv, datetime, NaT
        from openghg.processing import get_attributes, read_metadata

        metadata = read_metadata(filepath=data_filepath, data=data, data_type="TEMPLATE")
        # This dictionary is used to store the gas data and its associated metadata
        combined_data = {}

        for n in range(n_gases):
            # Here we can convert the Dataframe to a Dataset and then write the attributes
            # Load in the JSON we need to process attributes
            gas_data = gas_data.to_xarray()

            site_attributes = {"attribute_a": 1, "attribute_b": 2}

            # Create a copy of the metadata dict
            species_metadata = metadata.copy()
            species_metadata["species"] = species
            species_metadata["source_name"] = source_name

            combined_data[species] = {
                "metadata": species_metadata,
                "data": gas_data,
                "attributes": site_attributes,
            }

        return combined_data
