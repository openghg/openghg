from dataclasses import dataclass
from ._basedata import _BaseData
from collections import abc

__all__ = ["ObsData"]


@dataclass(frozen=True)
class ObsData(_BaseData, abc.Mapping):
    """ This class is used to return observations data from the get_observations function

        Args:
            data: Dictionary of xarray Dataframes
            metadata: Dictionary of metadata
    """

    # Compatability layer for legacy format - mimicing the reponse of a dictionary
    # Previous format expected a dictionary containing the site code and data
    # as key:value pairs.
    # TODO: May also want to check other expected keys within the legacy 
    # dictionary format and add them below
    def __getitem__(self, key: str):
        """
        Returns the data attribute (xarray Dataset) when the site name is 
        specified.
        Included as a compatability layer for legacy format as a dictionary
        containing a Dataset for each site code.

        key (str): Site code
        """
        site = self.metadata["site"].lower()            
        if key.lower() == site:
            return self.data
        else:
            raise KeyError(f"Site '{key}' does not match to expected site '{site}'")

    def __iter__(self):
        """
        Returns site code as the key for the dictionary as would be expected.
        """
        site = self.metadata["site"]
        return iter([site])

    def __len__(self):
        """
        Returns number of key values (fixed at 1 at present)
        """
        # Fixed length as 1 at the moment but may need to update if other key
        # values are added.
        return 1
