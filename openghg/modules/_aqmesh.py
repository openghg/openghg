from typing import Dict, Optional, Union
from pathlib import Path

__all__ = ["AQMESH"]


class AQMESH:
    """Read AQMesh data files"""

    def read_file(
        self,
        data_filepath: Union[str, Path],
        species: str,
        network: str,
        inlet: str,
        instrument: Optional[str] = "NA",
        sampling_period: Optional[str] = None,
        measurement_type: Optional[str] = None,
    ) -> Dict:
        """Read AQMesh data files

        Args:
            filepath: Data filepath
            site: Site name
        Returns:
            dict: Dictionary of data
        """
        from addict import Dict as aDict
        from openghg.util import clean_string, load_json
        from pandas import to_datetime, read_csv

        # use_cols = [date_UTC,co2_ppm,location_name,ratification_status]
        use_cols = [0, 1, 4, 6]
        datetime_cols = {"time": ["date_UTC"]}
        na_values = [-999, -999.0]

        def date_parser(date):
            return to_datetime(date, utc=True)

        df = read_csv(
            data_filepath,
            index_col="time",
            usecols=use_cols,
            parse_dates=datetime_cols,
            na_values=na_values,
            date_parser=date_parser,
        )

        species_name = "co2"
        species_lower = species_name.lower()
        network = "aqmesh"

        rename_cols = {f"{species_name}_ppm": species_name, "location_name": "site"}
        df = df.rename(columns=rename_cols)
        df = df.dropna(axis="rows", subset=[species_lower])

        site_groups = df.groupby(df["site"])

        metadata = load_json(filename="aqmesh_metadata.json")

        metadata
        site_data = aDict()
        for site, site_df in site_groups:
            site_name = site.replace(" ", "").lower()
            site_data[site_name]["data"] = site_df.to_xarray()
            site_data[site_name]["metadata"] = metadata[network][site_name]

        return site_data.to_dict()
