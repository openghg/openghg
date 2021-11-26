from openghg.store.base import Datasource
from openghg.store import ObsSurface
from openghg.util import valid_site, create_daterange_str, InvalidSiteError

# from pyvis.network import Network
# import matplotlib.cm as cm
# import matplotlib
from typing import Dict, Union

__all__ = ["RankSources"]


class RankSources:
    def __init__(self) -> None:
        self._lookup_data: Dict = {}
        self._key_lookup: Dict = {}
        self._user_info: Dict = {}
        self._needs_update = True

    def get_sources(self, site: str, species: str) -> Dict:
        """Get the datasources for this site and species to allow a ranking to be set

        Args:
            site: Three letter site code
            species: Species name
        Returns:
            dict: Dictionary of datasource metadata
        """
        if not valid_site(site):
            raise InvalidSiteError(f"{site} is not a valid site code")

        # Save these
        self.site = site
        self.species = species

        obs = ObsSurface.load()
        datasource_uuids = obs.datasources()
        rank_table = obs.rank_data()

        # Shallow load the Datasources (only get their JSON metadata)
        datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

        matching_sources = [d for d in datasources if d.search_metadata(site=site, species=species)]

        if not matching_sources:
            return {}

        def name_str(d: Datasource) -> str:
            return "_".join([d.species(), d.inlet(), d.instrument()])

        self._user_info = {
            name_str(d): {
                "rank_data": rank_table.get(d.uuid(), "NA"),
                "data_range": d.daterange_str(),
            }
            for d in matching_sources
        }

        self._key_lookup = {d.inlet(): d.uuid() for d in matching_sources}

        self._lookup_data = {"site": site, "species": species}
        self._needs_update = False

        return self._user_info

    def get_specific_source(self, key: str) -> Dict:
        """Return the ranking data of a specific key

        Args:
            key: Key
        Returns:
            dict: Dictionary of ranking data
        """
        if self._needs_update:
            site = self._lookup_data["site"]
            species = self._lookup_data["species"]
            _ = self.get_sources(site=site, species=species)

        rank_data: Dict[str, Union[str, Dict]] = self._user_info[key]["rank_data"]
        return rank_data

    def set_rank(
        self,
        inlet: str,
        rank: Union[int, str],
        start_date: str,
        end_date: str,
        overwrite: bool = False,
    ) -> None:
        """Set the rank data for the

        Args:
            inlet: Inlet to set ranking data
            rank: Number between 1 and 9
            start_date: Start date
            end_date: End date
            overwrite: If True overwrite current ranking data
        Returns:
            None
        """
        obs = ObsSurface.load()

        inlet = inlet.lower()
        uuid = self._key_lookup[inlet]

        daterange = create_daterange_str(start=start_date, end=end_date)

        obs.set_rank(uuid=uuid, rank=rank, date_range=daterange, overwrite=overwrite)

        self._needs_update = True

    def clear_rank(self, key: str) -> None:
        """Clear the ranking data for a Datasource

        Args:
            key: Key for specific source
        Returns:
            None
        """
        obs = ObsSurface.load()
        uuid = self._key_lookup[key]
        obs.clear_rank(uuid=uuid)
        self._needs_update = True

    # def visualise_rankings(self) -> Network:
    #     """ Creates a small network graph of ranked data with each rank given a colour

    #         Note that this function should only be run from a Jupyter Notebook

    #         Args:
    #             rank_data (dict): Dictionary of the form given by RankSources.get_sources()
    #         Returns:
    #             pyvis.network.Network: Network graph
    #     """
    #     header_text = "OpenGHG ranked data"
    #     net = Network("800px", "100%", notebook=True, heading=header_text)
    #     # Set the physics layout of the network
    #     net.force_atlas_2based()

    #     rank_data = self._key_lookup

    #     a_key = list(rank_data.keys())[0]
    #     site = rank_data[a_key]["metadata"]["site"].upper()

    #     norm = matplotlib.colors.Normalize(vmin=0, vmax=10, clip=True)
    #     mapper = cm.ScalarMappable(norm=norm, cmap=cm.tab10)

    #     def colour_mapper(x):
    #         return matplotlib.colors.to_hex(mapper.to_rgba(int(x)))

    #     net.add_node(site, label=site, color="brown", value=5000)

    #     for key, data in rank_data.items():
    #         rank = data["rank"]
    #         site_name = data["metadata"]["site"].upper()
    #         data_range = data["data_range"]

    #         # HTML to show when the mouse is hovered over a node
    #         title = "</br>".join([f"<b>Rank:</b> {str(rank)}", f"<b>Site:</b> {site_name}", f"<b>Data range:</b> {data_range}"])

    #         if rank == 0:
    #             colour = colour_mapper(rank)
    #         else:
    #             # For now just use the highest rank for the color
    #             highest_rank = sorted(list(rank.keys()))[-1]
    #             colour = colour_mapper(highest_rank)

    #         split_key = key.split("_")
    #         label = " ".join((split_key[0].upper(), split_key[1].upper(), split_key[2], split_key[3]))

    #         net.add_node(key, label=label, title=title, color=colour, value=2000)
    #         net.add_edge(source=site, to=key)

    #     return net.show("openghg_rankings.html")
