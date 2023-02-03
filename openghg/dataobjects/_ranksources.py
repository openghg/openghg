# # type: ignore
# from typing import Dict, Union

# from openghg.store import ObsSurface
# from openghg.store.base import Datasource
# from openghg.util import create_daterange_str, verify_site


# class RankSources:
#     def __init__(self, cloud: bool = False) -> None:
#         self._cloud = cloud

#         raise NotImplementedError("Ranking currently not supported.")

#         if cloud:
#             raise NotImplementedError

#     def raw(self) -> Dict:
#         """Return the raw ranking data

#         Args:
#             None
#         Returns:
#             dict: Raw ranking data
#         """
#         return self._user_info

#     def get_sources(self, site: str, species: str) -> Dict:
#         """Get the datasources for this site and species to allow a ranking to be set

#         Args:
#             site: Three letter site code
#             species: Species name
#         Returns:
#             dict: Dictionary of datasource metadata
#         """
#         if self._cloud:
#             raise NotImplementedError
#             # return self._get_sources_cloud(site=site, species=species)
#         else:
#             return self._get_sources_local(site=site, species=species)

#     # def _get_sources_cloud(self, site: str, species: str) -> Dict:
#     #     site = verify_site(site=site)

#     #     args = {"site": site, "species": species}

#     #     self.site = site
#     #     self.species = species

#     #     response: Dict = self._service.call_function(function="rank.get_sources", args=args)

#     #     if not response:
#     #         raise ValueError(f"No sources found for {species} at {site}")

#     #     self._user_info: Dict = response["user_info"]
#     #     self._key_lookup: Dict = response["key_lookup"]
#     #     self._needs_update = False

#     #     return self._user_info

#     def _get_sources_local(self, site: str, species: str) -> Dict:
#         raise NotImplementedError
#         site = verify_site(site=site)

#         # Save these
#         self.site = site
#         self.species = species

#         obs = ObsSurface.load()
#         datasource_uuids = obs.datasources()
#         rank_table = obs.rank_data()

#         # Shallow load the Datasources (only get their JSON metadata)
#         datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

#         matching_sources = [d for d in datasources if d.search_metadata(site=site, species=species)]

#         if matching_sources:
#             self._user_info = {
#                 d.inlet(): {
#                     "rank_data": rank_table.get(d.uuid(), "NA"),
#                     "data_range": d.daterange_str(),
#                 }
#                 for d in matching_sources
#             }

#             self._key_lookup = {d.inlet(): d.uuid() for d in matching_sources}
#             self._needs_update = False
#         else:
#             self._user_info = {}

#         return self._user_info

#     def get_specific_source(self, inlet: str) -> Dict:
#         """Return the ranking data of a specific key

#         Args:
#             key: Key
#         Returns:
#             dict: Dictionary of ranking data
#         """
#         if self._needs_update:
#             self.get_sources(site=self.site, species=self.species)

#         rank_data: Dict[str, Union[str, Dict]] = self._user_info[inlet]["rank_data"]
#         return rank_data

#     def set_rank(
#         self,
#         inlet: str,
#         rank: Union[int, str],
#         start_date: str,
#         end_date: str,
#         overwrite: bool = False,
#     ) -> None:
#         """Set the rank data for the

#         Args:
#             inlet: Inlet to set ranking data
#             rank: Number between 1 and 9
#             start_date: Start date
#             end_date: End date
#             overwrite: If True overwrite current ranking data
#         Returns:
#             None
#         """
#         if self._cloud:
#             raise NotImplementedError
#         else:
#             return self._set_rank_local(
#                 inlet=inlet,
#                 rank=rank,
#                 start_date=start_date,
#                 end_date=end_date,
#                 overwrite=overwrite,
#             )

#     # def _set_rank_cloud(
#     #     self,
#     #     inlet: str,
#     #     rank: Union[int, str],
#     #     start_date: str,
#     #     end_date: str,
#     #     overwrite: bool = False,
#     # ) -> None:
#     #     inlet = inlet.lower()
#     #     uuid = self._key_lookup[inlet]

#     #     dateranges = create_daterange_str(start=start_date, end=end_date)

#     #     args: Dict[str, Union[str, int, List]] = {}
#     #     args["rank"] = rank
#     #     args["uuid"] = uuid
#     #     args["dateranges"] = dateranges
#     #     args["overwrite"] = overwrite

#     #     self._service.call_function(function="rank.set_rank", args=args)
#     #     self._needs_update = True

#     def _set_rank_local(
#         self,
#         inlet: str,
#         rank: Union[int, str],
#         start_date: str,
#         end_date: str,
#         overwrite: bool = False,
#     ) -> None:
#         obs = ObsSurface.load()

#         inlet = inlet.lower()
#         uuid = self._key_lookup[inlet]

#         daterange = create_daterange_str(start=start_date, end=end_date)

#         obs.set_rank(uuid=uuid, rank=rank, date_range=daterange, overwrite=overwrite)

#         self._needs_update = True

#     def clear_rank(self, inlet: str) -> None:
#         """Clear the ranking data for a Datasource

#         Args:
#             key: Key for specific source
#         Returns:
#             None
#         """
#         if self._cloud:
#             raise NotImplementedError
#             # return self._clear_rank_cloud(inlet=inlet)
#         else:
#             return self._clear_rank_local(inlet=inlet)

#     def _clear_rank_local(self, inlet: str) -> None:
#         """Clear the ranking data for a Datasource

#         Args:
#             key: Key for specific source
#         Returns:
#             None
#         """
#         obs = ObsSurface.load()
#         inlet = inlet.lower()
#         uuid = self._key_lookup[inlet]
#         obs.clear_rank(uuid=uuid)
#         self._needs_update = True

#     # def _clear_rank_cloud(self, inlet: str) -> None:
#     #     """Clear the ranking data for a Datasource

#     #     Args:
#     #         key: Key for specific source
#     #     Returns:
#     #         None
#     #     """
#     #     uuid = self._key_lookup[inlet]
#     #     args = {"uuid": uuid}
#     #     self._service.call_function(function="rank.clear_rank", args=args)
#     #     self._needs_update = True

#     # def visualise_rankings(self) -> Network:
#     #     """ Creates a small network graph of ranked data with each rank given a colour

#     #         Note that this function should only be run from a Jupyter Notebook

#     #         Args:
#     #             rank_data (dict): Dictionary of the form given by RankSources.get_sources()
#     #         Returns:
#     #             pyvis.network.Network: Network graph
#     #     """
#     #     header_text = "OpenGHG ranked data"
#     #     net = Network("800px", "100%", notebook=True, heading=header_text)
#     #     # Set the physics layout of the network
#     #     net.force_atlas_2based()

#     #     rank_data = self._key_lookup

#     #     a_key = list(rank_data.keys())[0]
#     #     site = rank_data[a_key]["metadata"]["site"].upper()

#     #     norm = matplotlib.colors.Normalize(vmin=0, vmax=10, clip=True)
#     #     mapper = cm.ScalarMappable(norm=norm, cmap=cm.tab10)

#     #     def colour_mapper(x):
#     #         return matplotlib.colors.to_hex(mapper.to_rgba(int(x)))

#     #     net.add_node(site, label=site, color="brown", value=5000)

#     #     for key, data in rank_data.items():
#     #         rank = data["rank"]
#     #         site_name = data["metadata"]["site"].upper()
#     #         data_range = data["data_range"]

#     #         # HTML to show when the mouse is hovered over a node
#     #         title = "</br>".join([f"<b>Rank:</b> {str(rank)}", f"<b>Site:</b> {site_name}", f"<b>Data range:</b> {data_range}"])

#     #         if rank == 0:
#     #             colour = colour_mapper(rank)
#     #         else:
#     #             # For now just use the highest rank for the color
#     #             highest_rank = sorted(list(rank.keys()))[-1]
#     #             colour = colour_mapper(highest_rank)

#     #         split_key = key.split("_")
#     #         label = " ".join((split_key[0].upper(), split_key[1].upper(), split_key[2], split_key[3]))

#     #         net.add_node(key, label=label, title=title, color=colour, value=2000)
#     #         net.add_edge(source=site, to=key)

#     #     return net.show("openghg_rankings.html")
