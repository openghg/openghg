import copy
from openghg.modules import Datasource, ObsSurface
from openghg.util import valid_site
from pyvis.network import Network
import matplotlib.cm as cm
import matplotlib

__all__ = ["RankSources"]


class RankSources:
    def get_sources(self, site, species=None):
        """ Get the datasources for this site and species to allow a ranking to be set

            Args:
                site (str): Three letter site code
                species (str): Species name
            Returns:
                dict: Dictionary of datasource metadata
        """
        if len(site) != 3 or not valid_site(site):
            # raise InvalidSiteError(f"{site} is not a valid site code")
            raise ValueError(f"{site} is not a valid site code")

        obs = ObsSurface.load()
        datasource_uuids = obs.datasources()

        # Shallow load the Datasources (only get their JSON metadata)
        datasources = [Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids]

        search_terms = [site]
        if species is not None:
            search_terms.append(species)

        matching_sources = [d for d in datasources if d.search_metadata(search_terms=search_terms, find_all=True)]

        def name_str(d):
            return "_".join([d.species(), d.site(), d.inlet(), d.instrument()])

        rank_info = {
            name_str(d): {"rank": d.rank(), "data_range": d.daterange_str(), "uuid": d.uuid(), "metadata": d.metadata()}
            for d in matching_sources
        }

        self._rank_data = copy.deepcopy(rank_info)
        # self._key_uuids = {key: rank_info[key]["uuid"] for key in rank_info}

        return rank_info

    def set_rank(self, rank_key: str, rank_data: dict):
        """ Set the rank data for the 

            Args:
                rank_key: Key of ranking data from the original dict
                return by get_sources.
                rank_data: Dictionary of ranking data for example
                co2_hfd_50m_picarro: {1: [daterange_1], 2: [daterange_2]}
            Returns:
                None
        """
        # First find the UUID
        uuid = self._rank_data[rank_key]["uuid"]

        obs = ObsSurface.load()

        for rank, dateranges in rank_data[rank_key].items():
            if int(rank) == 0:
                continue

            for daterange in dateranges:
                obs.set_rank(uuid=uuid, rank=rank, daterange=daterange)

        # Update local version of data
        self._rank_data[rank_key]["rank"] = rank_data

        obs.save()

    def visualise_rankings(self) -> Network:
        """ Creates a small network graph of ranked data with each rank given a colour

            Note that this function should only be run from a Jupyter Notebook

            Args:
                rank_data (dict): Dictionary of the form given by RankSources.get_sources()
            Returns:
                pyvis.network.Network: Network graph
        """
        header_text = "OpenGHG ranked data"
        net = Network("800px", "100%", notebook=True, heading=header_text)
        # Set the physics layout of the network
        net.force_atlas_2based()

        rank_data = self._rank_data

        a_key = list(rank_data.keys())[0]
        site = rank_data[a_key]["metadata"]["site"].upper()

        norm = matplotlib.colors.Normalize(vmin=0, vmax=10, clip=True)
        mapper = cm.ScalarMappable(norm=norm, cmap=cm.tab10)

        def colour_mapper(x):
            return matplotlib.colors.to_hex(mapper.to_rgba(int(x)))

        net.add_node(site, label=site, color="brown", value=5000)

        for key, data in rank_data.items():
            rank = data["rank"]
            site_name = data["metadata"]["site"].upper()
            data_range = data["data_range"]

            # HTML to show when the mouse is hovered over a node
            title = "</br>".join([f"<b>Rank:</b> {str(rank)}", f"<b>Site:</b> {site_name}", f"<b>Data range:</b> {data_range}"])

            if rank == 0:
                colour = colour_mapper(rank)
            else:
                # For now just use the highest rank for the color
                highest_rank = sorted(list(rank.keys()))[-1]
                colour = colour_mapper(highest_rank)

            split_key = key.split("_")
            label = " ".join((split_key[0].upper(), split_key[1].upper(), split_key[2], split_key[3]))

            net.add_node(key, label=label, title=title, color=colour, value=2000)
            net.add_edge(source=site, to=key)

        return net.show("openghg_rankings.html")

    def rank_sources(self, updated_rankings):
        """ Assign the precendence of sources for each.
            This function expects a dictionary of the form

            {'site_string': {'rank': [daterange_str, ...], 'daterange': 'start_end', 'uuid': uuid}, 

            Args:
                updated_ranking (dict): Dictionary of ranking
            Returns:
                None
        """
        from warnings import warn

        warn(message="This function will soon be removed, please use set_rank instead", category=DeprecationWarning)

        if updated_rankings == self._rank_data:
            return

        obs = ObsSurface.load()

        for key in updated_rankings:
            uuid = updated_rankings[key]["uuid"]

            for rank, daterange in updated_rankings[key]["rank"].items():
                if rank == 0:
                    continue

                for d in daterange:
                    obs.set_rank(uuid=uuid, rank=rank, daterange=d)

        obs.save()

    def create_daterange(self, start, end):
        """ Create a JSON serialisable daterange string for use in ranking dict

            Args:
                start (datetime): Start of daterange
                end (datetime): End of daterange
            Returns:
                str: Serialisable daterange string
        """
        from Acquire.ObjectStore import datetime_to_string
        from pandas import Timestamp

        if isinstance(start, str) and isinstance(end, str):
            start = Timestamp(start).to_pydatetime()
            end = Timestamp(end).to_pydatetime()

        return "".join([datetime_to_string(start), "_", datetime_to_string(end)])

    # def visualise_ranks():
    #     """ Return a dic

    #     """
