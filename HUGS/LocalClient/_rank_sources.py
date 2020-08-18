import copy
from HUGS.Modules import Datasource
from HUGS.Processing import DataTypes
from HUGS.Util import load_object, valid_site

__all__ = ["RankSources"]


class RankSources:
    def get_sources(self, site, species, data_type):
        """ Get the datasources for this site and species to allow a ranking to be set

            Args:
                site (str): Three letter site code
                species (str): Species name
                data_type (str): Must be valid datatype i.e. CRDS, GC
                See all valid datasources in the DataTypes class
            Returns:
                dict: Dictionary of datasource metadata
        """
        if len(site) != 3 or not valid_site(site):
            # raise InvalidSiteError(f"{site} is not a valid site code")
            raise ValueError(f"{site} is not a valid site code")

        data_type = DataTypes[data_type.upper()].name

        data_obj = load_object(class_name=data_type)

        datasource_uuids = data_obj.datasources()
        # Shallow load the Datasources (only get their JSON metadata)
        datasources = [Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids]

        matching_sources = [d for d in datasources if d.search_metadata(search_terms=[site, species], find_all=True)]

        def name_str(d):
            return "_".join([d.species(), d.site(), d.inlet(), d.instrument()])

        rank_info = {name_str(d): {"rank": d.rank(), "data_range": d.daterange_str(), "uuid": d.uuid()} for d in matching_sources}

        self._before_ranking = copy.deepcopy(rank_info)
        self._key_uuids = {key: rank_info[key]["uuid"] for key in rank_info}

        return rank_info

    def rank_sources(self, updated_rankings, data_type):
        """ Assign the precendence of sources for each.
            This function expects a dictionary of the form

            This function expects a dictionary of the form

            {'site_string': {'rank': [daterange_str, ...], 'daterange': 'start_end', 'uuid': uuid}, 

            Args:
                updated_ranking (dict): Dictionary of ranking
                data_type (str): Data type e.g. CRDS, GC
            Returns:
                None
        """
        if updated_rankings == self._before_ranking:
            return

        data_type = DataTypes[data_type.upper()].name
        data_obj = load_object(class_name=data_type)

        for key in updated_rankings:
            uuid = updated_rankings[key]["uuid"]

            for rank, daterange in updated_rankings[key]["rank"].items():
                if rank == 0:
                    continue

                for d in daterange:
                    data_obj.set_rank(uuid=uuid, rank=rank, daterange=d)

        data_obj.save()

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
