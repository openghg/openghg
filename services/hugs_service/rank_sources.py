from HUGS.Modules import Datasource


def rank_sources(args):
    """ Rank Datasources to be primary sources for specific species at specific sites.

        Args:
            args (dict): Dictionary containing ranking data
        Returns:
            None
    """
    try:
        ranking_data = args["ranking"]
    except KeyError:
        raise KeyError("No ranking data passed")

    for r in ranking_data:
        uuid = ranking_data[r]["uuid"]
        rank = ranking_data[r]["rank"]
        daterange = ranking_data[r]["daterange"]

        # If a Datasource is left as unranked, ignore it
        if rank == -1:
            continue

        datasource = Datasource.load(uuid=uuid, shallow=True)

        datasource.set_rank(rank=rank, daterange=daterange)

        datasource.save()
