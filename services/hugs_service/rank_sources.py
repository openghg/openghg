from HUGS.Modules import ObsSurface


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

    obs = ObsSurface.load()

    for key in ranking_data:
        uuid = ranking_data[key]["uuid"]

        for rank, daterange in ranking_data[key]["rank"].items():
            if rank == 0:
                continue

            for d in daterange:
                obs.set_rank(uuid=uuid, rank=rank, daterange=d)

    obs.save()
