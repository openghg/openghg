from HUGS.Processing import DataTypes
from HUGS.Util import load_object


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

    try:
        data_type = DataTypes[args["data_type"].upper()].name
    except KeyError:
        raise KeyError(f"Data type must be specified. Valid options are: {[e.value for e in DataTypes]}")

    data_obj = load_object(class_name=data_type)

    for key in ranking_data:
        uuid = ranking_data[key]["uuid"]
        for rank, daterange in ranking_data[key]["rank"].items():
            if rank == 0:
                continue

            for d in daterange:
                data_obj.set_rank(uuid=uuid, rank=rank, daterange=d)

    data_obj.save()
