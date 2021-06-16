from openghg.modules import ObsSurface


def set_rank(args):
    obs = ObsSurface.load()

    rank = args["rank"]
    uuid = args["uuid"]
    dateranges = args["dateranges"]
    overwrite = args["overwrite"]

    obs.set_rank(uuid=uuid, rank=rank, date_range=dateranges, overwrite=overwrite)

