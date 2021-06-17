from openghg.modules import ObsSurface


def clear_rank(args):
    obs = ObsSurface.load()

    uuid = args["uuid"]

    obs.clear_rank(uuid=uuid)
