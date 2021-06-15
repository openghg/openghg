from openghg.modules import ObsSurface


def set_rank(args):
    obs = ObsSurface.load()

    uuid = args["uuid"]

    obs.clear_rank(uuid=uuid)
