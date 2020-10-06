def clear_datasources(args):
    from openghg.modules import CRDS, GC

    gc = GC.load()
    crds = CRDS.load()

    crds.clear_datasources()
    gc.clear_datasources()

    crds.save()
    gc.save()
