def status():
    """ Load some objects from the object store and return True

    """
    from HUGS.Modules import CRDS, GC
    
    gc = GC.load()
    crds = CRDS.load()

    return True
