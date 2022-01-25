from xarray import Dataset

__all__ = ["plot_footprint"]


def plot_footprint(data: Dataset, label: str = None, vmin: float = None, vmax: float = None) -> None:
    """Plot a footprint

    Args:
        data: Dataset containing fp variable
        label: Label for colourbar
        vmin: Minimum value for colours
        vmax: MinimumMax value for colours
    Returns:
        None
    """
    import matplotlib.pyplot as plt
    import matplotlib.colors as colors

    fig, ax = plt.subplots()

    # Plot footprints as a 2D colour map
    data_fp = data.fp.isel(time=0)  # First time point
    lat = data_fp.lat
    lon = data_fp.lon
    footprint = data_fp.values

    vmin = 1e-5  # min is 0 and can't use 0 for a log scale
    vmax = footprint.max()

    im = ax.pcolormesh(
        lon, lat, footprint, norm=colors.LogNorm(vmin=vmin, vmax=vmax), shading="auto"
    )  # Put on a log scale
    cb = plt.colorbar(im, ax=ax)

    if label:
        cb.set_label(label)
