Plotting functions
==================

These functions help in the creation of simple plots.

You can plot multiple timeseries using the ``plot_timeseries`` function. This expects
an ObsData object or a list of objects. By default it will try and find the species in the
metadata stored within the object and plot that. You can modify the variables you'd like to plot
using the `x_var` and `y_var` arguments. You can also make changes to the title and axes labels.

.. autofunction:: openghg.plotting.plot_footprint

.. autofunction:: openghg.plotting.plot_timeseries

