---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.1
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Workflow 4: Working with carbon dioxide

For carbon dioxide data, the natural diurnal cycle must be taken into account when comparing any expected measurements against our observations.

In order to compare our measurements to modelled data for carbon dioxide ($\mathrm{CO_2}$), we need high frequency inputs for both the footprint and flux files. For NAME footprints these should contain an additional "H_back" dimension to store hourly footprints overall an initial time period for each release time. For any natural flux data to compare against, this should have a frequency of less than 24 hours.

*Operations around $CO_2$ and high resolution data may run slow at the moment, the plan is to profile this so we can optimise performance.*

> **_NOTE:_**  Plots created within this tutorial may not show up on the online documentation version of this notebook.

+++

## 1. Loading data sources into the object store

For this tutorial we will again use a helper function from the `openghg.tutorial` submodule to add some data to your local object store. As we're going to be comparing CO2 emissions with flux data here, we'll add surface, flux and footprint data to the object store. Don't worry if you get a message telling you the file has already been added, this might happen if you've been through one of the other tutorials previously.

```{code-cell} ipython3
from openghg.tutorial import populate_flux_data, populate_surface_data, populate_footprint_data
```

First we'll add some surface measurment data

```{code-cell} ipython3
populate_surface_data()
```

Next we'll add some flux data.

```{code-cell} ipython3
populate_flux_data()
```

And finally some footprint data

```{code-cell} ipython3
populate_footprint_data()
```

## 2. Creating a model scenario

To link together our observations to our ancillary data we can create a `ModelScenario` object, as shown in the previous tutorial [3_Comparing_with_emissions.ipynb](3_Comparing_with_emissions.ipynb), using suitable keywords to grab the data from the object store.

```{code-cell} ipython3
from openghg.analyse import ModelScenario

site = "TAC"
domain = "EUROPE"
species = "co2"
height = "185m"
source_natural = "natural"
start_date = "2017-07-01"
end_date = "2017-07-07"

scenario = ModelScenario(site=site,
                         inlet=height,
                         domain=domain,
                         species=species,
                         source=source_natural,
                         start_date=start_date,
                         end_date=end_date)
```

We can plot our observation timeseries using the `ModelScenario.plot_timeseries()` method as before:

```{code-cell} ipython3
scenario.plot_timeseries()
```

We can also check trace details of the extracted data by checking the available metadata. For instance for our footprint data we would expect this to have an associated species and for this to be labelled as "co2":

```{code-cell} ipython3
footprint_metadata = scenario.footprint.metadata
footprint_species = footprint_metadata["species"]
print(f"Our linked footprint has an associated species of '{footprint_species}'")
```

## 3. Comparing data sources

Once the correct high frequency emissions and footprints have been linked for our carbon dioxide data, we can start to plot comparisons between the sources and our measurement data.

```{code-cell} ipython3
scenario.plot_comparison(baseline="percentile")
```

As in the previous tutorial, multiple fluxes can be linked to your `ModelScenario` object if required. This can include additional high frequency (<24 hourly) or low frequency flux data. In this case we have added monthly "fossil fuel" emissions:

```{code-cell} ipython3
source_fossil = "ff-edgar-bp"

scenario.add_flux(species=species,
                  source=source_fossil,
                  domain=domain)
```

```{code-cell} ipython3
fossil_flux = scenario.fluxes[source_fossil]
fossil_flux
```

If we plot the modelled measurement comparison, this will stack the natural and fossil fuel flux sources and combine with the footprint data in an appropriate way:

```{code-cell} ipython3
# scenario.plot_comparison(baseline="percentile", recalculate=True)
```