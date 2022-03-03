---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.6
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Workflow 4: Working with carbon dioxide

For carbon dioxide data, the natural diurnal cycle must be taken into account when comparing any expected measurements against our observations.

In order to compare our measurements to modelled data for carbon dioxide ($\mathrm{CO_2}$), we need high frequency inputs for both the footprint and flux files. For NAME footprints these should contain an additional "H_back" dimension to store hourly footprints overall an initial time period for each release time. For any natural flux data to compare against, this should have a frequency of less than 24 hours.

*Operations around $CO_2$ and high resolution data may run slow at the moment, the plan is to profile this so we can optimise performance.*

> **_NOTE:_**  Plots created within this tutorial may not show up on the online documentation version of this notebook.

```{code-cell} ipython3
import os
import tempfile

tmp_dir = tempfile.TemporaryDirectory()
os.environ["OPENGHG_PATH"] = tmp_dir.name   # temporary directory

%load_ext autoreload
%autoreload 2
```

## 1. Loading data sources into the object store

For this tutorial we will again set up a temporary object store to store our data. See [1_Adding_observation_data.ipynb](1_Adding_observation_data.ipynb) for more details around this.

```{code-cell} ipython3
## Defining inputs

site = "TAC"
network = "DECC"
height = "100m"

domain = "EUROPE"
species = "co2"
```

```{code-cell} ipython3
## Observations
from openghg.client import process_obs
from openghg.util import retrieve_example_data

obs_data = retrieve_example_data(path="timeseries/tac_example.tar.gz")

site = "TAC"
species = "co2"
network = "DECC"

decc_results = process_obs(files=obs_data, data_type="CRDS", site=site, network=network)
```

```{code-cell} ipython3
## Emissions
from openghg.client import process_flux

flux_data = retrieve_example_data(path="flux/co2-flux_EUROPE_2017.tar.gz")

domain = "EUROPE"
species = "co2"
date = "2017"

source_natural = "natural"
source_fossil = "ff-edgar-bp"

flux_file_natural = [filename for filename in flux_data if source_natural in str(filename)][0]
flux_file_ff = [filename for filename in flux_data if source_fossil in str(filename)][0]

process_flux(files=flux_file_natural, species=species, source=source_natural, domain=domain, date=date, high_time_resolution=True)
process_flux(files=flux_file_ff, species=species, source=source_fossil, domain=domain, date=date)
```

```{code-cell} ipython3
## Footprints
from openghg.client import process_footprint

footprint_data = retrieve_example_data(path="footprint/tac_footprint_co2_201707.tar.gz")

site = "TAC"
domain = "EUROPE"
species = "co2"
height = "185m"
model = "NAME"
metmodel = "UKV"

process_footprint(files=footprint_data, site=site, height=height, domain=domain, model=model, metmodel=metmodel, species=species)
```

## 2. Creating a model scenario

To link together our observations to our ancillary data we can create a `ModelScenario` object, as shown in the previous tutorial [3_Comparing_with_emissions.ipynb](3_Comparing_with_emissions.ipynb), using suitable keywords to grab the data from the object store.

```{code-cell} ipython3
from openghg.analyse import ModelScenario

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
scenario.plot_comparison(baseline="percentile", recalculate=True)
```
