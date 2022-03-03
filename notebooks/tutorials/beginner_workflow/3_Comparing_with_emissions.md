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

# Workflow 3: comparing observations to emissions

In addition to observation files, ancillary data can also be added to an openghg object store which can be used to perform analysis.

At the moment, the accepted files include:
 - Footprints - regional outputs from an LPDM model (e.g. NAME)
 - Emissions/Flux - estimates of species emissions within a region
 - *[+Boundary conditions - to be added+]*
 - Global CTM output (e.g. GEOSChem)

These inputs must adhere to an expected format and are expected to minimally contain a fixed set of inputs.

*At the moment, the expected format for these files is created through standard methods from within the ACRG repository.*

+++

## Loading data sources into the object store

For this tutorial we will again set up a temporary object store to store our data.

See [1_Adding_observation_data.ipynb](1_Adding_observation_data.ipynb) for more details and advice on how to create a more permanent object store. Once a permanent object store is set up, these steps would only need to be performed once. Any added data can then be retrieved using searches.

```{code-cell} ipython3
import os
import tempfile

tmp_dir = tempfile.TemporaryDirectory()
os.environ["OPENGHG_PATH"] = tmp_dir.name   # temporary directory
```

For this, we will add observation, footprint and flux data to the object store. This data relates to Tacolneston (TAC) site within the DECC network and the area around Europe (EUROPE domain).

```{code-cell} ipython3
from openghg.client import process_obs

obs_file = "../data/DECC/tac.picarro.1minute.100m.201208.dat"

site="tac" 
network="DECC"
height="100m"
species="ch4"

process_obs(files=obs_file, data_type="CRDS", site=site, network=network, inlet=height)
```

```{code-cell} ipython3
from openghg.client import process_footprint

fp_file_path = "../data/footprints/TAC-100magl_EUROPE_201208.nc"

site="tac" 
height="100m"
domain="EUROPE"
model="NAME"

process_footprint(files=fp_file_path, site=site, height=height, domain=domain, model=model)
```

```{code-cell} ipython3
from openghg.util import retrieve_example_data

flux_data = retrieve_example_data(path="flux/ch4-ukghg-all_EUROPE_2012.tar.gz")
```

```{code-cell} ipython3
from openghg.util import retrieve_example_data
from openghg.client import process_flux

flux_data = retrieve_example_data(path="flux/ch4-ukghg-all_EUROPE_2012.tar.gz")

site="tac" 
domain="EUROPE"
date = "2012"

source_waste = "waste"
source_energyprod = "energyprod"

flux_data_waste = [filename for filename in flux_data if source_waste in str(filename)][0]
flux_data_energyprod = [filename for filename in flux_data if source_energyprod in str(filename)][0]

process_flux(files=flux_data_waste, species=species, source=source_waste, domain=domain, date=date)
process_flux(files=flux_data_energyprod, species=species, source=source_energyprod, domain=domain, date=date)
```

## Creating a model scenario

With this ancillary data, we can start to make comparisons between model data, such as bottom-up inventories, and our observations. This analysis is based around a `ModelScenario` class which can be created to link together observation, footprint and emissions data.

*Boundary conditions and other model data will be added soon*

Above we loaded observation data from the Tacolneston site into the object store. We also added an associated footprint (sensitivity map) and anthropogenic emissions maps both for a domain defined over Europe.

To access and link this data we can set up our `ModelScenario` instance using a similiar set of keywords. In this case we have also limited ourselves to a date range:

```{code-cell} ipython3
from openghg.analyse import ModelScenario

start_date = "2012-08-01"
end_date = "2012-09-01"

scenario = ModelScenario(site=site, 
                         inlet=height, 
                         domain=domain, 
                         species=species, 
                         source=source_waste, 
                         start_date=start_date,
                         end_date=end_date)
```

Using these keywords, this will search the object store and attempt to collect and attach observation, footprint and flux data. This collected data will be attached to your created `ModelScenario`. For the observations this will be stored as the `ModelScenario.obs` attribute. This will be an `ObsData` object which contains metadata and data for your observations:

```{code-cell} ipython3
scenario.obs
```

To access the undelying xarray Dataset containing the observation data use `ModelScenario.obs.data`:

```{code-cell} ipython3
ds = scenario.obs.data
```

The `ModelScenario.footprint` attribute contains the linked FootprintData (again, use `.data` to extract xarray Dataset):

```{code-cell} ipython3
scenario.footprint
```

And the `ModelScenario.fluxes` attribute can be used to access the FluxData. Note that for `ModelScenario.fluxes` this can contain multiple flux sources and so this is stored as a dictionary linked to the source name:

```{code-cell} ipython3
scenario.fluxes
```

An interactive plot for the linked observation data can be plotted using the `ModelScenario.plot_timeseries()` method:

```{code-cell} ipython3
scenario.plot_timeseries()
```

You can also set up your own searches and add this data directly.

```{code-cell} ipython3
from openghg.retrieve import get_obs_surface, get_footprint, get_flux

# Extract obs results from object store
obs_results = get_obs_surface(site=site,
                              species=species,
                              inlet=height,
                              start_date="2012-08-01",
                              end_date="2012-09-01")

# Extract footprint results from object store
footprint_results = get_footprint(site=site,
                                  domain=domain,
                                  height=height,
                                  start_date="2012-08-01",
                                  end_date="2012-09-01")

# Extract flux results from object store
flux_results = get_flux(species=species,
                        domain=domain,
                        source=source_waste)
```

```{code-cell} ipython3
scenario_direct = ModelScenario(obs=obs_results, footprint=footprint_results, flux=flux_results)
```

*You can create your own input objects directly and add these in the same way. This allows you to bypass the object store for experimental examples. At the moment these inputs need to be `ObsData`, `FootprintData` or `FluxData` objects (can be created using classes from openghg.dataobjects) but simpler inputs will be made available*.

One benefit of this interface is to reduce searching the database if the same data needs to be used for multiple different scenarios.

+++

## Comparing data sources

Once your `ModelScenario` has been created you can then start to use the linked data to compare outputs. For example we may want to calculate modelled observations at our site based on our linkec footprint and emissions data:

```{code-cell} ipython3
modelled_observations = scenario.calc_modelled_obs()
```

This could then be plotted directlt using the xarray plotting methods:

```{code-cell} ipython3
modelled_observations.plot()  # Can plot using xarray plotting methods
```

To compare the these modelled observations to the ovbservations themselves, the `ModelScenario.plot_comparison()` method can be used:

```{code-cell} ipython3
scenario.plot_comparison()
```

The `ModelScenario.footprints_data_merge()` method can also be used to created a combined output, with all aligned data stored directly within an `xarray.Dataset`:

```{code-cell} ipython3
combined_dataset = scenario.footprints_data_merge()
combined_dataset
```

When the same calculation is being performed for multiple methods, the last calculation is cached to allow the outputs to be produced more efficiently. This can be disabled for large datasets by using `cache=False`.

+++

For a `ModelScenario` object, different analyses can be performed on this linked data. For example if a daily average for the modelled observations was required, we could calculate this setting our `resample_to` input to `"1D"` (matching available pandas time aliases):

```{code-cell} ipython3
modelled_observations_12H = scenario.calc_modelled_obs(resample_to="1D")
modelled_observations_12H.plot()
```

To allow comparisons with multiple fluxes inputs, more than one flux source can be linked to your `ModelScenario`. This can be either be done upon creation or can be added using the `add_flux()` method. When calculating modelled observations, these flux sources will be aligned in time and stacked to create a total output:

```{code-cell} ipython3
scenario.add_flux(species=species, domain=domain, source=source_energyprod)
```

```{code-cell} ipython3
scenario.plot_comparison()
```

Output for individual sources can also be created by specifying the `sources` as an input:

```{code-cell} ipython3
# Included recalculate option to ensure this is updated from cached data.
modelled_obs_energyprod = scenario.calc_modelled_obs(sources="energyprod", recalculate=True)
modelled_obs_energyprod.plot()
```

*Plotting functions to be added for 2D / 3D data*

+++

---
