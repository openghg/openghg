# Hub for UK Greenhouse Gas Science
Repository for the HUGS project

# Running HUGS
HUGS currently uses a testing branch of the Acquire package and requires acquire to be cloned into the same folder as HUGS

For example

```
/development/acquire
/development/HUGS
```

HUGS will look for Acquire at `../acquire`

# Setting up nbstripout after cloning
When working with Jupyter notebooks in the repo their contents can be automatically stripped on commit
through the use of the nbstripout tool.

To setup the nbstripout tool after cloning this repository please run

`python tools/trust-origin-git-config -e`

