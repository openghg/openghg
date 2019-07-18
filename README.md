# Hub for UK Greenhouse Gas Science
Repository for the HUGS project

# Installation
HUGS currently uses a testing branch of the Acquire package and requires acquire to be cloned into the same folder as HUGS

For example 

```
/development/acquire
/development/HUGS
```

HUGS will look for Acquire at `../acquire`

1. `git clone https://github.com/chryswoods/acquire.git`
2. `git clone https://github.com/chryswoods/hugs.git`
3. `cd acquire`
4. `git checkout testingObjStore`


# Developers
If you want to work with HUGS and want to write a processing module for a certain type of data format please see the `_template.py` file in the Modules directory. This gives an outline of the way a class should be written to interact with the platform.

Please make a new branch for each feature you create. Each function should also have unit tests in the respective directory
in `test`.

After finishing your feature branch please submit a pull request to merge into devel.

# Setting up nbstripout after cloning
Jupyter notebook output may be automatically stripped on commit through the use of the nbstripout tool.

To setup the nbstripout tool after cloning this repository please run

`python tools/trust-origin-git-config -e`

