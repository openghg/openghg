## Clone

Please clone Acquire into the same top level directory so the folder structure looks like this

```
/a_directory
	/acquire # Cloned Acquire folder
	/hugs    # Cloned HUGS folder

```

`git clone https://github.com/chryswoods/acquire.git`
`git clone https://github.com/chryswoods/hugs.git`

Move into the acquire folder and change to the `devel` branch

`git checkout devel`

Now move into the hugs directory and change to the `devel` branch

`git checkout devel`


## Install dependencies

From inside the hugs directory please run 

`pip install -r requirements.txt` 
or 
`conda install --file requirements.txt`

## Post-install

bqplot and ipyleaflet must be enabled using the following commands

```
jupyter nbextension install --py --symlink --sys-prefix bqplot
jupyter nbextension enable --py --sys-prefix bqplot
jupyter nbextension enable --py --sys-prefix ipyleaflet
```

## Register with HUGS

In the hugs directory under `user/notebooks/account` run `jupyter notebook` and open the `register.ipynb` notebook. This notebook will guide you through the process of creating an account. You will need a 2FA authentication app like andOTP or Google Authenticator.

Now test your newly created account using the `login.ipynb` notebook.

## Use HUGS

A simple graphical interface to the HUGS service is available in the `HGUS_interface.ipynb` notebook within the `user/notebooks` directory. Please scroll down until you see the 'Scroll to here' text and then do Cell -> Run All Above from the toolbar. A simple GUI that allows interaction with functions running on the cloud should appear.

### Clearing Datasources

Please remember to click the `Clear Datasources` button before uploading the BSD and HFD data files. This must be done as Datasources are still being assigned random UUIDs. This leads to the same data being written to multiple Datasources and errors when recombining data. This will be fixed with the assignment of fixed UUIDs for specific Datasources.
