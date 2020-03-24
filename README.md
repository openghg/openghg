## Install

Please clone the HUGS repository to your computer using 

`git clone https://github.com/chryswoods/hugs.git`

Change into the `hugs` directory and then install using `pip`

`pip install .`

HUGS will soon be available on `PyPi` for installation using `pip`.

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