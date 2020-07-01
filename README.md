[![Build Status](https://dev.azure.com/wm19361/HUGS/_apis/build/status/hugs-cloud.hugs?branchName=devel)](https://dev.azure.com/wm19361/HUGS/_build/latest?definitionId=1&branchName=devel)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## HUGS - HUb for greenhouse Gas data Science 

HUGS is a cloud-based data analysis “hub” for greenhouse gas measurements, modelling and data analysis, funded as a NERC “Constructing a Digital Environment” feasibility study. We aim to streamline the process for greenhouse gas data sharing, analysis and visualisation and add value through automated processes such as atmospheric chemistry transport model runs associated with data.

The HUGS Cloud platform was developed as part of a project to create a platform that would allow researchers in atmospheric chemistry to use the power of the cloud to upload, process and analyse their data.

### Processing

HUGS is capable of processing many different types of data including AGAGE/ICOS/NOAA/EUROCOM amongst others. It is built with modularity in mind so it is easy to create new modules to process different types of data. 

### Storage and retrieval

The processed data is stored within a cloud object store which allows for large amounts of data to be easily stored and retrieved. As cloud storage is utilised datasets can grow as new data is added to them with no limits on the size of a dataset. We are currently using the Oracle object store but we have built the platform using open source software to be cloud agnostic. Data stored on the platform is chunked to allow efficient storage and retrieval. This makes it quick to pull a week's worth of data from a decade of measurements.

### Analyis

Using notebooks hosted on our [JupyterHub](https://hub.hugs-cloud.com) it is possible to query the object store and analyse data within the cloud. This allows analysis and processing of large datasets on any computational device as all processing is done on scalable cloud instances.

![Jupyter notebook analysis](https://hugs-cloud.com/assets/images/HUGS_notebook_interface.jpg)

### Compliance

We ensure data stored on the platform can be exported as [CF compliant](http://cfconventions.org/) NetCDF files. Here we utilise the [CF Checker](https://github.com/cedadev/cf-checker) tool to ensure that all datasets hold the correct attributes for all data contained within them.

### Cloud Hosted

Currently the HUGS platform can be accessed through our [JupyterHub](https://hub.hugs-cloud.com). To register for an account please fill out our [registration form](https://hugs-cloud.com/registration/). 

### Open-source technologies

As this is an open source project under the Apache licence, all technologies on which the platform is based are readily available and open source themselves. We harness the power of the [Fn](https://fnproject.io) serverless framework for processing and retrieval of data and our JupyterHub based analysis platform is orchestrated using Kubernetes. For more information on our use of these and other open source technologies please see the documentation.
