#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 16:19:30 2019

@author: rt17603
"""

## Quick bit of code to
#  Open a footprint
#  Display the footprint
#  Open an emissions file
#  Display the emissions


import os
import glob
import xarray as xray
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import cartopy.crs as ccrs
from cartopy.feature import BORDERS


data_path = "/data/shared"
fp_directory = os.path.join(data_path,'LPDM/fp_NAME/')

def domain_volume(domain,fp_directory=fp_directory):
    '''
    The domain_volume function extracts the volume (lat, lon, height) within a domain from a related footprint file.
    
    Args:
        domain (str) : 
            Domain of interest (e.g. one of 'AUSTRALIA', 'CARIBBEAN','EASTASIA','EUROPE','NAMERICA','PACIFIC',
            'SOUTHAFRICA','SOUTHASIA','WESTUSA')
        fp_directory (str, optional) : 
            fp_directory can be specified if files are not in the default directory. 
            Must point to a directory which contains subfolders organized by domain.
        
    Returns:
        xarray.DataArray (3): 
            Latitude, longitude, height
    '''
    directory = os.path.join(fp_directory,domain)
    print(f"Looking for files in {directory}")
    listoffiles = glob.glob(os.path.join(directory,"*"))
    if listoffiles:
        filename = listoffiles[0]
        print('Using footprint file: {0} to extract domain'.format(filename))
        with xray.open_dataset(filename) as temp:
            fields_ds = temp.load()
        
        fp_lat = fields_ds["lat"].values
        fp_lon = fields_ds["lon"].values
        fp_height = fields_ds["height"].values
    
        return fp_lat,fp_lon,fp_height     
    else:
        raise Exception('Cannot extract volume for domain: {1}. No footprint file found within {0}'.format(directory,domain))
        #return None

def plot_domain(ax, lat_bounds=[],lon_bounds=[],domain=None,coastline=True,borders=True,
                fig=None,subplot=[1,1,1],figsize=None,show=True):
    '''
    The plot_domain function plots an area specified by the lat_bounds and lon_bounds and the coastline within tha domain.
    Projection: PlateCarree/equirectangular
    Either lat_bounds and lon_bounds OR domain must be specified.
    
    Args:
        lat_bounds : Lower and upper latitude bounds (in degrees) (2-item list).
        lon_bounds : Lower and upper longitude bounds (in degrees) (2-item list)
        domain     : domain of interest (e.g. one of 'AUSTRALIA', 'CARIBBEAN','EASTASIA','EUROPE','NAMERICA','PACIFIC','SOUTHAFRICA',
                     'SOUTHASIA','WESTUSA')
                     Note: domain takes precedence over any lat_bounds or lon_bounds specified.
        coastline  : Whether or not to plot the coastline within this domain. Default=True
        borders    :
        fig
        subplot
        show       : Whether or not to immediately plot the domain using plt.show() or to leave within the buffer. Default=True
    
    Returns:
        fig,ax : plt.figure and plt.axis object
        
        If show:
             matplotlib interactive plot of specified domain
        
    '''
    # if not fig:
    #     fig = plt.figure(figsize=figsize)
    
    #ax = plt.axes(projection=ccrs.PlateCarree())
    # ax = fig.add_subplot(*subplot, projection=ccrs.PlateCarree())
    
    if domain:
        fp_lat,fp_lon,fp_height = domain_volume(domain, fp_directory=".")
        lat_bounds = [np.min(fp_lat),np.max(fp_lat)]
        lon_bounds = [np.min(fp_lon),np.max(fp_lon)]
    
    ax.set_extent((lon_bounds[0],lon_bounds[-1],lat_bounds[0],lat_bounds[-1]),crs=ccrs.PlateCarree())
    if coastline:
        ax.coastlines(color="0.2")
    if borders:
        ax.add_feature(BORDERS,edgecolor="0.5")
    
    # if show:
    #     plt.show()
    
    return fig, ax


if __name__=="__main__":

    ## Footprint plot
    
    fp_directory = "."
    fp_filename = os.path.join(fp_directory,"WAO-20magl_EUROPE_201511.nc")

    ds = xray.open_dataset(fp_filename)

    print("Footprint file as a dataset")
    print(ds)

    domain = "EUROPE"    

    fig = plt.figure()
    ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

    plot_domain(ax=ax, domain=domain)
    
    fp_name = "fp"
    lon_name = "lon"
    lat_name = "lat"
    
    cmap = cm.get_cmap("inferno")
    levels = np.linspace(np.percentile(ds[fp_name].values,5),np.percentile(ds[fp_name].values,95),20)

    long_values = ds[fp_name][lon_name].values
    lat_values = ds[fp_name][lat_name].values
    zero_values = ds[fp_name][:,:,0].values
    
    # ax.contourf(long_values, lat_values, zero_values, cmap=cm.get_cmap("inferno"), levels=levels)

    ax.contour(long_values, lat_values, zero_values, cmap=cm.get_cmap("inferno"), levels=levels)

    plt.show()
    
    # Emissions plot
    
    # emissions_directory = "."
    # emissions_filename = os.path.join(emissions_directory,"ch4-enteric-fermentation_EUROPE_2010.nc")
    
    # ds_emissions = xray.open_dataset(emissions_filename)
    
    # print("Emissions file as a dataset")
    # print(ds_emissions)

    # em_fig = plt.figure()
    # em_ax = em_fig.add_subplot(111, projection=ccrs.PlateCarree())
    
    # # plot_domain(ax=em_ax, domain=domain)
    
    # emissions_name = "flux"
    # lon_name = "lon"
    # lat_name = "lat"
    
    # emissions = ds_emissions[emissions_name]
    
    # cmap = cm.get_cmap("viridis")
    # lower = np.percentile(emissions.values[emissions.values>0],5)
    # upper = np.percentile(emissions.values,95)
    # levels = np.linspace(lower,upper,20)
    
    # em_ax.contourf(ds_emissions[emissions_name][lon_name].values, ds_emissions[emissions_name]
    #                [lat_name].values, ds_emissions[emissions_name][:, :, 0].values, cmap=cmap, levels=levels)
    
    # plt.show()
    
    
