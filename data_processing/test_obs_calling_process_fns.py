
"""
Created on Mon Jan 14 11:31:20 2019

@author: al18242
"""
import acrg_obs
import pytest
import numpy as np
import pandas as pd
import xarray as xr
import os
import shutil
import glob

test_dir = os.path.dirname(os.path.abspath(__file__))
# def checkUnits(data):
#     '''
#     Check the data units are numbers as needed in footprints_data_merge
    
#     Inputs:
#         data - output from get_obs
#     '''
#     assert isinstance(data[".units"], int) or isinstance(data[".units"], float)
    
# def checkFilenames(fnames):
#     '''
#     lists of file names should be strings, without / in them
#     '''
#     for f in fnames:
#         assert isinstance(f, str)
#         assert "/" not in f
    
# def test_get_obs_structure():
#     '''
#     Test that the get_obs function returns the correct output structure
#     '''
#     start_date = "20160101"
#     end_date = "20170101"
#     sites = ["MHD", "GOSAT-UK"]
    
#     recreated_data = acrg_obs.get_obs(sites, "CH4", start_date, end_date,
#                                       data_directory="files/obs/",
#                                       keep_missing=True, average=["1H", None],
#                                       max_level=17)
    
#     assert isinstance(recreated_data, dict)
    
#     for site in sites:
#         assert site in recreated_data
    
    
# def test_get_obs_site():
#     '''
#     call get_obs on an example data file and test the properties are as expected
    
#     Called with keep_missing=True and average=["1H"] - these are then used to check that
#     the start and end indicies within the output are as expected
#     '''
#     start_date = "20160101"
#     end_date = "20160201"
#     recreated_data = acrg_obs.get_obs(["MHD"], "CH4", start_date, end_date,
#                                       data_directory="files/obs/",
#                                       keep_missing=True, average=["1H"])
    
#     checkUnits(recreated_data)
    
#     #test the date range is as expected
#     assert np.amax(recreated_data["MHD"].index) == pd.to_datetime(end_date)-pd.Timedelta(hours=1)
#     assert np.amin(recreated_data["MHD"].index) == pd.to_datetime(start_date)
#     assert "mf" in recreated_data["MHD"].columns.values
#     assert ("dmf" in recreated_data["MHD"].columns.values) or ("vmf" in recreated_data["MHD"].columns.values)
    
# def test_get_obs_gosat():
#     '''
#     call get_obs on an example data file and test the properties are as expected
    
#     Date indicies are checked to be within the correct bounds as satellite measurements are not continous
#     '''
#     start_date = "20160602"
#     end_date = "20160604"
#     recreated_data = acrg_obs.get_obs(["GOSAT-UK"], "CH4", start_date, end_date,
#                                       data_directory="files/obs/",
#                                       max_level = 17)
    
#     checkUnits(recreated_data)
    
#     #test the date range is as expected
#     assert np.amax(recreated_data["GOSAT-UK"].index) < pd.to_datetime(end_date)
#     assert np.amin(recreated_data["GOSAT-UK"].index) >= pd.to_datetime(start_date)
#     assert "mf" in recreated_data["GOSAT-UK"].columns.values
#     assert ("dmf" in recreated_data["GOSAT-UK"].columns.values) or ("vmf" in recreated_data["GOSAT-UK"].columns.values)

# def test_dmf_average():
#     '''
#     Test if the quadratic sum function returns the expected values for empty and non empty inputs
#     '''
#     inputs = np.array([3.0, 4.0])
#     assert acrg_obs.read.quadratic_sum(inputs) == 2.5
#     inputs = np.array([])
#     assert np.isnan(acrg_obs.read.quadratic_sum(inputs))

# def test_is_number():
#     '''
#     Test that is_number can parse units correctly
#     '''
#     assert acrg_obs.read.is_number(1e-9) == True
#     assert acrg_obs.read.is_number("1e-9") == True
    
# def test_listsearch():
#     '''
#     Test the listsearch function is able to get a correct string from a dictionary of synonyms, 
#     and that it reutrns None when this is not possible
#     '''
#     correctString = "Correct"
#     synonyms = {
#             "Correct": {
#                     "alt":["AlsoCorrect"]
#                     }
#             }
#     assert acrg_obs.read.listsearch(["Correct"], correctString, synonyms) is "Correct"
#     assert acrg_obs.read.listsearch(["correct"], correctString, synonyms) is "correct"
#     assert acrg_obs.read.listsearch(["AlsoCorrect"], correctString, synonyms) is "AlsoCorrect"
#     assert acrg_obs.read.listsearch(["Alsocorrect"], correctString, synonyms) is "Alsocorrect"
#     assert acrg_obs.read.listsearch(["Wrong", "AlsoCorrect"], correctString, synonyms) is "AlsoCorrect"
#     assert acrg_obs.read.listsearch(["Wrong"], correctString, synonyms) is None
    
#     assert acrg_obs.read.listsearch(["Correct"], "AlsoCorrect", synonyms) is "Correct"
    
#     with pytest.raises(ValueError):
#         acrg_obs.read.listsearch(["Correct"], "NotCorrect", synonyms)
        
        
    
# def test_file_search_and_split():
#     '''
#     Test that file_search_and_split correctly returns lists, where the filename is seperated from directory
#     '''
#     fnames, split = acrg_obs.read.file_search_and_split("files/obs/GOSAT/GOSAT-UK/*.nc")
    
#     assert isinstance(fnames, list)
#     assert isinstance(split, list)
#     checkFilenames(fnames)
    
#     assert len(fnames) == len(split)
    
# def test_file_list():
#     '''
#     Test file-list returns the correct types
#     '''
#     directory, fnames = acrg_obs.read.file_list("MHD", "CH4", "AGAGE", data_directory="files/obs")
#     assert isinstance(directory, str)
#     assert isinstance(fnames, list)
#     checkFilenames(fnames)
    
def test_process_utils_attributes():    
    '''
    Test the acrg_obs.utils.attributes function
    
    Just makes sure that the function is returning a dataset with a few select 
    things changed. Could make this more comprehensive.
    '''
    
    attributes_test_file = os.path.join(test_dir,
                                        "files/obs/process_attributes_input.nc")

    with xr.open_dataset(attributes_test_file) as ds:
        ds.load()
    
    out = acrg_obs.utils.attributes(ds, "CFC-113", "MHD",
                                   global_attributes = {"test": "testing"},
                                   units = "ppt",
                                   scale = "TEST",
                                   sampling_period = 60,
                                   date_range = ["2000-01-01", "2000-01-10"])

    assert "cfc113" in out.variables
    assert "time" in out.variables
    assert out.time.attrs["sampling_period_seconds"] == 60
    assert "seconds since" in out.time.encoding["units"]
    assert out.attrs["Calibration_scale"] == "TEST"
    assert out.attrs['station_long_name'] == u'Mace Head, Ireland'
    assert out.attrs['test'] == u'testing'
    assert out.cfc113.units == "1e-12"


def test_obs_process_gc():

    gc_files_directory = os.path.join(test_dir,
                                      "files/obs/GC")
    
    acrg_obs.process_gcwerks.gc("CGO", "medusa", "AGAGE",
                                input_directory = gc_files_directory,
                                output_directory = gc_files_directory,
                                version = "TEST")

    # Test if CGO directory has been created
    assert os.path.exists(os.path.join(gc_files_directory, "CGO"))
    
    # Check that enough files have been created
    assert len(glob.glob(os.path.join(gc_files_directory, "CGO/*.nc"))) == 56
    
    # As an example, get CF4 data
    cf4_file = os.path.join(gc_files_directory,
                            "CGO/AGAGE-GCMSMedusa_CGO_20180101_cf4-70m-TEST.nc")
    # Check if file exists
    assert os.path.exists(cf4_file)
    
    # Open dataset
    with xr.open_dataset(cf4_file) as f:
        ds = f.load()
    
    # Check a particular value (note that time stamp is 10 minutes before analysis time,
    # because in GCWerks files, times are at the beginning of the sampling period)
    assert np.allclose(ds.sel(time = slice("2018-01-01 04:33", "2018-01-01 04:35")).cf4.values,
                       np.array(83.546))

    assert np.allclose(ds.sel(time = slice("2018-01-20", "2018-01-20"))["cf4 repeatability"].values[0:1],
                       np.array(0.03679))

    # clean up
    shutil.rmtree(os.path.join(gc_files_directory, "CGO"))
    
def test_obs_process_crds():

    gc_files_directory = os.path.join(test_dir,
                                      "files/obs/CRDS")
    
    acrg_obs.process_gcwerks.crds("BSD", "DECC",
                                input_directory = gc_files_directory,
                                output_directory = gc_files_directory,
                                version = "TEST")

    # Test if CGO directory has been created
    assert os.path.exists(os.path.join(gc_files_directory, "BSD"))
    
    # Check that enough files have been created
    assert len(glob.glob(os.path.join(gc_files_directory, "BSD/*.nc"))) == 3
    
    # As an example, get CF4 data
    ch4_file = os.path.join(gc_files_directory,
                            "BSD/DECC-CRDS_BSD_20140130_ch4-248m-TEST.nc")
    # Check if file exists
    assert os.path.exists(ch4_file)
    
    # Open dataset
    with xr.open_dataset(ch4_file) as f:
        ds = f.load()
    
    # Check a particular value (note that time stamp is 10 minutes before analysis time,
    # because in GCWerks files, times are at the beginning of the sampling period)
    assert np.allclose(ds.sel(time = slice("2014-01-30 14:00:00", "2014-01-30 14:01:00")).ch4.values,
                       np.array(1953.88))
    assert np.allclose(ds.sel(time = slice("2014-01-30 14:00:00", "2014-01-30 14:01:00"))["ch4 variability"].values,
                       np.array(0.398))

    # clean up
    shutil.rmtree(os.path.join(gc_files_directory, "BSD"))
    
    

