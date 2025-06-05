#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def get_erddap_data(erddap_url, dataset, data_protocol="griddap", 
                    variables=None, constraints=None, timeout_maxretries=5):
    """
    Function: get_erddap_data
    This function uses the erddapy python library to access data from ERDDAP servers,
    and to return it to users in convenient formats for python users.
    Data can be pulled from "tabledap" or "griddap" formats, with different
    output types, depending on the dap type.
    
    Author: Seth Travis, setht1@uw.edu
            NorthWest Environmental Moorings Group
            University of Washington, Applied Physics Laboratory
            https://nwem.apl.washington.edu/index.shtml
            2023
       
    
    Inputs:
    erddap_url    - The url address of the erddap server to pull data from
    variables     - The selected variables within the dataset.
    data_protocol - The erddap data protocol for the chosen dataset.
                    Options include "tabledap" or "griddap"
                    The default option is given as "griddap"
    dataset       - The ID for the relevant dataset on the erddap server
                    If no variables are given, it is assumed that all variables
                    will be pulled.
    constraints   - These are set by the user to help restrict the data pull
                    to only the area and timeframe of interest.
                    If no constraints are given, all data in a dataset is pulled.
                    Constraints should be given as a dictionary, where
                    each entry is a bound and/or selection of a specific axis variable
                    Exs. {"longitude<=": "min(longitude)+10", "longitude>=": "0"}
                         {"longitude=": "140", "time>=": "max(time)-30"}
    
    Outputs:
    erddap_data   - This variable contains the pulled data from the erddap server.
                    If the data_protocol is "griddap",  then erddap_data is an xarray dataset
                    If the data_protocol is "tabledap", then erddap_data is a pandas dataframe
                    
    
    Example Implementation:
    
    erddap_url = "https://nwem.apl.washington.edu/erddap/"
    dataset_id = "orca1_L2_gridded_025"
    constraints = {
            "cast_start_time>=": "2022-01-01T00:00:00Z",
            "cast_start_time<=": "2022-12-31T23:59:59Z"
        }
    variables = [
            "sea_water_temperature",
            "sea_water_practical_salinity",
            "sea_water_sigma_theta" 
        ]
    
    grid_data = get_erddap_data(erddap_url=erddap_url, dataset=dataset_id, 
                                data_protocol='griddap', 
                                variables=variables, constraints=constraints)
    
    """
    
    
    from erddapy import ERDDAP
    import pandas as pd
    import xarray as xr
    
    import time
    
    
    
    ############################################
    # Set-up the connection to the ERDDAP server
    ############################################
    
    # Connect to the erddap server
    e = ERDDAP(server=erddap_url, protocol=data_protocol, response='csv')
    
    # Identify the dataset of interest
    e.dataset_id = dataset
    
    
    #########################################
    # Pull the data, based upon protocol type
    #########################################
    
    download_attempt = 0
    
    # GRIDDAP Protocol
    if data_protocol == "griddap":
        
        # Initialize the connection
        e.griddap_initialize()

        # Update the constraints
        if constraints is not None:
            e.constraints.update(constraints)
            e.griddap_initialize()
            
        # Update the selection of the variables
        if variables is not None:
            e.variables = variables

        while download_attempt < timeout_maxretries:
            try:
                erddap_data = e.to_xarray()
                download_attempt = timeout_maxretries
            except Exception as error_msg:
                print('Connection error: ')
                print(error_msg)
                erddap_data = None
                download_attempt = download_attempt + 1
                print('   ...Download attempt #' + str(download_attempt) + ' timed out. Wait 10 seconds and try again.')
                time.sleep(10)
    
    # TABLEDAP Protocol
    elif data_protocol == "tabledap":

        # Update the constraints
        if constraints is not None:
            e.constraints = constraints
            
        # Update the selection of the variables
        if variables is not None:
            e.variables = variables

        while download_attempt < timeout_maxretries:
            try:
                erddap_data = e.to_pandas()
                download_attempt = timeout_maxretries
            except Exception as error_msg:
                print('Connection error: ')
                print(error_msg)
                erddap_data = None
                download_attempt = download_attempt + 1
                print('   ...Download attempt #' + str(download_attempt) + ' timed out. Wait 10 seconds and try again.')
                time.sleep(10)
    
            
    # Invalid protocol given
    else:
        print('Invalid ERDDAP protocol. Given protocol is: ' + data_protocol)
        print('Valid protocols include "griddap" or "tabledap". Please restart and try again with a valid protocol')
        erddap_data = None
    
    
    #############################
    if erddap_data is None:
        print('Error occurred in data download. No data has been accessed.')
    
    return erddap_data

