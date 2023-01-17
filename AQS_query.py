import pandas as pd
import numpy as np
import requests
from urllib.parse import urlencode
from yarl import URL
from time import sleep
from getpass import getpass

def get_state_codes(email, key):
    '''
    Queries AQS to get the list of state codes
    '''
    base_url = 'https://aqs.epa.gov/data/api/list/states'
    params = {
        'email' : email,
        'key'   : key  
    }
    
    url = URL('?'.join([base_url, urlencode(params)]), encoded=False)
    res = requests.get(url)

    if res.status_code == 200:
        posts = pd.DataFrame(res.json()['Data'])
        return posts
    else:
        print(f'status: {res.status_code}')
    return None
        
def get_county_codes(email, key, state):
    '''
    Queries AQS to get the list of county codes for the given state
    '''
    base_url = 'https://aqs.epa.gov/data/api/list/countiesByState'
    params = {
        'email' : email,
        'key'   : key  ,
        'state' : state
    }
    
    url = URL('?'.join([base_url, urlencode(params)]), encoded=False)
    res = requests.get(url)

    if res.status_code == 200:
        posts = pd.DataFrame(res.json()['Data'])
        return posts['code']
    else:
        print(f'status: {res.status_code}')
    return None
        
def get_param_codes_by_class(email, key, pc):
    '''
    Queries AQS to get the codes representing the parameters in parameter class (pc)
    '''
    base_url = 'https://aqs.epa.gov/data/api/list/parametersByClass'
    params = {
        'email' : email,
        'key'   : key  ,
        'pc'    : pc
    }
    
    url = URL('?'.join([base_url, urlencode(params)]), encoded=False)
    res = requests.get(url)

    if res.status_code == 200:
        posts = pd.DataFrame(res.json()['Data'])
        return posts['code']
    else:
        print(f'status: {res.status_code}')
    return None

def aqs_api_annual_county(email, key, param, bdate, edate, state, county):
    '''
    Query the AQS API to get the annual summary data for the given parameter, state, county
    bdate and edate have to be in the same year for the request to work
    '''
    base_url = 'https://aqs.epa.gov/data/api/annualData/byCounty'
    params = {
        'email' : email,
        'key'   : key  ,
        'param' : param,
        'bdate' : bdate,
        'edate' : edate,
        'state' : state,
        'county': county
    }
    
    url = URL('?'.join([base_url, urlencode(params)]), encoded=False)
    res = requests.get(url)
    
    if res.status_code == 200:
        posts = pd.DataFrame(res.json()['Data'])
        return posts
    else:
        print(f'status: {res.status_code}')
    return None


    
#---------------------------------
email = input('Email for query: ')
key = getpass('API key for query: ')
beginning_year = int(input('First year of interest: '))
end_year = int(input('Last year of interest (if only want one year put the same year): '))
start_state = input('State to begin at, please put in full name. Just hit enter if wish to get all states: ')

state_codes = get_state_codes(email,key)
if start_state!='':
    start_state_index = state_codes[state_codes['value_represented'].str.lower() == start_state.lower()].index[0]
else:
    start_state_index = 0

param_codes = get_param_codes_by_class(email, key, 'CRITERIA').astype(str)
params_1 = ','.join(param_codes[:4].values)
params_2 = ','.join(param_codes[4:].values)
params = [params_1, params_2]

years = np.arange(beginning_year, end_year+1).astype(str)

data_list = []
state_count = start_state_index

# loop over all desired states, years, counties, and parameters. 
# not including Canada, Mexico, and start from whichever state was input (if any)
for state_code, state in state_codes.iloc[start_state_index:].values[:-2]:
    state_count += 1
    
    # check if there is state data already, if so then write it to a csv
    if len(data_list)>0:
        data = pd.concat(data_list, ignore_index=True)
        if beginning_year == end_year:
            data.to_csv(f'data/AQS_county_data_{state_codes["value_represented"].iloc[state_count-2]}_{beginning_year}.csv', index=False)
        else:
            data.to_csv(f'data/AQS_county_data_{state_codes["value_represented"].iloc[state_count-2]}_{beginning_year}_{end_year}.csv', index=False)
    
    # make sure data_list is empty before filling it up again
    data_list=[]
    
    # Query for county codes at the top to avoid repeated queries
    county_codes = get_county_codes(email,key,state_code)
    for year in years:
        print('--------------------')
        print(f'Collecting data for state {state} in year {year} ({state_count}/{len(state_codes[:-2])})')
        print('--------------------')
        county_count = 0
        for county in county_codes:
            county_count += 1
            print(f'County {county_count}/{len(county_codes)}')
            for param in params:
                data_list.append( aqs_api_annual_county(email, key, param, year+'0101', year+'1231', state_code, county) )
                # Have to sleep for API to be happy
                sleep(2)

data = pd.concat(data_list, ignore_index=True)
if beginning_year == end_year:
    data.to_csv(f'data/AQS_county_data_{state}_{beginning_year}.csv', index=False)
else:
    data.to_csv(f'data/AQS_county_data_{state}_{beginning_year}_{end_year}.csv', index=False)