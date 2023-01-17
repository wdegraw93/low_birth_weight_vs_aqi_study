import pandas as pd
import numpy as np
import requests
from urllib.parse import urlencode
from yarl import URL

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
key = input('API key for query: ')
beginning_year = int(input('First year of interest: '))
end_year = int(input('Last year of interest (if only want one year put the same year): '))

state_codes = get_state_codes(email,key)
param_codes = get_param_codes_by_class(email, key, 'CRITERIA')
years = np.arange(beginning_year, end_year+1).astype(str)

data_list = []
for state_code, state in state_codes.values[:-2]:
    # Query for county codes at the top to avoid repeated queries
    county_codes = get_county_codes(email,key,state_code)
    for year in years:
        print('--------------------')
        print(f'Collecting data for {state} in {year}')
        print('--------------------')
        for param in param_codes:
            for county in county_codes:
                data_list.append( aqs_api_annual_county(email, key, param, year+'0101', year+'1231', state_code, county) )
                
data = pd.concat(data_list, ignore_index=True)
if beginning_year == end_year:
    data.to_csv(f'data/AQS_county_data_{beginning_year}.csv')
else:
    data.to_csv(f'data/AQS_county_data_{beginning_year}_{end_year}.csv')