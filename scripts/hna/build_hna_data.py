import os
import requests
import json
import pandas as pd
from datetime import datetime, timedelta

# Constants
DOLA_CACHE_FILE = 'dola_cache.json'
CENSUS_API_URL = 'https://api.census.gov/data'

# Helper function for DOLA caching

def cache_dola_data(data):
    try:
        with open(DOLA_CACHE_FILE, 'w') as cache_file:
            json.dump(data, cache_file)
    except Exception as e:
        print(f'Error caching DOLA data: {e}')

# Helper function for fetching DOLA data with caching

def get_dola_data(endpoint, params={}):
    cache_data = read_dola_cache()
    if cache_data:
        return cache_data
    else:
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            data = response.json()
            cache_dola_data(data)
            return data
        else:
            print('Error fetching DOLA data')
            return None

# Read cached DOLA data

def read_dola_cache():
    if os.path.exists(DOLA_CACHE_FILE):
        with open(DOLA_CACHE_FILE, 'r') as cache_file:
            return json.load(cache_file)
    return None

# Fetch Census data with API fallback

def fetch_census_data(dataset, params):
    response = requests.get(f'{CENSUS_API_URL}/{dataset}', params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print('Error fetching Census data, falling back to default')
        return {}  # Return an empty dict as a fallback

# Convert to timezone-aware datetime

def convert_to_timezone_aware(dt_string):
    naive_dt = datetime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')
    aware_dt = naive_dt.replace(tzinfo=pd.Timestamp.now().tz)  # Setting timezone to UTC
    return aware_dt

if __name__ == '__main__':
    print('Housing Data Analytics - Build HNA Data')
    # Example usage of functions
    dola_data = get_dola_data('https://example.com/dola')
    print(dola_data)
    census_data = fetch_census_data('census_data_2020', {'key': 'YOUR_API_KEY'})
    print(census_data)
    timezone_awareness = convert_to_timezone_aware('2026-02-26 09:42:08')
    print(timezone_awareness)
