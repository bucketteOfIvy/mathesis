
import pandas as pd
import city_helpers as ch
import geopandas as gpd
from os.path import exists as path_exists
import requests as rq

## Assume all above are not in the environment, as otherwise this
    ## will actually be brutal
RELEVANT_TERMS = ['streetlight', 'street light', 'dumping', 'graffiti']
    
DESIRED_COLUMNS = ['ID', 'city', 'latitude', 'longitude', 'request_type']
SAVE_LOC = '../data/311/unjoined'

soda_params = {
    'la_2018': {
        'url': 'https://data.lacity.org/resource/h65r-yf5i.json',
        'params' :{
            '$limit': 50000,
            '$order': 'CreatedDate DESC'
        },
        'offset_param': '$offset',
        'columns': ['srnumber', 'requesttype', 'latitude', "longitude"],
        'file_name': 'la_311_18.json'
    },
    'la_2019': {
        'url':'https://data.lacity.org/resource/pvft-t768.json',
        'params' :{
            '$limit': 50000,
            '$order': 'CreatedDate DESC'
        },
        'offset_param': '$offset',
        'columns': ['srnumber', 'requesttype', 'latitude', "longitude"],
        'file_name': 'la_311_19.json'
        },
    'la_2020': {
        'url': 'https://data.lacity.org/resource/rq3b-xjk8.json',
        'params' :{
            '$limit': 50000,
            '$order': 'CreatedDate DESC'
        },
        'offset_param': '$offset',
        'columns': ['srnumber', 'requesttype', 'latitude', "longitude"],
        'file_name': 'la_311_20.json'  
    },
    'la_2021': {
        'url': 'https://data.lacity.org/resource/97z7-y5bt.json',
        'params' :{
            '$limit': 50000,
            '$order': 'CreatedDate DESC'
        },
        'offset_param': '$offset',
        'columns': ['srnumber', 'requesttype', 'latitude', "longitude"],
        'file_name': 'la_311_21.json'       
    },
    'la_2022': {
        'url': 'https://data.lacity.org/resource/i5ke-k6by.json',
        'params': {
            '$limit': 50000,
            '$order': 'CreatedDate DESC'
        },
        'offset_param': '$offset',
        'columns': ['srnumber', 'requesttype', 'latitude', "longitude"],
        'file_name': 'la_311_22.json'    
    },
    'chicago': {
        'url': 'https://data.cityofchicago.org/resource/v6vf-nfxy.json',
        'params': {
            '$limit': 50000,
            "$where": "created_date between '2018-01-01T00:00:00' and '2022-12-31T23:59:59'"
        },
        'offset_param': '$offset',
        'columns': ['sr_number', 'sr_type', 'latitude', "longitude"],
        'file_name': 'chicago_311_18_22.json'  
    },
    'nyc': {
        'url': 'https://data.cityofnewyork.us/resource/erm2-nwe9.json',
          'params': {
            '$limit': 50000,
            "$where": "created_date between '2018-01-01T00:00:00' and '2022-12-31T23:59:59'"
        },
        'offset_param': '$offset',
        'columns': ['unique_key', 'complaint_type', 'latitude', "longitude"],
        'file_name': 'nyc_311_18_22.json'
    }
}
arcgis_params = {
    'detroit': {
        'url': 'https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/Improve_Detroit_Issues_Test/FeatureServer/0/query?',
        'params': {
            "where": "Created_At >= DATE '2018-01-01' AND Created_At < DATE '2023-01-01'",  # Filter for years 2018 to 2022
            "outFields": "ID, Request_Type_Title,Latitude,Longitude,Created_At",  # Retrieve all fields
            "f": "json",  # Response format
            "resultRecordCount": 50000,  # Number of records per request
            "resultOffset": 0  # Offset for pagination
        },
        'columns': ['ID', 'Request_Type_Title', 'Latitude', 'Longitude'],
        'offset_param': 'resultOffset',
        'file_name': 'detroit_311_18_22.json'
    }
}

if __name__ == "__main__":
    
    # Grab all LA data
    for reqs in soda_params.values():
        loc = f"{SAVE_LOC}/{reqs['file_name']}"
        if not path_exists(loc):
            pd.set_option('display.max_colwidth', None)
            results = ch.request_all_soda(reqs['url'], reqs['params'], 
                                        reqs['columns'], reqs['offset_param'])
            results.to_json(loc)


    phili_cols = ['service_request_id', 'subject', 'lat', 'lon']
    phili_url = 'https://phl.carto.com/api/v2/sql?q=SELECT%20service_request_id,subject,requested_datetime,lat,lon%20FROM%20public_cases_fc%20WHERE%20requested_datetime%20%3E=%20%272018-01-01%27%20AND%20requested_datetime%20%3C=%20%272022-12-31%27'
    phili_save = f"{SAVE_LOC}/phili_311_18_22.json"
    if not path_exists(phili_save):
        response = rq.get(phili_url)
        print('getting phili!')
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")       
        phili_data = pd.DataFrame(response.json()['rows'])
        print(phili_data.head())
        print(phili_data.shape)
        phili_data.to_json(phili_save)


    detroit = arcgis_params['detroit']
    detroit_save = f"{SAVE_LOC}/{detroit['file_name']}"
    if not path_exists(detroit_save):
        df = ch.request_all_arcgis(detroit['url'], detroit['params'],
                                   detroit['columns'], detroit['offset_param'],
                                   paranoid=True)
        
        print(f'Saving data for detroit')
        df.to_json(detroit_save)
    
    print('Cleaning and unifying...')
    
    DESIRED_COLUMNS = ['ID', 'city', 'latitude', 'longitude']

    ### Handle LA
    ladf = pd.concat([pd.read_json(f'../data/311/unjoined/la_311_{year}.json').assign(year=year)
                      for year in range(18, 23)], axis=0)

    ladf = ch.filter_text_col(ladf, 'requesttype', RELEVANT_TERMS)
    ladf['city'] = 'Los Angelos'
    ladf['ID'] = ladf.srnumber.apply(lambda x: f"LA{x}")
    ladf = ladf[DESIRED_COLUMNS]

    df = ladf.copy()
    
    del ladf # for memory purposes

    print("LA cleaned. Next...")
    print(f'Current df shape: {df.shape}')
    print(f'Current df non-head {df[-10:]}')

    ### Detroit
    dtdf = pd.read_json(f'../data/311/unjoined/detroit_311_18_22.json')
    dtdf = ch.filter_text_col(dtdf, 'Request_Type_Title', RELEVANT_TERMS)
    dtdf['city'] = 'Detroit'
    dtdf = dtdf.rename(columns={'Latitude':'latitude', 'Longitude':'longitude'})
    dtdf['ID'] = dtdf.ID.apply(lambda x: f"DT{x}")

    df = pd.concat([df, dtdf[DESIRED_COLUMNS]], axis=0)
    
    del dtdf

    print("Detroit handled! Next...")
    print(f'Current df shape: {df.shape}')
    print(f'Current df non-head {df[-10:]}')
    
    ## Phili
    phdf = pd.read_json('../data/311/unjoined/phili_311_18_22.json')
    phdf = ch.filter_text_col(phdf, 'subject', RELEVANT_TERMS)
    phdf['city'] = 'Philadelphia'
    phdf['ID'] = phdf.service_request_id.apply(lambda x: f"PH{x}")
    
    phdf = phdf.rename(columns={
        'lat':'latitude',
        'lon':'longitude'
    })

    df = pd.concat([df, phdf[DESIRED_COLUMNS]], axis=0)
    
    del phdf

    ## Chicago    
    cdf = pd.read_json('../data/311/unjoined/chicago_311_18_22.json')
    cdf = ch.filter_text_col(cdf, 'sr_type', RELEVANT_TERMS)
    cdf['city'] = 'Chicago'
    cdf['ID'] = cdf.sr_number.apply(lambda x : f"CH{x}")

    df = pd.concat([df, cdf[DESIRED_COLUMNS]], axis=0)

    del cdf

    print('Chicago done! NYC is next...')
    print(f'Current df shape: {df.shape}')
    print(f'Current df non-head {df[-10:]}')

    nyc_df = pd.read_json('../data/311/unjoined/nyc_311_18_22.json')
    nyc_df = nyc_df[['unique_key', 'complaint_type', 'latitude', 'longitude']]
    nyc_df = ch.filter_text_col(nyc_df, 'complaint_type', RELEVANT_TERMS)
    nyc_df['ID'] = nyc_df.unique_key.apply(lambda x: f"NY{x}")
    nyc_df['city'] = 'NYC'

    df = pd.concat([df, nyc_df[DESIRED_COLUMNS]], axis=0)    

    # for chunk in pd.read_json('../data/311/unjoined/nyc_311_18_22.json', lines=True, chunksize=10**3):
    #        
    #     ch.filter_text_col(chunk, 'complaint_type', RELEVANT_TERMS)
    #     chunk['city'] = 'NYC'
    #     chunk['ID'] = chunk['unique_key'].apply(lambda x: f"NY{x}")
    #   
    #     df = pd.concat([df, chunk[DESIRED_COLUMNS]], axis=0)

    del nyc_df

    print(f'Current df shape: {df.shape}')
    print(f'Current df non-head {df[-10:]}')
    
    print("Saving!!")
    df.to_csv('../data/311/311_requests_18_22.csv')

    
    

