
import pandas as pd
import city_helpers as ch
import geopandas as gpd
from os.path import exists as path_exists

# Goal vars;
# ID, Year, City, latitude, longitude,
DESIRED_COLUMNS = ['ID', 'city', 'year', 'latitude', 'longitude']
SAVE_LOC = '../data/crashes/unjoined'

la_params = {
        'url':'https://data.lacity.org/resource/d5tf-ez2w.json',
        'params': {
            '$where': "date_occ between '2018-01-01T00:00:00' and '2022-12-31T23:59:59'",
            "$limit": 50000,
            "$order": "date_occ DESC"
        },
        'offset_param': '$offset',
        'columns': ['dr_no', 'date_occ', 'crm_cd_desc', 'latitude', 'longitude'],
        'file_name': 'la_crashes_18_22.json'
}
soda_params = {
    'chicago': {
        'url': "https://data.cityofchicago.org/resource/85ca-t3if.json",
        'params' : {
            "$where": "crash_date between '2018-01-01T00:00:00' and '2022-12-31T23:59:59'",
            "$limit": 50000,  # Max records per request
            "$order": "crash_date DESC"
        },
        'offset_param': '$offset',
        'columns': ['crash_record_id', 'crash_date', 'injuries_total', 'latitude', 'longitude'],
        'file_name': 'chicago_crashes_18_22.json'
    },
    'nyc': {
        'url': 'https://data.cityofnewyork.us/resource/h9gi-nx95.json',
        'params': {
            '$where': "crash_date between '2018-01-01T00:00:00' and '2022-12-31T23:59:59'",
            "$limit": 50000,
            "$order": "crash_date DESC"
        },
        'offset_param': '$offset',
        'columns': ['crash_date', 'latitude', 'longitude', 'number_of_persons_killed', 'collision_id'],
        'file_name': 'nyc_crashes_18_22.json'
    },
}

arcgis_params = {
    'phili': {
        'url': 'https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/collision_crash_2018_2022/FeatureServer/0/query?',
        'params': {
            'where': 'crash_year>=2018 AND crash_year<=2022',
            'outFields':"crash_year,crn,longitude,latitude",
            'f':'json',
            'resultRecordCount':2000,
            'resultOffset':0
        },
        'columns':['crash_year','crn','longitude','latitude'],
        'offset_param': 'resultOffset',
        'file_name': 'phili_crashes_18_22.json'
    },
    'detroit22': {
        'url': 'https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/Traffic_Crashes/FeatureServer/12/query?',
        'params': {
            "where": "year >= 2018 AND year <= 2022",  # Filter for years 2018 to 2022
            "outFields": "crash_id,crash_date,latitude,longitude,num_fatal_injuries,year",  # Retrieve all fields
            "f": "json",  # Response format
            "resultRecordCount": 50000,  # Number of records per request
            "resultOffset": 0  # Offset for pagination
        },
        'columns': ['crash_id', 'year', 'latitude', 'longitude'],
        'offset_param': 'resultOffset',
        'file_name': 'detroit_crashes_22.json'
    },
        'detroit21': {
        'url': 'https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/Traffic_Crashes/FeatureServer/11/query?',
        'params': {
            "where": "year >= 2018 AND year <= 2022",  # Filter for years 2018 to 2022
            "outFields": "crash_id,crash_date,latitude,longitude,num_fatal_injuries,year",  # Retrieve all fields
            "f": "json",  # Response format
            "resultRecordCount": 50000,  # Number of records per request
            "resultOffset": 0  # Offset for pagination
        },
        'columns': ['crash_id', 'year', 'latitude', 'longitude'],
        'offset_param': 'resultOffset',
        'file_name': 'detroit_crashes_21.json'
    },
        'detroit20': {
        'url': 'https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/Traffic_Crashes/FeatureServer/10/query?',
        'params': {
            "where": "year >= 2018 AND year <= 2022",  # Filter for years 2018 to 2022
            "outFields": "crash_id,crash_date,latitude,longitude,num_fatal_injuries,year",  # Retrieve all fields
            "f": "json",  # Response format
            "resultRecordCount": 50000,  # Number of records per request
            "resultOffset": 0  # Offset for pagination
        },
        'columns': ['crash_id', 'year', 'latitude', 'longitude'],
        'offset_param': 'resultOffset',
        'file_name': 'detroit_crashes_20.json'
    },
        'detroit19': {
        'url': 'https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/Traffic_Crashes/FeatureServer/9/query?',
        'params': {
            "where": "year >= 2018 AND year <= 2022",  # Filter for years 2018 to 2022
            "outFields": "crash_id,crash_date,latitude,longitude,num_fatal_injuries,year",  # Retrieve all fields
            "f": "json",  # Response format
            "resultRecordCount": 50000,  # Number of records per request
            "resultOffset": 0  # Offset for pagination
        },
        'columns': ['crash_id', 'year', 'latitude', 'longitude'],
        'offset_param': 'resultOffset',
        'file_name': 'detroit_crashes_19.json'
    },
        'detroit18': {
        'url': 'https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/Traffic_Crashes/FeatureServer/8/query?',
        'params': {
            "where": "year >= 2018 AND year <= 2022",  # Filter for years 2018 to 2022
            "outFields": "crash_id,crash_date,latitude,longitude,num_fatal_injuries,year",  # Retrieve all fields
            "f": "json",  # Response format
            "resultRecordCount": 50000,  # Number of records per request
            "resultOffset": 0  # Offset for pagination
        },
        'columns': ['crash_id', 'year', 'latitude', 'longitude'],
        'offset_param': 'resultOffset',
        'file_name': 'detroit_crashes_18.json'
    },
}

if __name__ == "__main__":
    la_loc = f"{SAVE_LOC}/{la_params['file_name']}"
    if not path_exists(la_loc):
        la = ch.request_all_soda_la(la_params['url'], la_params['params'], 
                                    la_params['columns'], la_params['offset_param'])
        la.to_json(la_loc)

    for city, params in arcgis_params.items():

        file_save_loc = f'{SAVE_LOC}/{params["file_name"]}'
        if path_exists(file_save_loc):
            print(f'Data exits for {city}, skipping!')
            continue

        print(f"Requesting data for {city}")
        df = ch.request_all_arcgis(params['url'], params['params'], 
                                params['columns'], params['offset_param'])

        print(f'Saving data for {city}')
        df.to_json(file_save_loc)

    for city, params in soda_params.items():
        file_save_loc = f'{SAVE_LOC}/{params["file_name"]}'
        if path_exists(file_save_loc):
            print(f'Data exits for {city}, skipping!')
            continue
        
        print(f'Requesting data for {city}')
        df = ch.request_all_soda(params['url'], params['params'], 
                                params['columns'], params['offset_param'])

        print(f'Saving data for {city}')
        df.to_json(file_save_loc)


    print("Cleaning and merging city data...")

    print("Cleaning LA:")
    ladf = pd.read_json('../data/crashes/unjoined/la_crashes_18_22.json')
    ladf = ladf.rename(columns={'dr_no':'ID', 'date_occ':'year'})\
        .drop('crm_cd_desc', axis=1)
    ladf['year'] = ladf.year.apply(lambda x: int(x[:4]))
    ladf['city'] = 'Los Angeles'
    ladf['ID'] = ladf.ID.apply(lambda x: f"LA{x}")
    ladf = ladf[DESIRED_COLUMNS]

    print('Cleaning Detroit:')
    dtdf = [pd.read_json(f'../data/crashes/unjoined/detroit_crashes_{i}.json') 
            for i in range(18,23)]
    dtdf = pd.concat(dtdf, ignore_index=True)
    dtdf['city'] = 'Detroit'
    dtdf['ID'] = dtdf.crash_id.apply(lambda x: f"DT{x}")
    dtdf = dtdf[DESIRED_COLUMNS]

    print('Cleaning Philadelphia:')
    phdf = pd.read_json('../data/crashes/unjoined/phili_crashes_18_22.json')
    phdf['ID'] = phdf.crn.apply(lambda x: f"PH{x}")
    phdf['city'] = 'Philadelphia'
    phdf['latitude'] = phdf.latitude.apply(ch.to_decimal)
    phdf['longitude'] = phdf.longitude.apply(ch.to_decimal).apply(lambda x: -x)
    phdf['year'] = phdf['crash_year']
    phdf = phdf[DESIRED_COLUMNS]
    phdf = phdf[phdf['longitude'] != 74] # catch a weird entry

    print("Cleaning Chicago:")
    cdf = pd.read_json('../data/crashes/unjoined/chicago_crashes_18_22.json')
    cdf['city'] = 'Chicago'
    cdf['year'] = cdf.crash_date.apply(lambda x: int(x[:4]))
    cdf['ID'] = cdf.crash_record_id.apply(lambda x: f"CH{x}")
    cdf = cdf[DESIRED_COLUMNS]

    print("Cleaning NYC:")
    nydf = pd.read_json('../data/crashes/unjoined/nyc_crashes_18_22.json')
    nydf['year'] = nydf.crash_date.apply(lambda x: str(x[:4]))
    nydf['ID'] = nydf.collision_id.apply(lambda x: f'NY{x}')
    nydf['city'] = 'NYC'
    nydf = nydf[DESIRED_COLUMNS]

    print("Collating..")

    df = pd.concat([nydf, cdf, ladf, dtdf, phdf], ignore_index=True)
    del nydf, cdf, ladf, dtdf, phdf

    # remove null island
    df = df[(df.latitude != 0) & (df.longitude != 0)]

    # gpd
    df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude),
                        crs='EPSG:4326')

    print("Saving raw crash counts...")
    df.to_file('../data/crashes/crashes_18_22.shp')

    print("Attaching to census data...")
