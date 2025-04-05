### Author: Ashlynn Wimer
### Date 3/7/2025 
### About: Attaches crashes and 311s to 1940s census tracts

import pandas as pd
import geopandas as gpd

DESIRED_COLUMNS = ['ID', 'city', 'year', 'latitude', 'longitude']

def relevant_tract(area_name: str) -> bool:
    '''
    Takes a given census tract area name and returns whether it is relevant.
    '''
    relevant = ['CHICAGO', 'NEW YORK NY', 'DETROIT', 'LOS ANGELES', 'PHILADELPHIA']
    
    for name in relevant:
        if name in area_name:
            return True
        
    return False

def to_decimal(dms: str) -> float:
    '''
    Given a coordinate in degree minute:seconds format, convert
    the coordinates into a decimal.
    '''
    deg, ms = dms.split(' ')
    minutes, seconds = ms.split(':')
    
    return round((float(deg) + float(minutes) / 60 + float(seconds) / 3600), 5)

if __name__ == "__main__":

    print('Starting with crashes.')
    
    # Read in census a bit preliminarily
    cens = gpd.read_file('../data/shapes/cens1940shapes.shp')
    
    # Handle LA
    ladf = pd.read_json('../data/crashes/unjoined/la_crashes_18_22.json')
    ladf = ladf.rename(columns={'dr_no':'ID', 'date_occ':'year'})\
        .drop('crm_cd_desc', axis=1)
    ladf['year'] = ladf.year.apply(lambda x: int(x[:4]))
    ladf['city'] = 'Los Angeles'
    ladf = ladf[(ladf['latitude'] != 0) & (ladf['longitude'] != 0)]
    ladf['ID'] = ladf.ID.apply(lambda x: f"LA{x}")
    ladf = ladf[DESIRED_COLUMNS]

    # Handle Detroit
    dtdf = [pd.read_json(f'../data/crashes/unjoined/detroit_crashes_{i}.json') 
            for i in range(18,23)]
    dtdf = pd.concat(dtdf, ignore_index=True)
    dtdf['city'] = 'Detroit'
    dtdf['ID'] = dtdf.crash_id.apply(lambda x: f"DT{x}")
    dtdf = dtdf[DESIRED_COLUMNS]
    dtdf = dtdf[(dtdf['latitude'] != 0) & (dtdf['longitude'] != 0)]


    # Handle Phili
    phdf = pd.read_json('../data/crashes/unjoined/phili_crashes_18_22.json')
    phdf['ID'] = phdf.crn.apply(lambda x: f"PH{x}")
    phdf['city'] = 'Philadelphia'
    phdf['latitude'] = phdf.latitude.apply(to_decimal)
    phdf['longitude'] = phdf.longitude.apply(to_decimal).apply(lambda x: -x)
    phdf['year'] = phdf['crash_year']
    phdf = phdf[(phdf['latitude'] != 0) & (phdf['longitude'] != -74)]
    phdf=phdf[DESIRED_COLUMNS]
    
    # Handle Chicago
    cdf = pd.read_json('../data/crashes/unjoined/chicago_crashes_18_22.json')
    cdf['city'] = 'Chicago'
    cdf['year'] = cdf.crash_date.apply(lambda x: int(x[:4]))
    cdf['ID'] = cdf.crash_record_id.apply(lambda x: f"CH{x}")
    cdf = cdf[DESIRED_COLUMNS]
    cdf = cdf[(cdf['latitude'] != 0) & (cdf['longitude'] != -74)]

    cdf.head(), cdf.describe()

    # Handle New York City
    nydf = pd.read_json('../data/crashes/unjoined/nyc_crashes_18_22.json')
    nydf['year'] = nydf.crash_date.apply(lambda x: str(x[:4]))
    nydf['ID'] = nydf.collision_id.apply(lambda x: f'NY{x}')
    nydf['city'] = 'NYC'
    nydf = nydf[(nydf['latitude'] != 0) & (nydf['longitude'] != 0)]
    nydf = nydf[(nydf.longitude > -75) & (nydf.longitude < -72)]
    nydf = nydf[DESIRED_COLUMNS]

    # Merge
    crashes = pd.concat([nydf, phdf, cdf, dtdf, ladf], axis=0, ignore_index=True)
    crashes = gpd.GeoDataFrame(crashes, 
        geometry=gpd.points_from_xy(crashes.longitude, 
                                        crashes.latitude, crs='EPSG:4326'))

    del nydf
    del phdf
    del cdf
    del dtdf
    del ladf

    print(f'Concatenated! Result:')
    print(crashes.head())

    crashes['year'] = crashes.year.astype(int)
    crashes = crashes.to_crs("ESRI:102003")

    # Reread in case we broke something
    cens = gpd.read_file('../data/shapes/cens1940shapes.shp')
    cens = cens[cens.AREANAM.apply(relevant_tract)]

    # Spatial join crashes
    cens_crashes = cens.join(
        gpd.sjoin(crashes, cens).groupby("index_right").size().rename("n_crashes"),
        how="left",
    )

    del crashes
    del cens

    cens_crashes.drop(['PRETRAC', 'POSTTRC'], axis=1, inplace=True)
    
    print('Saving progress...')
    cens_crashes.to_file('../data/shapes/censCrashes.shp')

    df311 = pd.read_csv('../data/311/311_requests_18_22.csv')
    gdf311 = gpd.GeoDataFrame(df311, 
                              geometry=gpd.points_from_xy(df311.longitude, df311.latitude),
                              crs='EPSG:4326')

    del df311

    gdf311 = gdf311.to_crs('ESRI:102003')

    cens_crashes = cens_crashes.join(
        gpd.sjoin(gdf311, cens_crashes).groupby('index_right').size().rename('n_311s'),
        how='left'
    )

    del gdf311

    # Save -- maybe final dataset?
    cens_crashes.to_file('../data/shapes/census_final.shp')

