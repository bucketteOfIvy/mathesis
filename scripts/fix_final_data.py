### Author: Ashlynn Wimer
### Date: 3/21/2025
### About: We have a broken treatment indicator! Let's fix it.

import pandas as pd
import geopandas as gpd

YES_LST = ['one', 'nominal', 'threat', 'three', 'slight', 'two', '6', 'scattered', 'many', 'yes', 'few', 'east', 'south', 'west', 'nom.', 'negro', '37', '2']
# % is only really safe to use on this sample, as we hand verified it
NO_LST = ['no', '0', 'none', '%']

def valid_response(nyn):
    for yes in YES_LST:
        if yes in (str(nyn)).lower():
            return True
        
    for no in NO_LST:
        if no in (str(nyn)).lower():
            return False
    
    return False

def patchwork_fixes(all_field):
    '''
    Rough and tumble fixing method.
    '''
    if 'no negro' in all_field.lower():
        return False
    
    if 'no infiltration' in all_field.lower():
        return False
    
    if 'negro' in all_field.lower():
        return True

    return False

# We *want* a treatment indicator of whether a tract has black families
# or not [in the eyes of the HOLC]. But not all of the "negro_yes_or_no" 
# rows have an entry
# So we fix that
ad_data = pd.read_json('../data/shapes/ad_data.json')
redlining = gpd.read_file('../data/shapes/mappinginequality.gpkg')
census_final = gpd.read_file('../data/shapes/census_final.shp')

# Get our additional data
ad_data = ad_data.drop_duplicates('area_id').drop(columns='grade')
ad_data = ad_data.merge(redlining, on='area_id', how='left')
ad_data = gpd.GeoDataFrame(ad_data, geometry=ad_data.geometry, crs=redlining.crs).to_crs(census_final.crs)
ad_data = ad_data[['area_id', 'grade', 'negro_yes_or_no', 'all_fields', 'geometry']]
ad_data = ad_data.rename(columns={'grade':'GRADE_HOLC'}) 

# Make overlay
overlay = census_final.overlay(ad_data)
with_matching_grades = overlay.grade == overlay.GRADE_HOLC
overlay = overlay[with_matching_grades].reset_index(drop=True)

overlay['nyn'] = overlay.negro_yes_or_no.apply(valid_response)
overlay['nyn'] = overlay.nyn | overlay.all_fields.apply(patchwork_fixes)

overlay['nyn'] = overlay.groupby('GISJOIN')['nyn'].transform(lambda x: any(x))
overlay = overlay[['GISJOIN', 'nyn']].drop_duplicates()

census_final = census_final.merge(overlay, how='inner', on='GISJOIN')

treatment_labels = []

for grade, nyn in zip(census_final.grade, census_final.nyn):
    lbl = '_black' if nyn else ''
    treatment_labels.append(f"{grade}{lbl}")

census_final['treatment_labels'] = treatment_labels

census_final.to_file('../data/shapes/census_final_fixed.shp')
