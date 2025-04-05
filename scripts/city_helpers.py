import pandas as pd
import requests 

def request_all_soda(url: str, default_params: dict, 
                            rel_columns: list, offset_param='$offset') -> pd.DataFrame:
    '''
    Request all results from a given SODA API url, until no new results
    can be found. 

    Inputs:
      url (str): the base url to make requests from. Should be the json format
      default_params (dict): dictionary of additional params for request.
      rel_columns (list): columns to keep.
    
    Returns: dataframe with features rel_columns containing all relevant results
      from the SODA API.
    '''

    df = None
    got_bigger, prior_size = True, 0
    while got_bigger:

        response = requests.get(url, params=default_params)
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            print(f"\tUrl: {response.url}")
        
        print('response received!')
        next_chunk = pd.DataFrame(response.json())
        
        if len(next_chunk) == 0:
            print('Oopsie, out of new entries!')
            return df

        if df is None:
            df = next_chunk.copy()
            print(df.columns)
            print(df)
            df = df[rel_columns]
            prior_size = df.shape[0]
            default_params[offset_param] = f'{prior_size}'
            continue

        df = pd.concat([df, next_chunk[rel_columns]], ignore_index=True)\
            .drop_duplicates()
        
        if prior_size == len(df):
            prior_size = False

        prior_size=df.shape[0]
        default_params[offset_param] = f'{prior_size}'

        print(f"DataFrame is now at {prior_size} rows.")

    return df
        
def request_all_soda_la(url: str, default_params: dict, 
                            rel_columns: list, offset_param='$offset') -> pd.DataFrame:
    '''
    Request all traffic crash results from Los Angeles, a city where the output format
    is just incredibly cursed.

    Inputs:
      url (str): the base url to make requests from. Should be the json format
      default_params (dict): dictionary of additional params for request.
      rel_columns (list): columns to keep.
    
    Returns: dataframe with features rel_columns containing all relevant results
      from the SODA API.
    '''

    df = None
    got_bigger, prior_size = True, 0

    while got_bigger:

        response = requests.get(url, params=default_params)
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
        
        print('response received!')
        # so this will throw an error atm
        next_chunk = response.json() # must clean before reading to dataframe
        
        lats, longs = [], []
        for i, obs in enumerate(next_chunk):
            lats.append(obs['location_1']['latitude'])
            longs.append(obs['location_1']['longitude'])
            
            del next_chunk[i]['location_1']        

        next_chunk = pd.DataFrame(next_chunk)
        next_chunk['latitude'] = lats
        next_chunk['longitude'] = longs

        if len(next_chunk) == 0:
            print('Oopsie, out of new entries!')
            return df

        if df is None:
            df = next_chunk.copy()
            print(df.columns)
            print(df)
            df = df[rel_columns]
            prior_size = df.shape[0]
            default_params[offset_param] = f'{prior_size}'
            continue

        df = pd.concat([df, next_chunk[rel_columns]], ignore_index=True)\
            .drop_duplicates()
        
        if prior_size == len(df):
            got_bigger = False

        prior_size=df.shape[0]
        default_params[offset_param] = f'{prior_size}'

        print(f"DataFrame is now at {prior_size} rows.")

    return df
        


def request_all_arcgis(url: str, default_params: dict, 
                            rel_columns: list, offset_param='resultOffset',
                            paranoid: bool = False) -> pd.DataFrame:
    '''
    Request all results from a given SODA API url, until no new results
    can be found. 

    Inputs:
      url (str): the base url to make requests from. Should be the json format
      default_params (dict): dictionary of additional params for request.
      rel_columns (list): columns to keep.
      paranoid (bool): Set to True if you're paranoid that you aren't getting good data.
    
    Returns: dataframe with features rel_columns containing all relevant results
      from the SODA API.
    '''

    df = None
    got_bigger, prior_size = True, 0

    while got_bigger:

        response = requests.get(url, params=default_params)
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")

        # we're done and things were Oddly Even
        if len(list(response.json()['features'])) == 0:
            return df 
        
        next_chunk = [pd.DataFrame(response.json()['features'][i]['attributes'], index=[i]) 
                      for i in range(len(list(response.json()['features'])))]
        next_chunk = pd.concat(next_chunk, ignore_index=True)

        if len(next_chunk) == 0:
            print('Oopsie, out of new entries!')
            return df

        if df is None:
            df = next_chunk.copy()
            print(df.columns)
            print(df)
            df = df[rel_columns]
            prior_size = df.shape[0]
            default_params[offset_param] = f'{prior_size}'
            continue

        df = pd.concat([df, next_chunk[rel_columns]], ignore_index=True)\
            .drop_duplicates()
        
        if prior_size == len(df):
            prior_size = False

        prior_size=df.shape[0]
        default_params[offset_param] = f'{prior_size}'

        print(f"DataFrame is now at {prior_size} rows.")
        if paranoid:
            print('Hey paranoid, here\'s the head:')
            print(next_chunk.head())

    return df

def to_decimal(dms: str) -> float:
    '''
    Given a coordinate in degree minute:seconds format, convert
    the coordinates into a decimal.
    '''
    deg, ms = dms.split(' ')
    minutes, seconds = ms.split(':')
    
    return round((float(deg) + float(minutes) / 60 + float(seconds) / 3600), 5)

def filter_text_col(df: pd.DataFrame, col: str, relevant_terms: list[str]) -> pd.DataFrame:
    '''
    Given a dataframe df, a column name col, and a list of relevant terms to appear
    in that column, subsets the dataframe to only contain rows with the relevant
    terms in the col column.
    '''

    def is_relevant(s):
        '''
        Return whether s is relevant or irrelevant
        '''
        for val in relevant_terms:
            if val in str(s).lower():
                return True
        return False

    mask = df[col].apply(lambda x: is_relevant(x))

    return df[mask]
        
