from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from psmpy import PsmPy
from psmpy.plotting import *
from psmpy.functions import cohenD

import pandas as pd
import geopandas as gpd

import matplotlib.transforms as transforms
import matplotlib.pyplot as plt
from warnings import warn

import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf

import osmnx as ox

# Columns NOT to match on
EXCLUDE_LIST = ['YEAR', 'STATE', 'STATEA', 
                'COUNTY', 'COUNTYA', 'TRACTA', 
                'geometry', 'AREANAM',
                'n_311s', 'n_crashes', 'whiteP', 
                'nonWhtP', 'blackP'] # let's try on num_crashes first

# Constant for propensity calculation
PROP_BALANCE = False

# Constants for KNN matching
CALIPER = 0.2
DROP_UNMATCHED = True
KNN_WITH_REPLACEMENT = False
PROP_MATCHER = 'propensity_logit'
N_FEATURES = 15

def feature_selection(df: pd.DataFrame, exclude_list: list[str]=EXCLUDE_LIST,
                      n_features: int=N_FEATURES, return_importances: bool=False,
                      outcome: str='treatment_') -> list[str]:
    '''
    Given a pandas DataFrame of pretreatment covariates, identifiers, and 
    HOLC grades, exclude the irrelevant columns and identify the 15 most
    relevant features.
    '''
    data = df.drop(columns = exclude_list)
    data = data.set_index('GISJOIN')
    old_size = data.shape[0]
    data.dropna(inplace=True)
    if data.shape[0] - old_size > 0: warn(f'Data had {data.shape[0] - old_size} unhandled NAs.')
    
    X, y = data.drop(columns=[outcome]), data[outcome]

    rf = DecisionTreeClassifier(random_state=42)
    rf.fit(X, y)
    
    print(f'Classifier scored a {rf.score(X, y)} on full dataset')
    importances = pd.DataFrame(
        {'importance': rf.feature_importances_,
        'name':rf.feature_names_in_}
    )
    
    if return_importances:
        return(importances.sort_values('importance', ascending=False)[:n_features])
    
    return importances.sort_values('importance', ascending=False)[:n_features].name.tolist()

def display_cor_plot(df: pd.DataFrame, covariates: list[str],
                     outcomes: list[str]=['n_311s', 'n_crashes', 'treatment']) -> None:
    '''
    Given a dataframe and list of columns in features, display the 
    correlation plot for this dataframe.
    '''
    relevant_features = covariates + outcomes
    cr = df[relevant_features].corr()
    
    heatmap_len = len(relevant_features)
    f = plt.figure(figsize=(19,15))
    plt.matshow(cr, fignum=f.number)
    plt.xticks(range(heatmap_len), relevant_features, fontsize=14, rotation=45)
    plt.yticks(range(heatmap_len), relevant_features, fontsize=14, rotation=45)

    cb = plt.colorbar()
    cb.ax.tick_params(labelsize=14)

    plt.title('Correlation Matrix', fontsize=16)
    plt.show()

def make_psmpy(data: pd.DataFrame, treatment: str, outcome: str, index='GISJOIN') -> PsmPy:
    '''
    Run propensity matching on the given dataset and return matched result.
    '''
    has_missing_covariates = data.drop(columns=[outcome]).isna().any().any()
    if has_missing_covariates:
        old_size = data.shape[0]
        mask = ~data.drop(columns=[outcome]).isna().any(axis=1)
        data = data[mask]
        new_size = data.shape[0]
        warn(f"Found NAs in covariates, dropping. Lost {old_size-new_size} observations out of {old_size}")

    has_missing_outcome_values = data[outcome].isna().any()
    if has_missing_outcome_values:
        warn("Found missing outcome values. Filling with 0")
        data[outcome] = data[outcome].fillna(0)

    return PsmPy(data, treatment, target=outcome, indx=index, exclude=[outcome], seed=42)

def retrieve_matches(psmpy: PsmPy, original_df: pd.DataFrame,
                     outcomes: list[str]=['n_crashes', 'n_311s'], treatment: str='treatment_') -> pd.DataFrame:
    '''
    Given a psmpy object which has already done matching and the original
    dataframe on which the matching was done, retrieve the relevant output
    dataframe.
    '''
    relevant_features = outcomes + [treatment]

    matches = psmpy.matched_ids
    matches = matches.GISJOIN.tolist() + matches.matched_ID.tolist()
    matches = original_df.loc[original_df.GISJOIN.isin(matches), relevant_features]

    return matches

def update_results(running_results: dict[list[str]], results_crash, results_311, city: str,
                   treatment: str='treatment_'):
    '''
    Update the running results dictionary with new values
    '''
    running_results['city'].append(city)
    running_results['311_coef'].append(results_311.params[treatment])
    running_results['311_se'].append(results_311.bse[treatment])
    running_results['311_p'].append(results_311.pvalues[treatment])
    running_results['crash_coef'].append(results_crash.params[treatment])
    running_results['crash_se'].append(results_crash.bse[treatment])
    running_results['crash_p'].append(results_crash.pvalues[treatment])

def run_models_update_results(matched_df: pd.DataFrame, all_results: dict[list[str]], city: str,
                              treatment: str="treatment_"):
    '''
    Run actual models + update the results dict
    '''

    mean_crashes = matched_df.n_crashes.mean()
    var_crashes = matched_df.n_crashes.var()
    alpha_crashes_est = (var_crashes - mean_crashes) / (mean_crashes ** 2)

    print(f"Estimating an alpha of {alpha_crashes_est} for crashes")
    crash_model = smf.glm(
        formula=f'n_crashes ~ {treatment}',
        data=matched_df,
        family=sm.families.NegativeBinomial(alpha=alpha_crashes_est),
        offset=matched_df['log_exposure']
    )

    print(f"""Checking Asumptions of Negative Binomial Model for 311s:
    \tMean 311s: {matched_df.n_311s.mean()}
    \tVariance of 311s: {matched_df.n_311s.var()}
    """)
    mean_311 = matched_df.n_311s.mean()
    var_311 = matched_df.n_311s.var()
    alpha_311s_est = (var_311 - mean_311) / (mean_311 ** 2)

    print(f"Estimating an alpha of {alpha_311s_est} for 311s")
    threeoneone_model = smf.glm(
        formula=f'n_311s ~ {treatment}',
        data=matched_df,
        family=sm.families.NegativeBinomial(alpha=alpha_311s_est),
        offset=matched_df['log_exposure']
    )

    results_crash = crash_model.fit()
    results_311 = threeoneone_model.fit()

    print(results_crash.summary())
    print(results_311.summary())

    update_results(all_results, results_crash, results_311, city)


def enforce_administrative_boundaries(city_data: gpd.geodataframe, city_name: str) -> gpd.GeoDataFrame:
    '''
    Given a geodataframe containing the data for a given city and the name of the city,
    ensure that we are only using tracts within the city's modern administrative bounds.
    '''
    city = ox.geocode_to_gdf(city_name)
    city = city.to_crs(city_data.crs)
    
    # GeoPandas intersection is a 1 to 1 operation with no brodcasting
    repeated_city = [city[['geometry']] for i in range(city_data.shape[0])]
    city = pd.concat(repeated_city, axis=0)

    # Mask and return
    mask = city_data.intersects(city, align=False)
    return city_data[mask]

def plot_estimates(results: pd.DataFrame, value_to_plot: str,
                   ylabel: str, title: str):
    # Figure out what to plot
    se = f"{value_to_plot}_se"
    coef = f"{value_to_plot}_coef"

    fig, ax = plt.subplots(figsize=(12, 7.5))

    # Get IRRs
    lower_bound, upper_bound = np.exp(results[coef] - 1.96 * results[se]), np.exp(results[coef] + 1.96 * results[se]) 
    irr = np.exp(results[coef])
    yerr = [irr - lower_bound, upper_bound - irr]
    
    # Debug and QoL print
    print(f"""
          IRR: ----
          \t{irr}
          Errors: ----
          \t{yerr}
          """)

    plt.errorbar(
        x=results["city"], y=irr, 
        yerr = [irr - lower_bound, upper_bound - irr],
        fmt='o', color='black', capsize=5, label="Estimated IRR"
    )
    
    # Needed for shifting things rightwards    
    x_shift = transforms.ScaledTranslation(5/72, 0, fig.dpi_scale_trans)
    text_color = 'black'
    fontsize = 12
    # Add confidence interval text labels at the ends of the error bars
    for x, y, low, high in zip(results["city"], irr, lower_bound, upper_bound):
        ax.text(x, low, f"{low:.2f}", ha='left', va='bottom', fontsize=fontsize, color=text_color,
                transform=ax.transData + x_shift)
        ax.text(x, y, f"{y:.2f}", ha='left', va='center', fontsize=fontsize, color=text_color,
                transform=ax.transData + x_shift)
        ax.text(x, high, f"{high:.2f}", ha='left', va='top', fontsize=fontsize, color=text_color,
                transform=ax.transData + x_shift)

    plt.axhline(y=1, color="red", linestyle="--", label="No Effect (IRR = 1)")  # Reference line

    # Labels and formatting
    plt.ylabel(ylabel)
    plt.xlabel("City")
    plt.title(title)
    plt.xticks(rotation=30)
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)

def plot_both_estimates(results: pd.DataFrame, value_to_plot_1: str, value_to_plot_2: str,
                   label_1: str, label_2: str, ylabel: str, title: str):
    fig, ax = plt.subplots(figsize=(12, 7.5))

    colors = ["green", "blue"]  # Different colors for each set
    markers = ["o", "s"]  # Different marker styles
    offset = 0.2  # Shift to avoid overlapping
    
    x = np.arange(len(results))  # Numeric x-axis positions for shifting

    for i, (value_to_plot, label, color, marker, shift) in enumerate(zip(
        [value_to_plot_1, value_to_plot_2], [label_1, label_2], colors, markers, [-offset, offset]
    )):
        se = f"{value_to_plot}_se"
        coef = f"{value_to_plot}_coef"

        # Calculate IRRs and confidence intervals
        lower_bound, upper_bound = np.exp(results[coef] - 1.96 * results[se]), np.exp(results[coef] + 1.96 * results[se])
        irr = np.exp(results[coef])

        # Plot error bars with shifted x positions
        plt.errorbar(
            x=x + shift, y=irr, 
            yerr=[irr - lower_bound, upper_bound - irr],
            fmt=marker, color=color, capsize=5, label=f"Estimated IRR - {label}"
        )

        # Text labels
        x_shift = transforms.ScaledTranslation((-1) ** (i+1) * 20/72, 0, fig.dpi_scale_trans)  # Keep text properly aligned
        fontsize = 12
        for x_pos, y, low, high in zip(x + shift, irr, lower_bound, upper_bound):
            ax.text(x_pos, low, f"{low:.2f}", ha='center', va='bottom', fontsize=fontsize, color=color,
                    transform=ax.transData + x_shift)
            ax.text(x_pos, y, f"{y:.2f}", ha='center', va='center', fontsize=fontsize, color=color,
                    transform=ax.transData + x_shift)
            ax.text(x_pos, high, f"{high:.2f}", ha='center', va='top', fontsize=fontsize, color=color,
                    transform=ax.transData + x_shift)

    # Reference line at IRR = 1
    plt.axhline(y=1, color="red", linestyle="--")  

    # Labels and formatting
    plt.ylabel(ylabel)
    plt.xlabel("City")
    plt.title(title)
    plt.xticks(ticks=np.arange(len(results)), labels=results["city"], rotation=30)
    plt.legend()

    plt.show()
