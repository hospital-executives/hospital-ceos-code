import pandas as pd
import numpy as np
from itertools import combinations

cleaned_himss_path = '/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/final_himss.feather'
cleaned_himss = pd.read_feather(cleaned_himss_path)

## sullivan case study
# contact_list = [1355983, 113559, 665251,138384,126418,797336]  

# cathy/katherine/keith case study
contact_list = [139855,539657,676573,1352457,1345131,133081,2341284]
new_himss_filtered = new_himss[new_himss['contact_uniqueid'].isin(contact_list)]

def generate_pairwise_combinations(df):
    pairs = []
    confirmed_list = []
    match_list = []

    for _, group in df.groupby('final_block'):
        if len(group) > 1:  # Only generate combinations if there are at least 2 elements
            for id1, id2 in combinations(group['id'], 2):
                # Get confirmed statuses
                confirmed1 = group.loc[group['id'] == id1, 'confirmed'].values[0]
                confirmed2 = group.loc[group['id'] == id2, 'confirmed'].values[0]
                
                # Get new_contact_uniqueid values
                new_contact_uniqueid1 = group.loc[group['id'] == id1, 'new_contact_uniqueid'].values[0]
                new_contact_uniqueid2 = group.loc[group['id'] == id2, 'new_contact_uniqueid'].values[0]
                
                # Determine if both are confirmed
                confirmed = confirmed1 and confirmed2
                
                # Set match value based on new_contact_uniqueid1 and new_contact_uniqueid2
                if confirmed:
                    match = 1 if new_contact_uniqueid1 == new_contact_uniqueid2 else 0
                else:
                    match = np.nan  # Set match to NaN if not confirmed
                
                pairs.append((id1, id2))
                confirmed_list.append(confirmed)
                match_list.append(match)

    # Convert pairs, confirmed, and match to a DataFrame
    pairs_array = np.array(pairs)
    df_result = pd.DataFrame(pairs_array, columns=['id1', 'id2'])
    
    # Add the 'confirmed' and 'match' columns
    df_result['confirmed'] = confirmed_list
    df_result['match'] = match_list

    return df_result
    pairs = []
    confirmed_list = []
    match_list = []

    for _, group in df.groupby('final_block'):
        if len(group) > 1:  # Only generate combinations if there are at least 2 elements
            for id1, id2 in combinations(group['id'], 2):
                # Check if both are confirmed
                confirmed1 = group.loc[group['id'] == id1, 'confirmed'].values[0]
                confirmed2 = group.loc[group['id'] == id2, 'confirmed'].values[0]
                
                confirmed = confirmed1 and confirmed2
                
                # Check if the ids match if confirmed is True
                if confirmed:
                    match = 1 if id1 == id2 else 0
                else:
                    match = np.nan
                
                pairs.append((id1, id2))
                confirmed_list.append(confirmed)
                match_list.append(match)

    # Convert pairs, confirmed, and match to a DataFrame
    pairs_array = np.array(pairs)
    df_result = pd.DataFrame(pairs_array, columns=['id1', 'id2'])
    
    # Add the 'confirmed' and 'match' columns
    df_result['confirmed'] = confirmed_list
    df_result['match'] = match_list

    return df_result

pairwise_df = generate_pairwise_combinations(new_himss_filtered)

def merge_variables_dict_optimized(df, B, columns):
    """
    Efficiently merge specified columns from DataFrame B into DataFrame df based on id1 and id2
    using dictionaries for fast lookups, optimized for large datasets.
    
    Parameters:
    df (DataFrame): The first DataFrame containing 'id1' and 'id2'.
    B (DataFrame): The second DataFrame containing 'id' and the columns to merge.
    columns (list): List of column names from B to merge into df.
    
    Returns:
    DataFrame: Updated DataFrame with merged columns like 'firstname1', 'firstname2', etc.
    """
    # Create dictionaries for fast lookups
    dicts = {col: B.set_index('id')[col].to_dict() for col in columns}
    
    # Pre-allocate memory by adding all new columns at once
    for col in columns:
        df[f'{col}1'] = df['id1'].map(dicts[col])
        df[f'{col}2'] = df['id2'].map(dicts[col])
    
    return df

result = merge_variables_dict_optimized(pairwise_df, cleaned_himss, ['firstname', 'lastname', 'entity_name'])
