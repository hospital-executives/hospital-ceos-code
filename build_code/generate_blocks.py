import os 
import sys
import subprocess

# Automate package installation
def install(package):
    try:
        # Try installing using pip
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    except subprocess.CalledProcessError as e:
        print(f"Failed to install {package} using pip. Trying with pip3...")
        try:
            # If pip is not found, try with pip3
            subprocess.check_call([sys.executable, "-m", "pip3", "install", package])
        except subprocess.CalledProcessError as e:
            print(f"Failed to install {package} using both pip and pip3. Please check your Python and pip installation.")

# list packages
required_packages = ['pandas', 'networkx']  

# import packages
for package in required_packages:
    try:
        __import__(package)  # Try to import the package
    except ImportError:
        install(package)

data_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data"
data_dir = data_path
# Get command-line arguments
if len(sys.argv) == 9:
    himss_path = sys.argv[1]
    confirmed_1_path = sys.argv[2]
    remaining_1_path = sys.argv[3]
    updated_gender_path = sys.argv[4]
    himss_1_path = sys.argv[5] 
    confirmed_2_path = sys.argv[6]
    remaining_2_path = sys.argv[7]
    data_dir = sys.argv[8]
else: 
    himss_path = os.path.join(data_dir, 
    "derived/himss_entities_contacts_0517_v1.feather")
    confirmed_1_path = os.path.join(data_path, "derived/auxiliary/confirmed_1.csv")
    remaining_1_path = os.path.join(data_path, "derived/auxiliary/remaining_1.csv")
    updated_gender_path = os.path.join(data_dir, 
    "derived/auxiliary/updated_gender.csv")
    himss_1_path = os.path.join(data_dir, "derived/auxiliary/himss_1.csv")
    confirmed_2_path = os.path.join(data_path, "derived/auxiliary/confirmed_2.csv")
    remaining_2_path = os.path.join(data_path, "derived/auxiliary/remaining_2.csv")


import pandas as pd
import networkx as nx
sys.path.append(os.path.join(os.path.dirname(__file__), 'helper_scripts'))
import blocking_helper
import cleaned_confirmed_helper as cc

# LOAD DFS
#print(himss_path)
#print(updated_gender_path)
#print(himss_1_path)
#print(confirmed_1_path)
#print(remaining_1_path)
#print(data_dir)
 
gender_df = pd.read_csv(updated_gender_path)
gender_df_unique = gender_df.drop_duplicates(subset='firstname')

cleaned_himss = pd.read_csv(himss_1_path)
cleaned_confirmed = pd.read_csv(confirmed_1_path)
cleaned_remainder = pd.read_csv(remaining_1_path)

## DEBUG FROM HERE - ALSO DELETE WHEN RUNNING FROM MAKEFILE
#gender_df = pd.read_csv('/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/auxiliary/updated_gender.csv')
#gender_df_unique = gender_df.drop_duplicates(subset='firstname')

##cleaned_himss = pd.read_csv('/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/auxiliary/himss_1.csv')
#cleaned_confirmed = pd.read_csv('/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/auxiliary/confirmed_1.csv')
#cleaned_remainder = pd.read_csv('/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/auxiliary/remaining_1.csv')

# backfill the John Null case which accidentally got dropped
list_of_dfs = [cleaned_confirmed, cleaned_remainder, cleaned_himss]  # Replace with your actual list of DataFrames
for df in list_of_dfs:
    # Replace NaN values in the 'lastname' column with "null"
    df['lastname'] = df['lastname'].fillna("null")
    
# ADD GENDER
#himss_path = '/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/himss_entities_contacts_0517.feather'
himss = blocking_helper.load_himss(himss_path)
himss_by_nickname = blocking_helper.clean_for_metaphone(himss[['firstname']])
himss_nicknames = himss_by_nickname[himss_by_nickname['Inside'].notna()]
himss_nicknames.loc[:, 'Before'] = himss_nicknames['Before'].str.lower()
himss_nicknames.loc[:, 'Inside'] = himss_nicknames['Inside'].str.lower()

name_gender_map = pd.Series(gender_df_unique.gender.values, 
index=gender_df_unique.firstname).to_dict()

dataframes = [cleaned_himss, cleaned_confirmed, cleaned_remainder]

# Update each DataFrame
for i, df in enumerate(dataframes):
    dataframes[i] = cc.update_gender(df, name_gender_map)

cleaned_remained = cleaned_remainder.copy()

# add metaphone codes
cleaned_confirmed['first_meta'] = cleaned_confirmed['firstname'].apply(blocking_helper.get_metaphone)
cleaned_confirmed['old_first_meta'] = cleaned_confirmed['old_firstname'].apply(blocking_helper.get_metaphone)
cleaned_confirmed['last_meta'] = cleaned_confirmed['lastname'].apply(blocking_helper.get_metaphone)
cleaned_confirmed['old_last_meta'] = cleaned_confirmed['old_lastname'].apply(blocking_helper.get_metaphone)

cleaned_remained['first_meta'] = cleaned_remained['firstname'].apply(blocking_helper.get_metaphone)
cleaned_remained['old_first_meta'] = cleaned_remained['old_firstname'].apply(blocking_helper.get_metaphone)
cleaned_remained['last_meta'] = cleaned_remained['lastname'].apply(blocking_helper.get_metaphone)
cleaned_remained['old_last_meta'] = cleaned_remained['old_lastname'].apply(blocking_helper.get_metaphone)

# generate blocks
#data_dir = '/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data'
f_graph, f_df = blocking_helper.generate_female_blocks(cleaned_himss, 
                                                            himss_nicknames,
                                                            data_dir) #3760 comps
last_graph, last_df = blocking_helper.generate_last_blocks(cleaned_himss, himss, 
                                                           data_dir, 0.9, 
                                                           gender = "all") # 2961 comps


m_graph, m_df = blocking_helper.generate_male_blocks(cleaned_himss, 
                                                        cleaned_himss, himss_nicknames, data_dir) # 3125 comps


female_components = list(nx.connected_components(f_graph))
female_metaphone_to_component = {}
for component_id, component in enumerate(female_components):
    for metaphone_code in component:
        female_metaphone_to_component[metaphone_code] = component_id

male_components = list(nx.connected_components(m_graph))
male_metaphone_to_component = {}
for component_id, component in enumerate(male_components):
    for metaphone_code in component:
        male_metaphone_to_component[metaphone_code] = component_id

last_components = list(nx.connected_components(last_graph))
last_metaphone_to_component = {}
for component_id, component in enumerate(last_components):
    for metaphone_code in component:
        last_metaphone_to_component[metaphone_code] = component_id

# add blocks
def remove_last_s(meta):
    if meta and meta != "S" and meta[-1] == 'S':
        return meta[:-1]
    return meta
    
cleaned_confirmed['last_meta'] = cleaned_confirmed['last_meta'].apply(remove_last_s)
cleaned_confirmed['last_component'] = cleaned_confirmed['last_meta'].map(last_metaphone_to_component)

cleaned_remained['last_meta'] = cleaned_remained['last_meta'].apply(remove_last_s)
cleaned_remained['last_component'] = cleaned_remained['last_meta'].map(last_metaphone_to_component)
    

max_female_group= max(female_metaphone_to_component.values())
def map_component(row, metaphone_code):
    if row['gender'] == 'F':
        return female_metaphone_to_component.get(row[metaphone_code], 'Unknown')
    else:
        temp_component = male_metaphone_to_component.get(row[metaphone_code], 'Unknown')
        if isinstance(temp_component, int|float):
            return temp_component +  max_female_group
        return male_metaphone_to_component.get(row[metaphone_code], 'Unknown') 
cleaned_confirmed['first_component'] = cleaned_confirmed.apply(lambda row: map_component(row, 'first_meta'), axis=1)
cleaned_confirmed['old_first_component'] = cleaned_confirmed.apply(lambda row: map_component(row, 'old_first_meta'), axis=1)

cleaned_remained['first_component'] = cleaned_remained.apply(lambda row: map_component(row, 'first_meta'), axis=1)
cleaned_remained['old_first_component'] = cleaned_remained.apply(lambda row: map_component(row, 'old_first_meta'), axis=1)
cleaned_remaining = cleaned_remained

## add last name frequencies
lastname_contact_df = cleaned_confirmed[['lastname', 
                                             'contact_uniqueid']].drop_duplicates()
lastname_counts = lastname_contact_df['lastname'].value_counts().to_dict()
# 95th percentile is 8
cleaned_confirmed['lastname_count'] = cleaned_confirmed['lastname'].map(lastname_counts).fillna(0).astype(int)
cleaned_remaining['lastname_count'] = cleaned_remaining['lastname'].map(lastname_counts).fillna(0).astype(int)

cleaned_confirmed.to_csv(confirmed_2_path)
cleaned_remaining.to_csv(remaining_2_path)