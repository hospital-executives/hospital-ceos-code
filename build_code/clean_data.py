
import pandas as pd
import os
import sys

# specify data path 
data_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data"
# TAKE ARGS FROM MAKEFILE
# Get arguments from the command line
if len(sys.argv) == 8:
    # If arguments are provided, get them from the command line
    confirmed_r = sys.argv[1]
    remaining_r = sys.argv[2]
    himss_entities_contacts = sys.argv[3]
    confirmed_1 = sys.argv[4]
    remaining_1 = sys.argv[5]
    himss_1 = sys.argv[6]
    nicknames_path = sys.argv[7]
else:
    print('WARNING: Not using Makefile Input')
    # Otherwise, use default file paths
    confirmed_r = os.path.join(data_path, "derived/r_confirmed.feather")
    remaining_r =  os.path.join(data_path, "derived/r_remaining.feather")
    himss_entities_contacts = os.path.join(data_path, 
    "derived/himss_entities_contacts_0517_v1.feather")
    confirmed_1 = os.path.join(data_path, "derived/auxiliary/confirmed_1.csv")
    remaining_1 = os.path.join(data_path, "derived/auxiliary/remaining_1.csv")
    himss_1 = os.path.join(data_path, "derived/auxiliary/himss_1.csv")
    nicknames_path = os.path.join(data_path, "derived/auxiliary/himss_nicknames.csv")

#code_path = sys.argv[1]
#data_path = sys.argv[2]
#code_path = '/Users/loaner/hospital-ceos-code/'
#user_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/"

cleaned_r_path = str(confirmed_r)
remaining_r_path = str(remaining_r)
himss_path = str(himss_entities_contacts)

sys.path.append(os.path.join(os.path.dirname(__file__), 'helper_scripts'))

# LOAD HELPER FILES
import blocking_helper

confirmed_df = pd.read_feather(cleaned_r_path)
confirmed_df = confirmed_df.dropna(subset = ['entity_zip', 'entity_state', 
                                                'contact_uniqueid'])

remainder_df = pd.read_feather(remaining_r_path)
remainder_df = remainder_df.dropna(subset=['entity_zip', 'entity_state',
                                            'contact_uniqueid'])

himss = blocking_helper.load_himss(himss_path)

# clean dfs - drop nicknames and suffixes
dataframes = [himss, confirmed_df, remainder_df]
cleaned_dataframes = []
for df in dataframes:
    df_copy = df.copy()

    # Drop nicknames
    df_copy['firstname'] = df_copy['firstname'].apply(blocking_helper.drop_nickname)

    # Drop suffixes from last names
    df_copy = blocking_helper.clean_lastnames(df_copy, 'lastname')

    # Append cleaned DataFrame to list
    cleaned_dataframes.append(df_copy)

cleaned_himss, cleaned_confirmed, cleaned_remainder = cleaned_dataframes

# create nickname pairings from HIMSS
himss_by_nickname = blocking_helper.clean_for_metaphone(himss[['firstname']])
himss_nicknames = himss_by_nickname[himss_by_nickname['Inside'].notna()]
himss_nicknames.loc[:, 'Before'] = himss_nicknames['Before'].str.lower()
himss_nicknames.loc[:, 'Inside'] = himss_nicknames['Inside'].str.lower()

# clean typos 
firstname_to_lastnames, lastname_to_firstnames = blocking_helper.generate_name_mappings(cleaned_himss)
firstname_to_contact_count = cleaned_confirmed.groupby('firstname')['contact_uniqueid'].nunique().to_dict()
lastname_to_contact_count = cleaned_confirmed.groupby('lastname')['contact_uniqueid'].nunique().to_dict()

firstname_replacements = blocking_helper.generate_firstname_mapping(
        cleaned_confirmed, firstname_to_contact_count,firstname_to_lastnames, 
        lastname_to_firstnames)
def replace_firstname(row):
    return firstname_replacements.get((row['firstname'], row['lastname']), row['firstname'])

cleaned_himss['old_firstname'] = cleaned_himss['firstname']
cleaned_confirmed['old_firstname'] = cleaned_confirmed['firstname']
cleaned_remainder['old_firstname'] = cleaned_remainder['firstname']
cleaned_remainder['firstname'] = cleaned_remainder.apply(replace_firstname, axis=1)
cleaned_himss['firstname'] = cleaned_himss.apply(replace_firstname, axis=1)
cleaned_confirmed['firstname'] = cleaned_confirmed.apply(replace_firstname, axis=1)

lastname_replacements = blocking_helper.generate_lastname_mapping(
        cleaned_confirmed, lastname_to_contact_count, firstname_to_lastnames, 
        lastname_to_firstnames)
def replace_lastname(row):
    return lastname_replacements.get((row['lastname'], row['firstname']), row['lastname'])

cleaned_himss['old_lastname'] = cleaned_himss['lastname']
cleaned_confirmed['old_lastname'] = cleaned_confirmed['lastname']
cleaned_remainder['old_lastname'] = cleaned_remainder['lastname']
cleaned_remainder['lastname'] = cleaned_remainder.apply(replace_lastname, axis=1)
cleaned_himss['lastname'] = cleaned_himss.apply(replace_lastname, axis=1)
cleaned_confirmed['lastname'] = cleaned_confirmed.apply(replace_lastname, axis=1)

cleaned_confirmed.to_csv(str(confirmed_1))
cleaned_remainder.to_csv(str(remaining_1))
cleaned_himss.to_csv(str(himss_1))

# also clean nicknames
himss_by_nickname = blocking_helper.clean_for_metaphone(himss[['firstname']])
himss_nicknames = himss_by_nickname[himss_by_nickname['Inside'].notna()]
himss_nicknames['Before'] = himss_nicknames['Before'].str.lower()
himss_nicknames['Inside'] = himss_nicknames['Inside'].str.lower()

himss_nicknames.to_csv(str(nicknames_path))
