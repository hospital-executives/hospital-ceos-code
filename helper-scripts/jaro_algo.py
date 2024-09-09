import pandas as pd
import json
import jellyfish
import time
import os
import sys
from metaphone import doublemetaphone

module_path = os.path.abspath('/Users/loaner/BFI Dropbox/Katherine Papen/' \
                              'hospital_ceos/_code/')
sys.path.append(module_path)
import blocking_helper 


## IMPORT AND CLEAN FILES
user_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/"

# load gender df
gender_path = os.path.join(user_path, "nicknames dictionaries/updated_gender_by_metaphone.csv")
gender_df = pd.read_csv(gender_path)
gender_df.rename(columns={"Name": "firstname"}, inplace=True)
gender_df['firstname'] = gender_df['firstname'].str.lower()

# load himss df
himss_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/"\
    "_data/derived/himss_entities_contacts_0517.feather"
himss = blocking_helper.load_himss(himss_path)
himss_names = himss[['firstname', 'lastname']]

# load confirmed df
confirmed_path= os.path.join(user_path, "derived/himss_entity_contacts_0517_confirmed.feather")
confirmed = pd.read_feather(confirmed_path)
test_data = confirmed[['firstname', 'lastname', 'contact_uniqueid']]
test_data['firstname'] = test_data['firstname'].str.lower()
test_no_na = test_data.dropna(subset=['contact_uniqueid'])

# get male cases only 
merged_df = pd.merge(test_no_na, gender_df, on="firstname", how="left")
test_df = merged_df[(merged_df['Gender'] == "M") | (pd.isna(merged_df['Gender']))]

test_cleaned = test_df.dropna(subset=['contact_uniqueid', 'lastname'])
suffixes = r'\b(Jr|II|III|Sr)\b'
test_cleaned['lastname'] = test_cleaned['lastname'].str.replace(suffixes, '', regex=True) \
                              .str.replace(r"[,\.']", '', regex=True) \
                              .str.strip()
test_cleaned['last_metaphone'] = test_cleaned['lastname'].apply(lambda name: doublemetaphone(name)[0])

test_cleaned['base_code'] = test_cleaned['last_metaphone'].apply(lambda x: x[:-1] if x.endswith('S') else x)

## get frequencies
frequency_df = confirmed.groupby('lastname')['contact_uniqueid'].nunique().reset_index()
frequency_df['lastname'] = frequency_df['lastname'].str.lower()

# Assume blocking_helper.generate_metaphone generates a dictionary and a map for metaphones
# Here we create a simple placeholder for demonstration purposes
def generate_metaphone(names_df):
    from metaphone import doublemetaphone
    metaphone_dict = {}
    name_to_metaphone = {}
    for name in names_df['lastname']:
        metaphone_code = doublemetaphone(name)[0]
        metaphone_dict[name] = metaphone_code
        name_to_metaphone[name] = metaphone_code
    return metaphone_dict, name_to_metaphone

metaphone_dict, name_to_metaphone = generate_metaphone(frequency_df[['lastname']])
infrequent_names = frequency_df[frequency_df['contact_uniqueid'] <= 10 ]['lastname']
unique_lastnames = frequency_df[frequency_df['contact_uniqueid'] == 1 ]['lastname']

# Optimize similar name finding function
def find_similar_names(name, names_list, name_to_metaphone):
    return [
        (n, jellyfish.jaro_winkler_similarity(name, n))
        for n in names_list
        if name != n and
           (name[0] == n[0] or name[-1] == n[-1]) and
           name_to_metaphone.get(name) != name_to_metaphone.get(n) and
           jellyfish.jaro_winkler_similarity(name, n) > 0.85
    ]

# Measure execution time
start_time = time.time()

# Create a list of unique lastnames for faster access
unique_lastnames_list = unique_lastnames['lastname'].unique()

# Find all names that are within a Jaro-Winkler similarity threshold from the infrequent names
similar_names_dict = {
    name: find_similar_names(name, unique_lastnames_list, name_to_metaphone)
    for name in infrequent_names['lastname']
}

# Convert the dictionary to a DataFrame for better visualization
similar_names_records = [
    (k, v[0], v[1]) for k, vals in similar_names_dict.items() for v in vals
]
similar_names_df = pd.DataFrame(similar_names_records, columns=['name', 'similar_name', 'distance'])

end_time = time.time()
print(f"Execution time: {end_time - start_time} seconds")

# Save the results to a CSV file
similar_names_df.to_csv('jaro_output.csv', index=False)
