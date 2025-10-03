import pandas as pd
import os
import sys

data_dir = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data"
# Get command-line arguments
if len(sys.argv) == 5:
    confirmed_1_path = sys.argv[1]
    updated_gender_path = sys.argv[2]
    data_dir = sys.argv[3]
    gender_path = sys.argv[4]
else:
    print('WARNING: Not using Makefile Input')
    confirmed_1_path = os.path.join(data_dir, 
    "derived/auxiliary/confirmed_1.csv")
    updated_gender_path = os.path.join(data_dir, 
    "derived/auxiliary/updated_gender.csv")
    gender_path = os.path.join(data_dir, "supplemental/gender.csv")

sys.path.append(os.path.join(os.path.dirname(__file__), 'helper_scripts'))

# LOAD HELPER FILES
import blocking_helper

#path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/auxiliary/confirmed_1.csv"

cleaned_confirmed = pd.read_csv(confirmed_1_path)

contact_dict = {}
for _, row in cleaned_confirmed.iterrows():
    contact_id = row['contact_uniqueid']
    firstname = row['firstname']
    if contact_id in contact_dict:
        if firstname not in contact_dict[contact_id]:
            contact_dict[contact_id].append(firstname)
    else:
        contact_dict[contact_id] = [firstname]

# Convert the dictionary to a DataFrame
contact_list = [{'contact_uniqueid': key, 
                **{f'name_{i+1}': name for i, name in enumerate(value)}} 
                for key, value in contact_dict.items()]
confirmed_names = pd.DataFrame(contact_list)

gender_df = pd.read_csv(gender_path)

# using metaphone to reassign gender
cleaned_df = blocking_helper.clean_for_metaphone(confirmed_names[['name_1', 
                                                                    'name_2',
                                                                    'name_3']])
metaphone_dict, name_to_metaphone = blocking_helper.generate_metaphone(cleaned_df)
updated_genders = blocking_helper.impute_gender_by_metaphone(gender_df, 
                                                                    metaphone_dict, 
                                                                    name_to_metaphone)
updated_genders.loc[updated_genders['firstname'] == 'sandra', 'gender'] = 'F'
updated_genders.loc[updated_genders['firstname'] == 'sondra', 'gender'] = 'F'
updated_genders.loc[updated_genders['firstname'] == 'pam', 'gender'] = 'F'
updated_genders.loc[updated_genders['firstname'] == 'val', 'gender'] = 'F'
updated_genders.loc[updated_genders['firstname'] == 'patty', 'gender'] = 'F'
updated_genders.loc[updated_genders['firstname'] == 'patti', 'gender'] = 'F'
updated_genders.loc[updated_genders['firstname'] == 'pat', 'gender'] = 'F'
updated_genders.loc[updated_genders['firstname'] == 'sondra', 'gender'] = 'F'

updated_genders.to_csv(updated_gender_path, index=False)