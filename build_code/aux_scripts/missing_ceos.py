import os
import pandas as pd

user_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data"

himss_path = os.path.join(user_path, 
    "derived/himss_entities_contacts_0517_v1.feather")

himss = pd.read_feather(himss_path)

ceos = himss[(himss['title_standardized'] == 'CEO:  Chief Executive Officer')]
ceos['missing_ceo'] = ceos['lastname'].isna()
missing_ceos = ceos[['entity_uniqueid', 'year', 'missing_ceo']]

any_missing = himss.copy()
any_missing['missing_title'] = any_missing['lastname'].isna()

ceos.to_csv("/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/missing_ceos.csv")

any_missing.to_csv("/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/any_missing.csv")
