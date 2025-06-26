
import pandas as pd
from flashtext import KeywordProcessor
import re
from collections import defaultdict

# Loading aha feather file - note change path.
df = pd.read_feather(r'C:\Users\amana\Dropbox\Hospital Exec\hospital_ceos\_data\derived/final_confirmed_aha_update_530.feather') #Change path


csuite_execs = df[df['c_suite'] == 1]['contact_uniqueid'].unique() # This gets a unique list of c-suite execs by contact id - it includes anyone who ever was a c suite exec.
df['exec_ever_csuite'] = df['contact_uniqueid'].isin(csuite_execs).astype(int) # Creates a new column which indicates if a particular individual was ever a c-suite exec
csuite_df = df[df['contact_uniqueid'].isin(csuite_execs)] # This forms a dataframe with only executives who ever were c suite execs.
csuite_df = csuite_df.groupby(['contact_uniqueid', 'year', 'entity_uniqueid']).first().reset_index() # converts the dataframe so each entry is a unique person X facility X year combination.
filename = r'C:\Users\amana\Files Python\csuite.feather' # Saves filename to csuite.feather - note: change file path.

if not csuite_df.empty:
    csuite_df.to_feather(filename)
    print(f"Data saved to {filename}")
# exports csuite_df to a feather file.

pre2010df = pd.read_csv(r'C:\Users\amana\Dropbox\Hospital Exec\hospital_ceos\_data\scrape_output\dojarchivepre2010cleaned.csv')
# generates a dataframe based on the cleaned pre2010 DOJ archives csv.

post2010df = pd.read_csv(r'C:\Users\amana\Dropbox\Hospital Exec\hospital_ceos\_data\scrape_output\dojarchivepost2010cleaned.csv')
# generates a dataframe based on the cleaned post2010 DOJ archives csv.


names = csuite_df['firstname'].str.strip() + ' ' + csuite_df['lastname'].str.strip()
#takes the first and last name of each executive in an executive-facility-year combination and adds them to the list names.

lastnames = csuite_df['lastname'].str.strip()
#takes the last name of each executive in an executive-facility-year combination and adds them to the list names.


# The following section creates a dictionary which uses first names as a key, and the values of the first name correspond to their 'nicknames'.
#Note currently unable to incorporate nickname matching, but the dictionary code is in the comment below.

#tag_to_names = csuite_df.groupby('first_component')['firstname'].apply(list).to_dict() 
# Creates a dictionary where the keys are the values of the first component, and the values are the first names associated with each first component.


#name_to_nicknames = {} # 

#for tag, name_list in tag_to_names.items():
    #for name in name_list:
        #name_to_nicknames[name] = name_list

#We can use keyword processor from the FlashText module, to find name matches in text of DOJ articles

# setting up keyword processor

kp = KeywordProcessor(case_sensitive=False)

# Adds each executive name to the keyword processor as a keyword
for name in names:
    kp.add_keyword(name)


name_to_indices = {name: [] for name in names} 
# Creates a name to index dictionary, will eventually contain row numbers in post2010 and pre2010 DOJ dataframes where names are found.


# The below loops over each row in the Full Text column in the pre2010 dataframe - idx is the row index number, text is the full text.
# The loops checks that the text is a string, gets a list of names of executives found in that specific text article
# and for each found name, appends the row index number to a list associated with the name in the name_to_indices dictionary.
for idx, text in pre2010df['Full Text'].items():
    if not isinstance(text, str):
        continue
    found_names = kp.extract_keywords(text)
    for name in found_names:
        name_to_indices[name].append(idx)


# Setting up another keyword processor for lastnames
kp2 = KeywordProcessor(case_sensitive=False)

#Adds each executive lastname as a keyword
for name in lastnames:
    kp2.add_keyword(name)

# Creates a name to index dictionary, will eventually contain row numbers in post2010 and pre2010 DOJ dataframes where lastnames are found.
lastname_to_indices = {name: [] for name in lastnames}


# The below loops over each row in the Full Text column in the pre2010 dataframe, as above, except here it looks for lastname matches, not full name matches. 
for idx, text in pre2010df['Full Text'].items():
    if not isinstance(text, str):
        continue
    found_names = kp2.extract_keywords(text)
    for name in found_names:
        lastname_to_indices[name].append(idx)

# Creates two new columns in csuite_df, # for each row with a corresponding executive's name,
# the first records the row index numbers in the pre2010 dataframe where the name can be found in the DOJ articles
# the second records the row index numbers in the pre2010 dataframe where the last name can be found in the DOJ articles.

csuite_df['Row Indices Full Name (pre2010)'] = [name_to_indices[name] for name in names]
csuite_df['Row Indices Last Name (pre2010)'] = [lastname_to_indices[name] for name in lastnames]


# Creates another keyword processor, and searches for names. Here, kp3 looks at finding full names in the post2010 DOJ articles.

kp3 = KeywordProcessor(case_sensitive=False)
for name in names:
    kp3.add_keyword(name)

post_name_to_indices = {name: [] for name in names}

for idx, text in post2010df['Full Text'].items():
    if not isinstance(text, str):
        continue
    found_names = kp3.extract_keywords(text)
    for name in found_names:
        post_name_to_indices[name].append(idx)

#Creates another keyword processor, kp4. This tries to find lastnames for post2010 DOJ articles.        

kp4 = KeywordProcessor(case_sensitive=False)
for name in lastnames:
    kp4.add_keyword(name)

post_lastname_to_indices = {name: [] for name in lastnames}

for idx, text in post2010df['Full Text'].items():
    if not isinstance(text, str):
        continue
    found_names = kp4.extract_keywords(text)
    for name in found_names:
        post_lastname_to_indices[name].append(idx)


csuite_df['Row Indices (post2010)'] = [post_name_to_indices[name] for name in names]
csuite_df['Row Indices LastName (post2010)'] = [post_lastname_to_indices[name] for name in lastnames]


csuite_df['Name'] = [name for name in names] # Creates a column for the corresponding full name of executives.

keep_columns = ['Name', 'firstname', 'lastname', 'full_name', 'first_component', 'last_component', 'Row Indices Full Name (pre2010)', 'Row Indices Last Name (pre2010)', 'Row Indices (post2010)', 'Row Indices LastName (post2010)']

namematch = csuite_df[keep_columns] # Keeps the name, component and the row index of the pre and post 2010 DOJ dataframes where names can be found.

namematch_unique = namematch.drop_duplicates(subset='Name', keep='first') # Drops rows if their names are duplicated.

filename = r'C:\Users\amana\Files Python\names.feather' # Saves to names feather - note change file


if not namematch_unique.empty:
    namematch_unique.to_feather(filename)
    print(f"Data saved to {filename}")
# Saves to feather file