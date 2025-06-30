import pandas as pd
import dateparser
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

DROPBOX_PATH = r"C:\Users\amana\Dropbox\Hospital Exec\hospital_ceos" # Setting dropbox path - note change path

###Summary statistics for pre2010 DOJ archive articles

pre2010df = pd.read_csv(rf'{DROPBOX_PATH}\_data\scrape_output\dojarchivepre2010cleaned.csv') 
# Generates a dataframe based on the cleaned pre2010 DOJ archives csv.


#Creates a new column for whether articles have health words - here health, hospital, medicaid, and medicare and assigns the value as 0.
pre2010df["Health Words (health, hospital, medicaid, medicare)"] = 0
keywords_original = ['hospital', 'health', 'medicaid', 'medicare'] # Creates a list of keywords that will be searched for in the article texts.

#Creates a new column for whether articles have health words - here: hospital, medicaid, medicare
pre2010df["Health Words (hospital, medicaid, and medicare)"] = 0
keywords_shortened = ['hospital', 'medicaid', 'medicare'] # Creates another list of keywords that is searched for in article texts

#Creates a new column for if articles have health words - we remove false positives. Health care/healthcare is used as a keyword instead of health, and 
# medicare is not used as a keyword due to the phrase 'social security, medicare' when referring to tax fraud. 
# (Later, articles are checked if the amount of medicare mentions is more than the times 'social security, medicare' is mentioned.)
pre2010df["Health Words (Most false positives removed)"] = 0 
keywords_refined = ['healthcare', 'health care', 'medicaid', 'hospital']


# This iterates over every row and corresponding full text article in the pre2010 DOJ archives 
# and checks if the text contains any of the keywords: health, hospital, medicaid, medicare

for i, text in enumerate(pre2010df['Full Text']):
    if isinstance(text, str) and any(word in text.lower() for word in keywords_original):
        pre2010df.at[i, 'Health Words (health, hospital, medicaid, medicare)'] = 1

# This iterates over every row and corresponding full text article in the pre2010 DOJ archives 
# and checks if the text contains any of the keywords: hospital, medicaid, medicare

for i, text in enumerate(pre2010df['Full Text']):
    if isinstance(text, str) and any(word in text.lower() for word in keywords_shortened):
        pre2010df.at[i, "Health Words (hospital, medicaid, and medicare)"] = 1

# This iterates over every row and corresponding full text article in the pre2010 DOJ archives 
# and checks if the text contains any of the keywords: 'health care', 'healthcare', 'hospital', 'medicaid', 
# or if the text contains 'medicare' which is not part of 'social security, medicare'.

for i, text in enumerate(pre2010df['Full Text']):
    if isinstance(text, str):
        lower_text = text.lower()

        medicare_count = lower_text.count('medicare')

        # Counts how many are part of "social security, medicare"
        soc_security_medicare_count = lower_text.count('social security, medicare')

        # Checks if there is any medicare mention not in the excluded phrase - 'social security, medicare', if so, makes a note of this
        if medicare_count > soc_security_medicare_count:
            pre2010df.at[i, 'Health Words (Most false positives removed)'] = 1
        elif any(word in lower_text for word in keywords_refined):
            pre2010df.at[i, 'Health Words (Most false positives removed)'] = 1


pre2010df.at[4560, 'date'] = "3/30/2002" # This specific date behaves strangely, so it is manually entered here. 


# Since date formats are inconsistent, the below uses the dateparser module to convert all dates to the same format (month-day-year).
pre2010df['parsed_date'] = pre2010df['Date'].apply(lambda x: dateparser.parse(x, settings={'DATE_ORDER': 'MDY'}) if isinstance(x, str) else None) 

#Grabs the specific year associated with article. 

pre2010df['year'] = pre2010df['parsed_date'].dt.year


# Gets dataframes which indicates the proportion of articles which have different set of keywords out of the total number of articles by year.

yearly_percent_keywords_original = pre2010df.groupby('year')['Health Words (health, hospital, medicaid, medicare)'].mean().reset_index()
yearly_percent_keywords_shortened= pre2010df.groupby('year')['Health Words (hospital, medicaid, and medicare)'].mean().reset_index()
yearly_percent_keywords_refined= pre2010df.groupby('year')['Health Words (Most false positives removed)'].mean().reset_index()


# Creates a plot, which maps the proportion of health related articles over time - with different qualifications.
plt.figure(figsize=(10, 6)) 
plt.plot(yearly_percent_keywords_original['year'], yearly_percent_keywords_original['Health Words (health, hospital, medicaid, medicare)'], label='Percent Original Keywords', marker='o')
plt.plot(yearly_percent_keywords_shortened['year'], yearly_percent_keywords_shortened['Health Words (hospital, medicaid, and medicare)'], label='Percent Shortned Keywords', marker='o')
plt.plot(yearly_percent_keywords_refined['year'], yearly_percent_keywords_refined['Health Words (Most false positives removed)'], label='Percent Refined Keywords', marker='o')
plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

# Setting Labels and Title
plt.title('Percentage of Health Related Articles Over Time (1994-2009)')
plt.xlabel('Year')
plt.ylabel('Percentage')

plt.legend()
plt.grid(True)
plt.tight_layout()

plt.ylim(0, 0.3)



# Save the figure
plt.savefig(rf'{DROPBOX_PATH}\_data\scrape_output\DOJ_Pre2010__Yearly_Health_Article_Proportion.png', dpi=300)
plt.close()
### Summary statistics for post 2010 DOJ archives.

post2010df = pd.read_csv(rf'{DROPBOX_PATH}\_data\scrape_output\dojarchivepost2010cleaned.csv')
# Generates a dataframe based on the cleaned post2010 DOJ archives csv.


#Creates a new column for whether articles have health words - here health, hospital, medicaid, and medicare and assigns the value as 0.
post2010df["Health Words (health, hospital, medicaid, medicare)"] = 0

#Creates a new column for whether articles have health words - here: hospital, medicaid, medicare
post2010df["Health Words (hospital, medicaid, and medicare)"] = 0

#Creates a new column for if articles have health words - we remove false positives. Health care/healthcare is used as a keyword instead of health, and 
# medicare is not used as a keyword due to the phrase 'social security, medicare' when referring to tax fraud. 
# (Later, articles are checked if the amount of medicare mentions is more than the times 'social security, medicare' is mentioned.)
post2010df["Health Words (Most false positives removed)"] = 0 




#Lines 126-148 are identical to the process for the pre2010 dataframe in lines 32-60.

for i, text in enumerate(post2010df['Full Text']):
    if isinstance(text, str) and any(word in text.lower() for word in keywords_original):
        post2010df.at[i, 'Health Words (health, hospital, medicaid, medicare)'] = 1


for i, text in enumerate(post2010df['Full Text']):
    if isinstance(text, str) and any(word in text.lower() for word in keywords_shortened):
        post2010df.at[i, "Health Words (hospital, medicaid, and medicare)"] = 1

for i, text in enumerate(post2010df['Full Text']):
    if isinstance(text, str):
        lower_text = text.lower()

        medicare_count = lower_text.count('medicare')

        # Counts how many are part of "social security, medicare"
        soc_security_medicare_count = lower_text.count('social security, medicare')

        # Checks if there is any medicare mention not in the excluded phrase - 'social security, medicare', if so, makes a note of this
        if medicare_count > soc_security_medicare_count:
            post2010df.at[i, 'Health Words (Most false positives removed)'] = 1
        elif any(word in lower_text for word in keywords_refined):
            post2010df.at[i, 'Health Words (Most false positives removed)'] = 1

# We also want to check if the topic tag of the post2010 articles is related to health care fraud.

post2010df["Health Fraud Tag"] = 0

topicwords = ['health care fraud'] #Indicates topic word to search through to see if topic tag is related to healthcare.

# This iterates over every row and corresponding topic tag in the post2010 DOJ archives 
# and checks if the topic tag is 'health care fraud'.

for i, text in enumerate(post2010df['Topic']):
    if isinstance(text, str) and any(word in text.lower() for word in topicwords):
        post2010df.at[i, 'Health Fraud Tag'] = 1


#Below converts all dates to a single format.
post2010df['parsed_date'] = post2010df['Date'].apply(lambda x: dateparser.parse(x, settings={'DATE_ORDER': 'MDY'}) if isinstance(x, str) else None) 

#Grabs the year associated with each article.
post2010df['year'] = post2010df['parsed_date'].dt.year

# Gets dataframes which indicates the proportion of articles which have different set of keywords out of the total number of articles by year.
yearly_percent_keywords_original = post2010df.groupby('year')['Health Words (health, hospital, medicaid, medicare)'].mean().reset_index()
yearly_percent_keywords_shortened= post2010df.groupby('year')['Health Words (hospital, medicaid, and medicare)'].mean().reset_index()
yearly_percent_keywords_refined= post2010df.groupby('year')['Health Words (Most false positives removed)'].mean().reset_index()
yearly_percent_health_fraud_tag= post2010df.groupby('year')['Health Fraud Tag'].mean().reset_index()

#Ploting the figure.

plt.figure(figsize=(10, 6)) 

plt.plot(yearly_percent_keywords_original['year'], yearly_percent_keywords_original['Health Words (health, hospital, medicaid, medicare)'], label='Percent Original Keywords', marker='o')
plt.plot(yearly_percent_keywords_shortened['year'], yearly_percent_keywords_shortened['Health Words (hospital, medicaid, and medicare)'], label='Percent Shortned Keywords', marker='o')
plt.plot(yearly_percent_keywords_refined['year'], yearly_percent_keywords_refined['Health Words (Most false positives removed)'], label='Percent Refined Keywords', marker='o')
plt.plot(yearly_percent_health_fraud_tag['year'], yearly_percent_health_fraud_tag['Health Fraud Tag'], label='Percent Tagged as Health Fraud', marker='o')
plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))


# Setting Title and Axis Labels.
plt.title('Percentage of Health Related Articles Over Time (2009-2025)')
plt.xlabel('Year')
plt.ylabel('Percentage')

plt.legend()
plt.grid(True)
plt.tight_layout()

plt.ylim(0, 0.3)



# Saving the figure
plt.savefig(rf'{DROPBOX_PATH}\_data\scrape_output\DOJ_Post2010__Yearly_Health_Article_Proportion.png', dpi=300)
plt.close()
### Summary statistics for name search matching - number andproportion of unique names matched 
# in pre and post 2010 DOJ archives.


exec_names_df = pd.read_feather(rf'{DROPBOX_PATH}\_data\scrape_output\names.feather') #Grabbing namesearch feather file and converting it to a dataframe


# Below checks if whether the full name was found in a pre 2010 or post 2010 DOJ arcticle for each name (and therefore corresponding cell indicating row indice numbers where the name came be found are non empty)
# And gives a binary (1/0) value to the foundpre and foundpost columns if the name was found in the older and later archives respectively.
exec_names_df['foundpre'] = exec_names_df['Row Indices Full Name (pre2010)'].apply(lambda x: int(len(x) if isinstance(x, (list, tuple)) else x.size > 0))
exec_names_df['foundpost'] = exec_names_df['Row Indices (post2010)'].apply(lambda x: int(len(x) if isinstance(x, (list, tuple)) else x.size > 0))

total_names_found_pre = exec_names_df['foundpre'].sum() # Gets number of names found in the pre2010 DOJ archives.
total_names_found_post = exec_names_df['foundpost'].sum() # Gets number of names found in the post2010 DOJ archives.
prop_names_found_pre = exec_names_df['foundpre'].mean() # Gets proportion of names found in the pre2010 DOJ archives.
prop_names_found_post = exec_names_df['foundpost'].mean() # Gets proportion of names found in the post2010 DOJ archives.

# Plotting total number of names found

labels = ['Total Pre-2010', 'Total Post-2010']
values = [total_names_found_pre, total_names_found_post]

plt.bar(labels, values)
plt.ylabel('Count')
plt.title('Number of Names Matched In the Pre and Post 2010 DOJ Archives (Out of 49746)', fontsize=10)
plt.xticks(rotation=15)

#Saving the Figure
plt.savefig(rf'{DROPBOX_PATH}\_data\scrape_output\DOJ_fullname_matches_totals.png', dpi=300)
plt.close()

#Plotting proportion of names found.
labels = ['Proportion Pre-2010', 'Proportion Post-2010']
values = [prop_names_found_pre, prop_names_found_post]

plt.bar(labels, values)
plt.xticks(fontsize=8)
plt.ylabel('Proportion')
plt.title('Proportion of Names Matched In the Pre and Post 2010 DOJ Archives (Out of 49746)', fontsize=10)
plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
plt.xticks(rotation=15)

#Saving the figure.
plt.savefig(rf'{DROPBOX_PATH}\_data\scrape_output\DOJ_fullname_matches_proportion.png', dpi=300)
plt.close()

