This folder contains code that scrapes archived press releases on Justice.gov, then creates summary statistics, and searches for hospital executive names in the press releases.

The folder contains six python files:

1) proof_of_concept_doj_scraper_post2010.py

2) proof_of_concept_doj_scraper_pre2010.py

3) post2010dojcleaner.py

4) pre2010dojcleaner.py

5) hospital_exec_namesearch.py

6) article_namesearch_descriptive_stats.py

proof_of_concept_doj_scraper_post2010.py scrapes the [2010-2025 archives](https://www.justice.gov/archives/press-releases-archive). For each article, it extracts the URL, the title, the date, the text, any topic 'tags', and the Attorney General office's region. Results are exported to the file dojarchivepost2010.csv. (A log file - post2010log.log, and a csv file containing a record of errors encountered while scraping, should errors exist - dojerrorspost2010.csv are also created.)
(Note the file will not work unless the user modifies the paths in lines 11, 131 and 132.) 

proof_of_concept_doj_scraper_pre2010.py scrapes articles from the [pre-2010 archives](https://www.justice.gov/archives/justice-news-archive). For each article, it extracts the URL, the title, the date, and the text. Results are exported to the file dojarchivepre2010.csv. (A log file - pre2010log.log, and a csv file containing a record of errors encountered while scraping, should errors exist - dojerrorspre2010.csv are also created.)
(Note the file will not work unless the user modifies the paths in lines 12, 154 and 155.)

post2010dojcleaner.py reads dojarchivepost2010.csv to a dataframe and adds the full text of articles, when they are missing, to the dataframe, and exports this to dojarchivepost2010cleaned.csv. (Note, the file will not work unless the user modifies the paths in lines 3 and 25.)

pre2010dojcleaner.py reads dojarchivepre2010.csv to a dataframe and adds the title and date of articles, when they are missing, to the dataframe, and exports this to dojarchivepre2010cleaned.csv. (Note, the file will not work unless the user modifies the paths in lines 3 and 97.)

hospital_exec_namesearch.py reads the aha file final_confirmed_aha_update_530.feather, and among execs who ever were c-suite execs, collapses data such that each observation is a unique person X facility X year. This dataframe is saved as csuite.feather.
The file then reads dojarchivepost2010cleaned.csv and dojarchivepre2010cleaned.csv, and searches for names of hospital execs in the post 2010 and pre 2010 DOJ archive articles.
The names.feather file is outputted, where where each row represents a unique name and certain columns indicate the row index values in the pre2010 and post2010 dataframes where the corresponding full name or last name can be found. (Note the file will not work unless the user modifies the paths in lines 8.)

article_namesearch_descriptive_stats.py generates descriptive statistics. It separately generates statistics for the pre2010 and post2010 archives reading dojarchivepre2010cleaned.csv and dojarchivepost2010cleaned.csv as dataframes (Note the path on line 6 must be changed or else the file will not run correctly).
For the pre2010 archives, it separately plots the proportion of articles, by year with the following characteristics.
a) The article text has 'health', 'hospital', 'medicare', or 'medicaid' (original keywords)
b) The article text has 'hospital', 'medicare', or 'medicaid' (shortenened keywords)
c) The article text has 'healthcare', 'health care', 'hospital', or 'medicaid', or the article text has 'medicare', not as part of 'social security, medicare' (refined keywords)
The figure is saved as DOJ_Pre2010__Yearly_Health_Article_Proportion.png. 

For the post 2010 archives it separately plots the proportion of articles, by year, with the following characteristics
a) The article text has 'health', 'hospital', 'medicare', or 'medicaid' (original keywords)
b) The article text has 'hospital', 'medicare', or 'medicaid' (shortenened keywords)
c) The article text has 'healthcare', 'health care', 'hospital', or 'medicaid', or the article text has 'medicare', not as part of 'social security, medicare' (refined keywords)
d) The article is tagged with 'health care fraud'.
The figure is saved as DOJ_Post2010__Yearly_Health_Article_Proportion.png. 

Moreover, the python file reads the names.feather file, and generates summary statistics indicating the total number and proportion of unique names found in the pre and post 2010 DOJ archives.

These are saved as DOJ_fullname_matches_totals.png and DOJ_fullname_matches_proportion.png respectively.

Python packages required:
"pandas",
"requests",
"bs4",
"logging",
"pyarrow", 
"flashtext", 
"dateparser",
"matplotlib"
