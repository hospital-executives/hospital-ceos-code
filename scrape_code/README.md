This folder contains code that scrapes archived press releases on Justice.gov.

The folder contains four python files:

1) proof_of_concept_doj_scraper_post2010.py

2) proof_of_concept_doj_scraper_pre2010.py

3) post2010dojcleaner.py

4) pre2010dojcleaner.py

proof_of_concept_doj_scraper_post2010.py scrapes the [2010-2025 archives](https://www.justice.gov/archives/press-releases-archive). For each article, it extracts the URL, the title, the date, the text, any topic 'tags', and the Attorney General office's region. Results are exported to the file dojarchivepost2010.csv. (A log file - post2010log.log, and a csv file containing a record of errors encountered while scraping, should errors exist - dojerrorspost2010.csv are also created.)
(Note the file will not work unless the user modifies the paths in lines 11, 131 and 132.) 

proof_of_concept_doj_scraper_pre2010.py scrapes articles from the [pre-2010 archives](https://www.justice.gov/archives/justice-news-archive). For each article, it extracts the URL, the title, the date, and the text. Results are exported to the file dojarchivepre2010.csv. (A log file - pre2010log.log, and a csv file containing a record of errors encountered while scraping, should errors exist - dojerrorspre2010.csv are also created.)
(Note the file will not work unless the user modifies the paths in lines 12, 154 and 155.)

post2010dojcleaner.py reads dojarchivepost2010.csv to a dataframe and adds the full text of articles, when they are missing, to the dataframe, and exports this to dojarchivepost2010cleaned.csv. (Note, the file will not work unless the user modifies the paths in lines 3 and 25.)

pre2010dojcleaner.py reads dojarchivepre2010.csv to a dataframe and adds the title and date of articles, when they are missing, to the dataframe, and exports this to dojarchivepre2010cleaned.csv. (Note, the file will not work unless the user modifies the paths in lines 3 and 97.)

Python packages required:
"pandas",
"requests",
"bs4",
"logging"
