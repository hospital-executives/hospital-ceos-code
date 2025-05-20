This folder contains proof of concept code that scrapes archived press released on Justice.gov.

The folder contains two python files:

1) proof_of_concept_doj_scraper_post2010.py

2) proof_of_concept_doj_scraper_pre2010.py

proof_of_concept_doj_scraper_post2010.py scrapes the first 31 pages of the [2010-2025 archives](https://www.justice.gov/archives/press-releases-archive). For each article, it extracts the URL, the title, the date, the text, any topic 'tags', and the Attorney General office's region. Results are exported to the file dojarchivepost2010.csv.
(Note the file will not work unless the user modifies the path that saves the csv  in line 108). 

proof_of_concept_doj_scraper_pre2010.py scrapes articles from the most recent 30 months of the [pre-2010 archives](https://www.justice.gov/archives/justice-news-archive). For each article, it extracts the URL, the title, the date, and the text. Results are exported to the file dojarchivepre2010.csv.
(Note the file will not work unless the user modifies the path that saves the csv  in line 82.)

Python packages required:
"pandas",
"requests",
"bs4",
