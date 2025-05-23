This folder contains the code to create the cleaned HIMSS files.

Make sure your _data folder is up to date before re-running the Makefile.

From the root of this directory, you should be able to run 'make' in terminal
and completely regenerate the two final products:
1) final_himss.feather, from which we can build our data for machine learning 
in the RCC
2) final_confirmed.dta, from which Maggie and Ambar can conduct their data
analysis

final_himss.feather contains all valid (i.e., U.S. entities, non-missing individual names) observations from the initial HIMSS data. final_confirmed.dta contains only the observations for which we each contact_uniqueid in the dataset is a 1:1 match.

This folder contains two subfolders:
1) helper-scripts, which contains all helper files necessary to generate the
aforementioned files
2) aux-scripts, which contains Katherine's exploratory work at different stages
of the process

R packages required (you can also open "config.R" and it will prompt you to install anything you don't already have: 
"tidyverse",
  "readr",
  "readxl",
  "tools",
  "haven",
  "foreign",
  "wordcloud",
  "feather",
  "tibble",
  "jsonlite",
  "stringi",
  "fuzzyjoin",
  "arrow",
  "stringdist",
  "data.table",
  "phonics",
  "scales",
  "knitr",
  "writexl",
  "stringr"

Python packages required can be found in helper_scripts/py_requirements.txt and are handled in clean_data.py.

If errors related to missing columns, files, etc. persist, check that the files are up to date from Dropbox.
