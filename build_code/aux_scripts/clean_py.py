import os
import pandas as pd

user_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data"

from ..helper-scripts import cleaned_confirmed_helper as cc

# load files
confirmed_path =  os.path.join(user_path, "derived/auxiliary/py_confirmed.csv")
remainder_path =  os.path.join(user_path, "derived/auxiliary/py_remaining.csv")

py_confirmed = pd.read_csv(confirmed_path)
py_remainder = pd.read_csv(remainder_path)

