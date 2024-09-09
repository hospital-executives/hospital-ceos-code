import os
import sys
import pandas as pd

path_source = sys.argv[1]
path = os.path.join(path_source, "male_diminutives.csv")
print(path)

# Check if the path exists
if os.path.exists(path):
    print(f"The path exists: {path}")
    
    # Check if it's a file
    if os.path.isfile(path):
        print(f"The path is a file: {path}")
    
    # Check if it's a directory
    elif os.path.isdir(path):
        print(f"The path is a directory: {path}")
    else:
        print(f"The path is neither a file nor a directory: {path}")
else:
    print(f"The path does not exist: {path}")