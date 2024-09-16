# This Python script generates the digraphs (ie mappings between names and
# their diminutives) used in the final script to clean the data.
# 
import sys
import os 
import pandas as pd
import networkx as nx
from metaphone import doublemetaphone

# define the following
cleaned_himss_path = sys.argv[1]
himss_nicknames_path = sys.argv[2] 
digraphname_path = sys.argv[3]
digraphmeta_path = sys.argv[4]
user_path = sys.argv[5]

sys.path.append(os.path.join(os.path.dirname(__file__), 'helper-scripts'))
# LOAD HELPER FILES
import blocking_helper
import cleaned_confirmed_helper as cc

# load dfs
cleaned_himss = pd.read_csv(cleaned_himss_path)
himss_nicknames = pd.read_csv(himss_nicknames_path)
female_path = os.path.join(user_path, "female_diminutives.csv")
female_df = blocking_helper.load_data(female_path, to_lower = True)
male_path = os.path.join(user_path, "male_diminutives.csv")
male_df = blocking_helper.load_data(male_path, to_lower = True)
carlton_path = os.path.join(user_path, 
                                    "carltonnorthernnames.csv")
carlton = blocking_helper.load_data(carlton_path)

# create name graph
G = nx.DiGraph() 

unique_cells = pd.unique(cleaned_himss["firstname"].values.ravel('K'))
non_nan_mask = ~pd.isna(unique_cells)
cleaned_array = unique_cells[non_nan_mask]
unique_cells = [cell for cell in cleaned_array if cell and cell.strip()]

 # Add nodes from the first DataFrame
cleaned_unique_firstnames = cleaned_himss['firstname'].dropna().unique()
for name in cleaned_unique_firstnames:
    G.add_node(name)

for edge_df in [male_df, female_df, himss_nicknames, carlton]:
    cc.add_edges_from_df(edge_df, G)
        
_, name_to_metaphone = cc.generate_metaphone(cleaned_himss)

G_metaphone = nx.DiGraph()

# Add nodes with metaphone codes to the new graph
for node in G.nodes():
    if pd.notna(node):  # Ensure the node is not nan
        metaphone_code = name_to_metaphone.get(node)
        if metaphone_code:  # Ensure metaphone_code is not None
            G_metaphone.add_node(metaphone_code)
        #else:
            #new_metaphone = doublemetaphone(node)[0]
            #G_metaphone.add_node(new_metaphone)

# Add edges with metaphone codes to the new graph
for edge in G.edges():
    node1, node2 = edge
    metaphone_code1 = name_to_metaphone.get(node1)
    metaphone_code2 = name_to_metaphone.get(node2)
    if metaphone_code1 and metaphone_code2:  # Ensure both metaphone codes are not None
        G_metaphone.add_edge(metaphone_code1, metaphone_code2)

# write to csv
edges = G.edges(data=True)
edge_list = [(u, v, d.get('weight', 1)) for u, v, d in edges]
df = pd.DataFrame(edge_list, columns=['source', 'target', 'weight'])
df.to_csv(digraphname_path, index=False)

edges = G_metaphone.edges(data=True)
edge_list = [(u, v, d.get('weight', 1)) for u, v, d in edges]
df = pd.DataFrame(edge_list, columns=['source', 'target', 'weight'])
df.to_csv(digraphmeta_path, index=False)