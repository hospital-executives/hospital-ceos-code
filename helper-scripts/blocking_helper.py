import csv
import pandas as pd
import pyarrow
import numpy as np
import re
import os
import itertools
import networkx as nx
import Levenshtein as lev
import matplotlib.pyplot as plt
import seaborn as sns

from metaphone import doublemetaphone
from itertools import combinations

## FUNCTIONS TO LOAD DATA
def load_data(file_path:str, to_lower = None):
    """
    Loads a CSV file into a pandas DataFrame, normalizes the row lengths, and 
    optionally converts string values to lowercase.

    Parameters
    ----------
    file_path : str
        The path to the CSV file to be loaded.
    to_lower : function or None, optional
        A function to apply to each element in the DataFrame to convert strings 
        to lowercase. If None, no conversion is applied (default is None).

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the data from the CSV file, with 
        normalized row lengths and optionally converted to lowercase.
    """
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)
    max_len = max(len(row) for row in data)
    normalized_data = [row + [''] * (max_len - len(row)) for row in data]
    df = pd.DataFrame(normalized_data, columns=range(max_len))
    df.columns = range(df.shape[1])  # Default column names to integers
    df.replace('NA', pd.NA, inplace=True)
    if to_lower is None:
        return df

    def to_lower(x):
        if isinstance(x, str):
            return x.lower()
    return df.apply(lambda col: col.map(to_lower))

def load_himss(file_path:str):
    """
    Loads and processes HIMSS data from a feather file, applying various 
    data cleaning steps.

    Parameters
    ----------
    file_path : str
        The path to the feather file containing the HIMSS data.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the processed HIMSS data.
    """
    himss = pd.read_feather(file_path)
    himss = himss[himss['year'] > 2008]
    himss['full_name'] = himss['firstname'].str.cat(himss['lastname'], 
                                                    sep='').str.lower()
    himss['full_name'] = himss['full_name'].str.replace(r'[^\w\s]', '', 
                                                        regex=True)  # Remove punctuation
    himss['full_name'] = himss['full_name'].str.replace(r'\s+', '', regex=True)  # Remove whitespace
    himss = himss[~himss['entity_zip'].str.contains(r'[A-Za-z]', na=False)]
    himss = himss[~himss['entity_state'].str.contains(r'AB|BC|PE|NB|NL|NS|MB|SK|PR', 
                                                    na=False)]
    himss = himss[himss['firstname'].notna()]
    himss = himss.dropna(subset = ['entity_zip', 'entity_state', 'contact_uniqueid'])
    return himss


## FUNCTIONS TO CLEAN + FORMAT DATA
# nickname related functions
def drop_nickname(name):
    if isinstance(name, str):
        name = re.split(r'[\(\)"\']', name)[0].strip()
        name = name.lower()
        return name
    
def split_content(cell):
    """
    Splits the content of a cell into two parts based on patterns of parentheses 
    or quotes.

    Parameters
    ----------
    cell : str
        The input string that may contain content inside parentheses or any 
        type of quotes.

    Returns
    -------
    tuple
        A tuple containing two elements:
        - The part of the string outside the parentheses or quotes (or the 
          original string if no match is found).
        - The part of the string inside the parentheses or quotes, or None if 
           no match is found.
    """
    cell = str(cell)  # Ensure the cell is a string
    # Regular expression to match content inside (), '' or any type of quotes
    match = re.search(r'([^(]*)\(([^)]*)\)|([^"\'“”\(]*)["\'“”]([^"\'“”\)]*)["\'“”]', cell)
    if match:
        if match.group(1) is not None and match.group(2) is not None:
            # Match for ()
            return match.group(1).strip(), match.group(2).strip()
        elif match.group(3) is not None and match.group(4) is not None:
            # Match for any type of quotes
            return match.group(3).strip(), match.group(4).strip()
    return cell, None

def clean_for_metaphone(df):
    """
    Cleans a pandas DataFrame or Series for metaphone processing by filtering 
    out invalid names and splitting nicknames.

    Parameters
    ----------
    df : pd.DataFrame or pd.Series
        The input data containing names to be cleaned.

    Returns
    -------
    pd.DataFrame
        A DataFrame with two columns:
        - 'Before': The part of the name before the nickname or the original 
           name if no nickname is found.
        - 'Inside': The nickname or None if no nickname is found.
    """
    if isinstance(df, pd.DataFrame):
        stacked_series = df.stack()
    elif isinstance(df, pd.Series):
        stacked_series = df
    else:
        raise TypeError("Input must be a pandas DataFrame or Series")
    
    unique_values = pd.Series(stacked_series.unique())

    # Filter out values containing a period or a single letter followed by a space
    drop_initials = unique_values[~unique_values.str.contains(r'\.|^[A-Z]\s', regex=True)]
    drop_initials = drop_initials.dropna()

    # Convert the filtered unique values to a DataFrame
    unique_df = pd.DataFrame(drop_initials, columns=['Unique Names'])

    # Split firstnames of the following formats:
    # - Firstname (Nickname), Firstname 'Nickname', Firstname "Nickname"
    def split_content(cell):
        cell = str(cell)  # Ensure the cell is a string
        # Regular expression to match content inside (), '' or any type of quotes
        match = re.search(r'([^(]*)\(([^)]*)\)|([^"\'“”\(]*)["\'“”]([^"\'“”\)]*)["\'“”]', cell)
        if match:
            if match.group(1) and match.group(2):
                # Match for ()
                return match.group(1).strip(), match.group(2).strip()
            elif match.group(3) and match.group(4):
                # Match for any type of quotes
                return match.group(3).strip(), match.group(4).strip()
        return cell, None

    # Create a list to hold the new rows
    new_rows = []

    for cell in unique_df['Unique Names']:
        before, inside = split_content(cell)
        if inside:
            new_rows.append([before, inside])
        else:
            new_rows.append([before, None])
    
    new_df = pd.DataFrame(new_rows, columns=['Before', 'Inside'])

    return new_df

def clean_cells_for_metaphone(df, pattern =
                               r'([^(]*)\(([^)]*)\)|([^"\'“”\(]*)["\'“”]([^"\'“”\)]*)["\'“”]'):
    """
    Cleans cells in a DataFrame by applying a regex pattern to filter out 
    unwanted content.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing cells to be cleaned.
    pattern : str, optional
        The regex pattern used to identify and filter out unwanted content.
        The default pattern matches content inside parentheses, single or 
        double quotes, or any type of quotes.

    Returns
    -------
    pd.DataFrame
        A DataFrame with cells containing content matching the regex pattern 
        replaced with pandas' NA.
    """
    # Function to apply regex filtering
    def filter_cells(cell):
        if pd.isna(cell):
            return cell
        if re.search(pattern, str(cell)):
            return pd.NA
        return cell
    
    for col in df.columns:
        df[col] = df[col].map(filter_cells)
    
    return df

# gender related functions
def impute_gender_by_metaphone(gender, metaphone_dict, name_to_metaphone):
    """
    Imputes and updates gender based on a DataFrame of confirmed genders and 
    metaphone dictionaries.

    Parameters
    ----------
    gender : pd.DataFrame
        A DataFrame containing names and their confirmed genders with columns 
        ["firstname", "gender"].
    metaphone_dict : dict
        A dictionary where keys are metaphone codes and values are sets of names
        corresponding to those codes.
    name_to_metaphone : dict
        A dictionary where keys are names and values are their corresponding 
        metaphone codes.

    Returns
    -------
    pd.DataFrame
        A DataFrame with columns ['Name', 'Gender'] where the gender is 
        imputed based on metaphone statistics.
    """
    name_to_gender = gender.set_index("firstname")["gender"].to_dict()

    metaphone_stats = {}
    for code, names in metaphone_dict.items():
        total = len(names)
        count_f = sum(1 for name in names if name_to_gender.get(name) == "F")
        count_m = sum(1 for name in names if name_to_gender.get(name) == "M")
        count_unknown = total - count_f - count_m

        percent_f = count_f / total
        percent_m = count_m / total
        percent_unknown = count_unknown / total

        if percent_m == 0 and percent_unknown <= 0.2:
            gender = "F"
        elif percent_f == 0 and percent_unknown <= 0.2:
            gender = "M"
        else:
            gender = pd.NA
        
        metaphone_stats[code] = {
            "percent_f": percent_f,
            "percent_m": percent_m,
            "percent_unknown": percent_unknown,
            "assigned_gender": gender
        }

    name_gender_data = []

    for name, code in name_to_metaphone.items():
        if name in name_to_gender:
            temp_gender = name_to_gender[name]
            if abs(metaphone_stats[code]['percent_f'] - 
                   metaphone_stats[code]['percent_m']) <= 0.15:
                gender = metaphone_stats[code]['assigned_gender']
            elif not pd.isna(temp_gender):
                gender = temp_gender
            else:
                gender = metaphone_stats[code]['assigned_gender']
        
        name_gender_data.append({'Name': name, 'Gender': gender})

    new_df = pd.DataFrame(name_gender_data)
    new_df = new_df.rename(columns={"Name": "firstname", "Gender": "gender"})
    new_df['firstname'] = new_df['firstname'].str.lower()
    return new_df

def update_gender(row, pattern1):
    if pd.isna(row['Gender']) and re.search(pattern1, row['firstname']):
        row['Gender'] = 'M'
    return row

# other functions
def generate_metaphone(df):
    """
    Generates metaphone codes for names in a DataFrame and returns mappings 
    between names and their metaphone codes.

    Parameters
    ----------
    df : pd.DataFrame
        A pandas DataFrame containing names for which metaphone codes need to 
        be generated.

    Returns
    -------
    tuple of dict
        - metaphone_dict: A dictionary where the keys are metaphone codes and 
          the values are sets of names corresponding to those codes.
        - name_to_metaphone: A dictionary where the keys are names and the 
          values are their corresponding metaphone codes.
    """
    metaphone_dict = {}
    name_to_metaphone = {}
    for col in df.columns:
        for name in df[col].dropna():
            primary, secondary = doublemetaphone(name)
            if primary:
                if primary not in metaphone_dict:
                    metaphone_dict[primary] = set()
                metaphone_dict[primary].add(name)
                name_to_metaphone[name] = primary
            elif secondary:
                if secondary not in metaphone_dict:
                    metaphone_dict[secondary] = set()
                metaphone_dict[secondary].add(name)
                name_to_metaphone[name] = secondary
    return metaphone_dict, name_to_metaphone

def clean_lastnames(df, column):

    # List of suffixes to remove
    suffixes = [' II', ' Jr', ' Sr', ' Sr.', ' Jr.', ' III', ' IV', ' V', 
                ' Esq', ' M.D.', ' Ph.D.', ' D.D.S.', "M.B.A", 'MBA', 
                "jr", "sr", "ii", "iii", "iv", "v"]
    
    def remove_suffixes(name):
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        return name

    def remove_unwanted_chars(name):
    # Remove spaces, commas, apostrophes, and periods
        return re.sub(r"[ ,'.]", "", name)
    
    df[column] = df[column].apply(lambda x: remove_unwanted_chars(remove_suffixes(str(x))))
    df[column] = df[column].str.lower()
    return df

def get_metaphone(name):
        if isinstance(name, str):
            return doublemetaphone(name)[0]
        return None 


## GENERATE NAME MAPPINGS
def generate_name_mappings(cleaned_himss):
    firstname_to_lastnames = {}
    lastname_to_firstnames = {}

    # Populate the dictionaries
    unique_combos = cleaned_himss[['firstname', 'lastname']].drop_duplicates()
    for _, row in unique_combos.iterrows():
        firstname, lastname = row['firstname'], row['lastname']
        
        # Add to firstname_to_lastnames
        if firstname in firstname_to_lastnames:
            if lastname not in firstname_to_lastnames[firstname]:
                firstname_to_lastnames[firstname].append(lastname)
        else:
            firstname_to_lastnames[firstname] = [lastname]
        
        # Add to lastname_to_firstnames
        if lastname in lastname_to_firstnames:
            if firstname not in lastname_to_firstnames[lastname]:
                lastname_to_firstnames[lastname].append(firstname)
        else:
            lastname_to_firstnames[lastname] = [firstname]

    return firstname_to_lastnames, lastname_to_firstnames
    
def generate_firstname_mapping(cleaned_confirmed, firstname_to_contact_count,
                               firstname_to_lastnames, lastname_to_firstnames,
                               frequency_threshold = 1, lev_threshold = 0.25):

    unique_firstnames = cleaned_confirmed['firstname'].unique()

    checked_pairs = set()  # Set to keep track of checked pairs
    replacement_mapping = {}

    for firstname in unique_firstnames:
        if firstname_to_contact_count.get(firstname, 0) <= frequency_threshold:
            lastnames = firstname_to_lastnames.get(firstname, [])
            for lastname in lastnames:
                other_firstnames = lastname_to_firstnames.get(lastname, [])
                for other in other_firstnames:
                    pair = tuple(sorted([firstname, other]))
                    if pair not in checked_pairs:
                        checked_pairs.add(pair)  

                        # Compare with Levenshtein distance
                        max_len = max(len(firstname), len(other))
                        if max_len > 0 and firstname != other and \
                            lev.distance(firstname, other)/max_len <= lev_threshold:
                            if firstname_to_contact_count.get(firstname, 0) >= \
                                firstname_to_contact_count.get(other, 0):
                                more_frequent_name = firstname
                            else:
                                more_frequent_name = other

                            replacement_mapping[(firstname, 
                                                 lastname)] = more_frequent_name
                            replacement_mapping[(other, 
                                                 lastname)] = more_frequent_name
                            
    return replacement_mapping

def generate_lastname_mapping(cleaned_confirmed, lastname_to_contact_count,
                               firstname_to_lastnames, lastname_to_firstnames,
                               frequency_threshold = 1, lev_threshold = 0.25):
    
    unique_lastnames = cleaned_confirmed['lastname'].unique()

    checked_pairs = set()  # Set to keep track of checked pairs
    replacement_mapping = {}

    for lastname in unique_lastnames:
        if lastname_to_contact_count.get(lastname, 0) <= frequency_threshold:
            firstnames = lastname_to_firstnames.get(lastname, [])
            for firstname in firstnames:
                other_lastnames = firstname_to_lastnames.get(firstname, [])
                for other in other_lastnames:
                    # Create a pair tuple
                    pair = tuple(sorted([lastname, other]))

                    # Check if the pair has already been checked
                    if pair not in checked_pairs:
                        checked_pairs.add(pair)  # Add the pair to the set of checked pairs if not already checked

                        # Compare with Levenshtein distance
                        max_len = max(len(lastname), len(other))
                        if max_len > 0 and lastname != other and \
                            lev.distance(lastname, other)/max_len < lev_threshold:
                            if lastname_to_contact_count.get(lastname, 0) \
                                >= lastname_to_contact_count.get(other, 0):
                                more_frequent_name = lastname
                            else:
                                more_frequent_name = other

                            replacement_mapping[(lastname, 
                                                 firstname)] = more_frequent_name
                            replacement_mapping[(other, 
                                                 firstname)] = more_frequent_name
    return replacement_mapping


## GENERATE GRAPHS
def generate_name_graph(df, list = None):
    """
    Generates a graph where each cell in a DataFrame is a node and all cells in 
    a given row are connected by edges.

    Parameters
    ----------
    df : pd.DataFrame
        A pandas DataFrame where each cell in the 'firstname' column will be a 
        node in the graph.
    list : list of pd.DataFrame, optional
        A list of additional DataFrames. For each DataFrame, every cell in a 
        row will be connected by edges in the graph.

    Returns
    -------
    networkx.Graph
        A graph where nodes represent unique cells from the 'firstname' column 
        and edges connect cells from the same row.
    """
    G = nx.Graph()

    unique_cells = pd.unique(df["firstname"].values.ravel('K'))
    non_nan_mask = ~pd.isna(unique_cells)
    cleaned_array = unique_cells[non_nan_mask]
    unique_cells = [cell for cell in cleaned_array if cell and cell.strip()]

    # Add nodes from the first DataFrame
    for name in df['firstname'].unique():
        G.add_node(name)

    # Add edges from the second DataFrame
    def add_edges_from_df(df):
        for _, row in df.iterrows():
            # Drop NA values and get unique names
            names = row.dropna()
            names = names[names != ''].unique()
            for name1, name2 in combinations(names, 2):
                if G.has_node(name1) and G.has_node(name2):
                    G.add_edge(name1, name2)

    if list:
        for edge_df in list:
            add_edges_from_df(edge_df)

    return(G)

def generate_metaphone_graph(graph):
    """
    Generates a new graph with metaphone codes as nodes based on an input graph 
    with names as nodes.

    Parameters
    ----------
    graph : networkx.Graph
        An input graph where each node represents a name.

    Returns
    -------
    networkx.Graph
        A new graph where nodes represent metaphone codes and edges connect 
        nodes whose original names were connected in the input graph.
    """

    names = list(graph.nodes())
    df = pd.DataFrame(names, columns=["Name"])
    
    # Generate the metaphone mapping
    _, name_to_metaphone = generate_metaphone(df)
    
    # Create a new graph with metaphone codes
    G_metaphone = nx.Graph()
    
    # Add nodes with metaphone codes to the new graph
    for node in graph.nodes():
        if pd.notna(node):  # Ensure the node is not nan
            metaphone_code = name_to_metaphone.get(node)
            if metaphone_code:  # Ensure metaphone_code is not None
                G_metaphone.add_node(metaphone_code)
            else:
                new_metaphone = doublemetaphone(node)[0]
                G_metaphone.add_node(new_metaphone)
        
    # Add edges with metaphone codes to the new graph
    for edge in graph.edges():
        node1, node2 = edge
        metaphone_code1 = name_to_metaphone[node1]
        metaphone_code2 = name_to_metaphone[node2]
        G_metaphone.add_edge(metaphone_code1, metaphone_code2)
    
    return G_metaphone


## GENERATE BLOCKS
def generate_female_blocks(merged_df, himss_nicknames, user_path):
    # load relevant data frames
    test_df = merged_df[(merged_df['gender'] == "F") | 
                        (pd.isna(merged_df['gender']))]

    fem_path = os.path.join(user_path, "supplemental/female_diminutives.csv")
    fem_df = load_data(fem_path, to_lower = True)

    def to_lower(x):
            if isinstance(x, str):
                return x.lower()
    himss_nicknames = himss_nicknames.apply(lambda col: col.map(to_lower))

    ## GENERATE METAPHONE GROUPS
    name_graph = generate_name_graph(test_df, [fem_df, himss_nicknames])
    G_metaphone = generate_metaphone_graph(name_graph)
    connected_components = list(nx.connected_components(G_metaphone))

    index_of_longest = max(range(len(connected_components)), key=lambda i: 
                           len(connected_components[i]))

    component_to_remove = connected_components[index_of_longest]
    G_metaphone.remove_nodes_from(component_to_remove)

    #new_nodes = [ "MFS", "NNS", "ALF", "ATR", "MTN", "P", "PL", "TNLS",
                 #"PFRL", "ATKR", "ALM"
    #]
    #G_metaphone.add_nodes_from(new_nodes)

    kim = ["KM", 'KMPRL']
    alex = ["LKS", "ALKS", "ALKSNTR", "ALKSS"]
    marg = ["MJ", "PK", "MRKRT", "MRK", "MK", "MKN","MLTRT","ML","MR", "MLSNT", 
            "AML", "MRJ", "MRN"]
    jacky = ["AK", "AKLN", 'AKT']
    h_name = ["HT", "HNRT", "HRT", "HL", "HLSN", "HSTR", "AS0R", "ASTR"]
    kass = ["KS", "KSNTR"]
    kat = ["K0RN", "KT","K0", "K0LN"]
    jess = ["JS", "JSK", "JSF","JSFN"]
    georgie = ["JRJN", "JRJ"]
    jenn = ["JNFR","JN","FRJN", "KNFR", "KN", "KNSTNS", "RJN", "JNT", "ANLN"]
    elizabeth = ["ALSP0", "ALS", "LS", "P0N" , "P0", "ALNR", "AL", "PS",
                 "PTS", "LSNT", "L", "PTRS", "TRKS","PTRKS","PT", "MLS", "HLN",
                 "MLS", "ALSN", "TRX", "LP", "TRS", "0RS"]
    addie = ["ATL", "ATLT","AT", "ATLN"]
    sandy = ["SNT", "SN0", "SNTR"]
    izzy = ["ASPL", "AS"]
    judy = ["JT0", "JT"]
    charlotte = ["XRLT", "LT"]
    lily = ["LL", "LLN"]
    laura = ["LR", "LRN", "LRL"]
    rebecca = ["RPK", "PK"]
    sue = ["S", "SSN", "SS", "SST", "SN"]

    groups = [kim, alex, marg, jacky, h_name, kass, kat,jess, georgie, jenn,
              elizabeth, addie, sandy, izzy, judy, charlotte, lily, laura,
              rebecca, sue]
    for group in groups:
        group_edges = list(itertools.combinations(group, 2))
        G_metaphone.add_nodes_from(group)
        G_metaphone.add_edges_from(group_edges)

    combined_list = {element for sublist in groups for element in sublist}
    remainder = component_to_remove - combined_list #s- set(new_nodes)
    #remainder_edges = list(itertools.combinations(remainder, 2))
    G_metaphone.add_nodes_from(remainder)

    # add pairs
    bev = ["PF", "PFRL"]
    marsha = ["MRS", "MRX"]
    rose = ["RSMR", "RS", "RSLN", "RSLNT"]
    pat = ["PT", "PTRS"]
    pam = ["PM", "PMLA", "PML"]
    sonya = ["SN", "SNJ"]
    groups = [bev, marsha, rose, pat, pam, sonya]
    for group in groups:
        group_edges = list(itertools.combinations(group, 2))
        G_metaphone.add_edges_from(group_edges)

    # Apply the function to the 'firstname' column and create a new column 'metaphone_code'
    test_df['metaphone_code'] = test_df['firstname'].apply(get_metaphone)

    connected_components = list(nx.connected_components(G_metaphone))
    metaphone_to_component = {}
    for component_id, component in enumerate(connected_components):
        for metaphone_code in component:
            metaphone_to_component[metaphone_code] = component_id

    # Map the component ID to the DataFrame
    test_df['component_id'] = test_df['metaphone_code'].map(metaphone_to_component)
    return(G_metaphone, test_df)

def generate_male_blocks(merged_df, merged_himss, himss_nicknames, user_path):
    test_df = merged_df[(merged_df['gender'] == "M") | (pd.isna(merged_df['gender']))]

    # get final data frame of F/NA gender from the himss cases 
    himss_male_names = merged_himss[(merged_himss['gender'] == "M") | \
                                (pd.isna(merged_himss['gender']))]

    # GET DATA FOR LINKING
    # female names dictionary
    male_path = os.path.join(user_path, "supplemental/male_diminutives.csv")
    male_df = load_data(male_path, to_lower = True)


    carlton_path = os.path.join(user_path, 
                                "supplemental/carltonnorthernnames.csv")
    carlton = load_data(carlton_path)

    test_graph = generate_name_graph(himss_male_names, 
                                     [male_df, himss_nicknames, carlton])

    G_metaphone = generate_metaphone_graph(test_graph)
    connected_components = list(nx.connected_components(G_metaphone))

    component_to_remove = connected_components[0]
    G_metaphone.remove_nodes_from(component_to_remove)

    new_nodes = [
        "TTLL", "NTN", "KFN", "KNS", "FLSN", "JMP", "JRM", "KL",
        "AJN", "KLFN", "KR0", "KLPRT", "KLP", "MLFN"
        #"KLT", "XSL", "RT", "MKS", "NTN", "KMPL", "TLPRT"
        #"MKS", "RLF", "MLFN", "ARK", "XRTN", "SLFN", "ALJ", "MMT",
        #"RSL", "ALRNT", "KLS", "LSL", "FNS", "TLTN", "AKNTS", "KRTN", 
        #"LSTR", "FKTR", "ARN", "FRTS", "ATRN", "PTR", "KLFRT", "MLKLM", "LF",
        #"XLTN"
    ]
    G_metaphone.add_nodes_from(new_nodes)

    ed = ["ATRT", "AT", "NT", "ATKR"]
    michael = ["MX","MXL"] # need to fix for michael
    jerry = ["JR","KRLT", "JRLT", "KRRT", "KR"]
    j_names = ["JK", "JN0N","JN", "JM", "JMS", "JKP", "JMSN", "JSN"]
    theo = ["0","0TR", "0T", "0TS"]
    nathan = ["N0N","N0NL"]
    fred = ["FRTRK", "FRT"]
    louis = ["LS","L"]
    doug = ["TKLS", "TK", "RXRT","RX", "RKRT","RK"] # and richard since tk has doug and dick
    joe = ['JSF', "J"]
    tom = ["TMS", 'TM']
    clay = ['KL','KLTN']
    andrew = ['ANTR', 'TR', 'TRNS', "ANTRSN"]
    steve = ["STFN", "STF"]
    randy = ["RNT", "RNTL", "RNTLF"]
    arthur = ["ART", "AR0R"]
    pat = ['PT', 'PTRK']
    h_names = ['HRLT','HR', 'HNR', 'HNK', 'HL']
    larry = ['LR', 'LRN', 'LRNS']
    len = ["LN", "LNRT"]
    clem = ['KLM', 'KLMNT']
    tim = ["TM", "TM0"]
    roger = ["RJR", "RKR"]
    chris = ["KRSXN", "KRSTFR"]
    nick = ["NKLS", "NK"]
    ad = ['ATLFS', 'ATLF', "TLF"]
    cam = ["KM","KMRN"]
    marty = ["MRT","MRTN"]

    # robert and william will have to leave

    groups = [ed, michael, jerry, j_names, theo, nathan, fred, louis, doug, joe, \
            tom, clay, andrew, steve, randy, arthur, pat, h_names, larry, \
            len, clem, roger, chris, nick, ad, cam, marty, tim]
    for group in groups:
        group_edges = list(itertools.combinations(group, 2))
        G_metaphone.add_nodes_from(group)
        G_metaphone.add_edges_from(group_edges)


    combined_list = {element for sublist in groups for element in sublist}
    remainder = component_to_remove - combined_list - set(new_nodes)
    remainder_edges = list(itertools.combinations(remainder, 2))
    G_metaphone.add_nodes_from(remainder)
    G_metaphone.add_edges_from(remainder_edges)

    remainder_list = list(remainder)

    # Apply the function to the 'firstname' column and create a new column 'metaphone_code'
    test_df['metaphone_code'] = test_df['firstname'].apply(get_metaphone)

    G = G_metaphone

    connected_components = list(nx.connected_components(G))

    #index_of_longest = max(range(len(connected_components)), key=lambda i: 
                            #len(connected_components[i]))

    component_to_remove = connected_components[0]
    G.remove_nodes_from(component_to_remove)

    # Initialize a dictionary to group elements by the first letter
    grouped_elements = {}

        # Group elements by the first letter
    for element in component_to_remove:
        first_letter = element[0].lower()  # Consider case insensitivity by converting to lowercase
        if first_letter not in grouped_elements:
            grouped_elements[first_letter] = []
        grouped_elements[first_letter].append(element)

    groups = list(grouped_elements.values())
    for group in groups:
        group_edges = list(itertools.combinations(group, 2))
        G.add_nodes_from(group)
        G.add_edges_from(group_edges)

    connected_components = list(nx.connected_components(G))

    metaphone_to_component = {}
    for component_id, component in enumerate(connected_components):
        for metaphone_code in component:
            metaphone_to_component[metaphone_code] = component_id

    # Map the component ID to the DataFrame
    test_df['component_id'] = test_df['metaphone_code'].map(metaphone_to_component)

    return G, test_df

def generate_last_blocks(merged_df, himss, data_dir, 
                         threshold = 0.9, gender = "M"):
    if gender == "M":
        test_df = merged_df[(merged_df['gender'] == "M") | (pd.isna(merged_df['gender']))]
    else: 
        test_df = merged_df


    suffixes = r'\b(Jr|II|III|Sr|ii|iii|IV|iv|jr|sr)\b'

    # Apply the custom function to the 'lastname' column
    test_df['lastname'] = test_df['lastname'].str.replace(suffixes, '', regex=True) \
                                .str.replace(r"[,\.']", '', regex=True) \
                                .str.strip()
    test_df['lastname'] = test_df['lastname'] \
        .str.lower()
    test_df['last_metaphone'] = test_df['lastname'] \
        .apply(lambda name: doublemetaphone(name)[0])
    test_df['base_code'] = test_df['last_metaphone'] \
        .apply(lambda x: x[:-1] if x.endswith('S') else x)
    
    unique_lastnames = test_df[['lastname', 'last_metaphone']].drop_duplicates()

    unique_lastnames['last_metaphone2'] = \
        test_df['lastname'].apply(lambda name: doublemetaphone(name)[1])
    unique_lastnames['last_metaphone2'].replace('', np.nan, inplace=True)

    unique_codes = set(test_df['base_code'].unique())

    G = nx.Graph()
    G.add_nodes_from(unique_codes)
    for code in unique_codes:
        if len(code) > 6:
            # Find all other codes with a Levenshtein distance of 1
            for other_code in unique_codes:
                if code != other_code and len(other_code) > 3 and \
                    lev.distance(code, other_code) <= 2:
                        G.add_edge(code, other_code)
    if '' in G.nodes():
        G.remove_node('')

    connected_components = list(nx.connected_components(G))

    ## test 3 - incorporating jaro + frequencies
    last_frequencies = test_df.groupby('last_metaphone')['contact_uniqueid'].nunique().reset_index()
    edges_path = os.path.join(data_dir, "derived/auxiliary/jaro_output.csv")
    edges = pd.read_csv(edges_path)
    edges['name'] = edges['name'].fillna('')
    edges['similar_name'] = edges['similar_name'].fillna('')
    edges['metaphone1'] = edges['name'].apply(lambda name: doublemetaphone(name)[0])
    edges['metaphone2'] = edges['similar_name'].apply(lambda name: doublemetaphone(name)[0])
    filtered_edges = edges.loc[edges['metaphone1'] != edges['metaphone2']]

    last_freq_dict = last_frequencies.set_index('last_metaphone')['contact_uniqueid'].to_dict()

    cut_off = filtered_edges[filtered_edges['distance'] >= threshold]

    for _, row in cut_off.iterrows():
        name1 = row['metaphone1']
        name2 = row['metaphone2']
        if (name1 in G and name2 in G and 
            (last_freq_dict.get(name1, 0) <= 25 or 
            last_freq_dict.get(name2, 0) <= 25)): # modify with  distances here
            G.add_edge(name1, name2)

    connected_components = list(nx.connected_components(G))

    # temp 
    himss_lastnames = himss[['lastname']].drop_duplicates()
    himss_lastnames['metaphone'] = himss_lastnames['lastname'].apply(lambda x: doublemetaphone(x)[0])

    index_of_longest = max(range(len(connected_components)), key=lambda i: 
                           len(connected_components[i]))

    component_to_remove = connected_components[index_of_longest]
    G.remove_nodes_from(component_to_remove)

    # Initialize a dictionary to group elements by the first letter
    grouped_elements = {}

    # Group elements by the first letter
    for element in component_to_remove:
        first_letter = element[0].lower()  # Consider case insensitivity by converting to lowercase
        if first_letter not in grouped_elements:
            grouped_elements[first_letter] = []
        grouped_elements[first_letter].append(element)

    groups = list(grouped_elements.values())
    for group in groups:
        group_edges = list(itertools.combinations(group, 2))
        G.add_nodes_from(group)
        G.add_edges_from(group_edges)

    connected_components = list(nx.connected_components(G))

    metaphone_to_component = {}
    for component_id, component in enumerate(connected_components):
        for metaphone_code in component:
            metaphone_to_component[metaphone_code] = component_id

# Assuming test_df is defined and has a 'last_metaphone' column
    test_df['component_id'] = test_df['last_metaphone'].map(metaphone_to_component)
    test_df['component_id'] = test_df['component_id'].fillna(test_df['base_code'].map(metaphone_to_component))

    return G, test_df

def graph_blocking_results(df):
    component_counts = df.groupby('contact_uniqueid')['component_id'].nunique()

    # Plot the histogram of distinct component IDs per contact_uniqueid
    bins = [0, 1, 2, 3, 4, 5]
    labels = ['[0,1]', '(1,2]', '(2,3]', '(3,4]', '(4,5]']
    binned_counts = pd.cut(component_counts, bins=bins, labels=labels, right=True, include_lowest=True)
    binned_counts = binned_counts.value_counts().sort_index()

    plt.figure(figsize=(10, 6))
    ax = sns.barplot(x=binned_counts.index, y=binned_counts.values)
    plt.xlabel('Bins of Number of Distinct Component IDs')
    plt.ylabel('Frequency')
    plt.title('Histogram of Distinct Component IDs per Contact UniqueID')
    plt.grid(True)
    for p in ax.patches:
        ax.annotate(f'{int(p.get_height())}', (p.get_x() + p.get_width() / 2., p.get_height()), 
                    ha='center', va='center', xytext=(0, 10), textcoords='offset points')

    plt.show()

    print(df['component_id'].value_counts())


