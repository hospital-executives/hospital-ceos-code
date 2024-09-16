import pandas as pd
import jellyfish
import time
import sys
import os
from metaphone import doublemetaphone

def main():
    # Expecting 3 arguments: code_dir, data_dir, and output
    if len(sys.argv) != 5:
        print("Usage: python jaro_algo.py <code_dir> <data_dir> <confirmed_r> <updated_gender> <output>")
        sys.exit(1)

    code_dir = sys.argv[1]
    confirmed_r = sys.argv[2]       # Path to CONFIRMED_R
    updated_gender = sys.argv[3]     # Path to UPDATED_GENDER
    output_file = sys.argv[4]

    # Add code_dir to sys.path to import blocking_helper
    try:
        import blocking_helper  # Now that code_dir is in the path, you can import the module
    except ImportError:
        print(f"Error: Could not import 'blocking_helper' from {code_dir}. Make sure it exists.")
        sys.exit(1)

    sys.path.append(code_dir)
    import blocking_helper 

    ## IMPORT AND CLEAN FILES

    # load gender df
    gender_df = pd.read_csv(updated_gender)
    gender_df.rename(columns={"Name": "firstname"}, inplace=True)
    gender_df['firstname'] = gender_df['firstname'].str.lower()

    # load confirmed df
    confirmed = pd.read_feather(confirmed_r)
    test_data = confirmed[['firstname', 'lastname', 'contact_uniqueid']]
    test_data['firstname'] = test_data['firstname'].str.lower()
    test_no_na = test_data.dropna(subset=['contact_uniqueid'])

    # get male cases only 
    merged_df = pd.merge(test_no_na, gender_df, on="firstname", how="left")
    test_df = merged_df[(merged_df['gender'] == "M") | (pd.isna(merged_df['gender']))]

    test_cleaned = test_df.dropna(subset=['contact_uniqueid', 'lastname'])
    suffixes = r'\b(Jr|II|III|Sr)\b'
    test_cleaned['lastname'] = test_cleaned['lastname'].str.replace(suffixes, '', regex=True) \
                                .str.replace(r"[,\.']", '', regex=True) \
                                .str.strip()
    test_cleaned['last_metaphone'] = test_cleaned['lastname'].apply(lambda name: doublemetaphone(name)[0])

    test_cleaned['base_code'] = test_cleaned['last_metaphone'].apply(lambda x: x[:-1] if x.endswith('S') else x)

    ## get frequencies
    frequency_df = confirmed.groupby('lastname')['contact_uniqueid'].nunique().reset_index()
    frequency_df['lastname'] = frequency_df['lastname'].str.lower()

    # Assume blocking_helper.generate_metaphone generates a dictionary and a map for metaphones
    # Here we create a simple placeholder for demonstration purposes
    def generate_metaphone(names_df):
        from metaphone import doublemetaphone
        metaphone_dict = {}
        name_to_metaphone = {}
        for name in names_df['lastname']:
            metaphone_code = doublemetaphone(name)[0]
            metaphone_dict[name] = metaphone_code
            name_to_metaphone[name] = metaphone_code
        return metaphone_dict, name_to_metaphone

    _, name_to_metaphone = generate_metaphone(frequency_df[['lastname']])
    infrequent_names = frequency_df[frequency_df['contact_uniqueid'] <= 10 ]['lastname']
    unique_lastnames = frequency_df[frequency_df['contact_uniqueid'] == 1 ]['lastname']

    # Optimize similar name finding function
    def find_similar_names(name, names_list, name_to_metaphone):
        return [
            (n, jellyfish.jaro_winkler_similarity(name, n))
            for n in names_list
            if name != n and
            (name[0] == n[0] or name[-1] == n[-1]) and
            name_to_metaphone.get(name) != name_to_metaphone.get(n) and
            jellyfish.jaro_winkler_similarity(name, n) > 0.85
        ]

    # Measure execution time
    start_time = time.time()

    # Create a list of unique lastnames for faster access
    unique_lastnames_list = unique_lastnames.unique().tolist()

    # Find all names that are within a Jaro-Winkler similarity threshold from the infrequent names
    similar_names_dict = {
        name: find_similar_names(name, unique_lastnames_list, name_to_metaphone)
        for name in infrequent_names
    }

    # Convert the dictionary to a DataFrame for better visualization
    similar_names_records = [
        (k, v[0], v[1]) for k, vals in similar_names_dict.items() for v in vals
    ]
    similar_names_df = pd.DataFrame(similar_names_records, columns=['name', 'similar_name', 'distance'])

    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")

    # Save the results to a CSV file
    similar_names_df.to_csv(output_file, index=False)


if __name__ == '__main__':
    main()
