
## let's see
# in old dta but not in curr new_py_cleaned
in_old_dta = set(old_dta['id']) - set(new_py_cleaned['id'])

in_old_dta_contact_ids = set(old_dta[old_dta['id'].isin(in_old_dta)]['contact_uniqueid'])

contact_ids = {str(int(x)) for x in in_old_dta_contact_ids}

remaining_comp = contact_ids & \
set(remaining5['contact_id1']).union(set(remaining5['contact_id2']))

remaining_meta = contact_ids & meta_remaining_ids

# potential rules - min same/min diff <= 0.05

remaining1 = comp_remaining[
    ~((comp_remaining['frequent_lastname_flag'] == 1 ) & 
    (comp_remaining['shared_states'].apply(len) == 0) & 
    ~(comp_remaining['name_in_same_row_firstname']) &
    (comp_remaining['firstname_jw_distance'] < 0.8 ) & 
    ~(comp_remaining['meta_in_same_row']))] # 127

remaining2 = remaining1[
    ~(~remaining1['shared_titles_flag'] & 
               ~(remaining1['shared_system_ids_flag'] == 1) &
               (remaining1['total_distance'] >= 500) & 
               (remaining1['diff_state_years_count'] > 1))
               ]

remaining3 = remaining2[~(
            (remaining2['min_diff_probability'].isna()) & 
           (remaining2['min_same_probability'].isna()) & 
           ~(remaining2['shared_titles_flag'] == 1))]

remaining4 = remaining3[~((
            (remaining3['min_diff_probability'] < .025) & 
           (remaining3['min_same_probability'] < .025) & 
           ~(remaining3['shared_titles_flag'] == 1)) & 
           ~(remaining3['unique_job_tuples']).apply(has_head))]

remaining5 = remaining4[
    ~(
        ((remaining4['min_diff_probability'] < 0.025) |  
         remaining4['min_diff_probability'].isna()) &  
        ((remaining4['min_same_probability'] < 0.025) |  
         remaining4['min_same_probability'].isna()) &  
        (~(remaining4['shared_titles_flag'] == 1)) &  
        ((remaining4['shared_states'].apply(len)) == 0) &  
        ~(remaining4['unique_job_tuples'].apply(has_head))
    )
]

remaining5[ ((remaining5['min_diff_probability'] < 0.05) |  
         remaining5['min_diff_probability'].isna()) &  
        ((remaining5['min_same_probability'] < 0.05) |  
         remaining5['min_same_probability'].isna()) &  
        (~remaining5['shared_titles_flag']) &  
        ((remaining5['shared_states'].apply(len)) == 0) & 
        (remaining5['amb_mismatch'])]


# maybe same
remaining3[(remaining3['lastname_jw_distance'] >= 0.9) & 
           ((remaining3['firstname_jw_distance'] >= 0.85) |
           (remaining3['name_in_same_row_firstname'])) & 
           (remaining3['shared_states'].apply(len) > 0) & 
           (remaining3['shared_titles_flag']) & 
            ~(remaining3['frequent_lastname_flag'])]

def clean_results_pt9(remaining_input,confirmed_graph,
                      dropped_comp, contact_count_dict,
                      new_himss):
    
    # add both male to df
    contact_gender_dict = new_himss.groupby('contact_uniqueid')\
    ['gender'].apply(set).to_dict()

    def both_male(contact1, contact2):
        return set(contact_gender_dict.get(contact1, [])) == {'M'} and \
            set(contact_gender_dict.get(contact2, [])) == {'M'}

    remaining_input['both_male'] = remaining_input.apply(lambda row:
                                        both_male(row['contact_id1'], 
                                                    row['contact_id2']), axis=1)

    ## add enitty type 
    contact_type_dict = new_himss.groupby('contact_uniqueid')\
        ['entity_type'].apply(set).to_dict()

    def type_mismatch(contact1, contact2):
        id1_types = contact_type_dict.get(contact1, [])
        id2_types = contact_type_dict.get(contact2, [])

        condition1 = any(t in ['Sub-Acute', 'Home Health'] for t in id1_types)
        condition2 = any(t in ['Sub-Acute', 'Home Health'] for t in id2_types)

        return condition1 ^ condition2


    remaining_input['type_mismatch'] = remaining_input.apply(lambda row:
                                        type_mismatch(row['contact_id1'], 
                                                    row['contact_id2']), axis=1)

    # new rules
    remaining1 = remaining_input[
        ~(remaining_input['both_male'] & 
        (remaining_input['lastname_lev_distance'] >= 3) & 
        ~((remaining_input['shared_system_ids_flag']) |
        (remaining_input['shared_entity_ids_flag']) |
        (remaining_input['shared_zips'].apply(len) > 0)))]  
    
    remaining2 = remaining1[~(
        (remaining1['firstname_jw_distance'] < .65) & 
        ~(remaining1['name_in_same_row_firstname']))]

    remaining3 = remaining2[~(
        remaining2['type_mismatch'] & 
        (remaining2['firstname_jw_distance'] < .9) & 
        ~(remaining2['name_in_same_row_firstname']))]

    remaining4 = remaining3[~(remaining3['type_mismatch'] & 
    (remaining3['total_distance'] > 500))]

    # dropped
    dropped1 = remaining_input[
        (remaining_input['both_male'] & 
        (remaining_input['lastname_lev_distance'] >= 3) & 
        ~((remaining_input['shared_system_ids_flag']) |
        (remaining_input['shared_entity_ids_flag']) |
        (remaining_input['shared_zips'].apply(len) > 0)))]  
    
    dropped2 = remaining1[(
        (remaining1['firstname_jw_distance'] < .65) & 
        ~(remaining1['name_in_same_row_firstname']))]

    dropped3 = remaining3[(
        remaining3['type_mismatch'] & 
        (remaining3['firstname_jw_distance'] < .9) & 
        ~(remaining3['name_in_same_row_firstname']))]

    dropped4 = remaining3[(remaining3['type_mismatch'] & 
    (remaining3['total_distance'] > 500))]

    dropped_comp.update(zip(dropped1['contact_id1'], dropped1['contact_id2']))
    dropped_comp.update(zip(dropped2['contact_id1'], dropped2['contact_id2']))
    dropped_comp.update(zip(dropped3['contact_id1'], dropped3['contact_id2']))
    dropped_comp.update(zip(dropped4['contact_id1'], dropped4['contact_id2']))

    update_confirmed_from_dropped(confirmed_graph, dropped_comp,
                                                        contact_count_dict)
    
    return remaining4, dropped_comp