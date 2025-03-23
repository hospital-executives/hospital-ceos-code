
# inputs
current_dir <- getwd()
source(file.path(current_dir, "config.R"))

args <- commandArgs(trailingOnly = TRUE)
confirmed_path <- args[1]
same_path <- args[2]
diff_path <- args[3]
confirmed_data <- read_csv(confirmed_path)

job_ranks <- tibble(
  title_standardized = c(
    "CEO:  Chief Executive Officer",
    "COO:  Chief Operating Officer",
    "CFO:  Chief Financial Officer",
    "CIO:  Chief Information Officer",
    "Chief Medical Officer",
    "Chief Nursing Head",
    "Chief Compliance Officer",
    "Chief Medical Information Officer",
    "CNIS:  Chief Nursing Informatics Officer",
    "Marketing Head",
    "Business Office Head",
    "Patient Safety Head",
    "CIO Reports to",
    "IT Director",
    "Director of Technology",
    "CSIO/IT Security Officer",
    "Medical Staff Chief",
    "Pathology Chief",
    "Head of Facility",
    "HIM Director",
    "ER Director",
    "Chief Experience/Patient Engagement Officer",
    "Clinical Systems Director",
    "Radiology Med Dir",
    "Laboratory Director",
    "Radiology Adm Dir",
    "Hospital Operations Head",
    "HR Head",
    "Ambulatory Care Head",
    "Cardiology Head",
    "OB Head",
    "Pharmacy Head",
    "Purchasing Head",
    "Patient Accounting/Revenue Cycle Head",
    "Patient Care Coordinator",
    "OR Head",
    "Quality Head",
    "Admissions Head",
    "Managed Care Head",
    "Cath-Lab Manager",
    "PACS Administrator",
    "Clinical/Biomedical Engineer",
    "Facility Management Head"
    # Add any other titles from the full list
  ),
  rank = c(
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43
    # Add corresponding ranks
  )
)

#probabilities
roles <- confirmed_data %>% 
  select(contact_uniqueid, year, title_standardized) %>% 
  left_join(job_ranks, by = "title_standardized") %>% 
  group_by(contact_uniqueid, year) %>%
  filter(rank == min(rank)) %>%
  ungroup() %>% 
  arrange(contact_uniqueid, year) %>% 
  group_by(contact_uniqueid) %>%
  mutate(previous_title_standardized = lag(title_standardized)) %>%
  ungroup() 

transitions <- roles %>% 
  filter(!is.na(previous_title_standardized)) %>% 
  #note the most common moves are to the same title, so dropping those (e.g. CEO to CEO)
  # Exclude transitions where the job title didn't change
  filter(previous_title_standardized != title_standardized) %>%
  count(previous_title_standardized, title_standardized) %>%
  arrange(desc(n)) %>% 
  group_by(previous_title_standardized) %>%
  mutate(probability = n / sum(n)) %>%
  ungroup()
#need to figure out how to handle mid year transitions, since that leads to some weirdness. ie someone is it director, then it directory AND CIO, to CIO. But this leads to the transition of CIO TO IT director showing as common. need to come up with a separate hierarchy (use Ambar's) to take the max (most senior) role in a given year


# Step 1: Modify the data to create all pairs of jobs held in the same year
job_pairs <- confirmed_data %>%
  select(contact_uniqueid, year, title_standardized) %>%
  #left_join(job_ranks, by = "title_standardized") %>%
  group_by(contact_uniqueid, year) %>%
  # Remove CIO Reports To, it shows hierarchy but in itself isn't a job
  filter(title_standardized != "CIO Reports to") %>% 
  # Only generate pairs if there are at least 2 jobs in the same year
  filter(n() > 1) %>%
  arrange(desc(title_standardized)) %>% 
  summarise(job_title_1 = combn(title_standardized, 2, FUN = function(x) x[1]),
            job_title_2 = combn(title_standardized, 2, FUN = function(x) x[2])) %>%
  ungroup() %>%
  filter(job_title_1 != job_title_2)  # Exclude pairs where both titles are the same

# Step 2: Count the occurrences of each pair of jobs and unique contact IDs
job_pair_counts <- job_pairs %>%
  group_by(job_title_1, job_title_2) %>%
  summarise(
    n = n(),  # Count total occurrences of the job pair
    unique_contacts = n_distinct(contact_uniqueid)  # Count unique contact IDs
  ) %>%
  arrange(desc(n))

# # Step 3: Calculate the probability of holding each pair of jobs
job_pair_probabilities <- job_pair_counts %>%
  mutate(
    total_contacts = sum(unique_contacts),  # Sum of all unique contacts for the pairs
    probability_contact = unique_contacts / total_contacts,
    probability_total = n/sum(n)# Probability based on unique contacts
  )

write.csv(transitions, diff_path, row.names = FALSE)
write.csv(job_pair_probabilities, same_path, row.names = FALSE)




