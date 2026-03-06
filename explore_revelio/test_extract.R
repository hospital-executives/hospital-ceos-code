library(RPostgres)
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='kpapen')

## determine data libraries available
res <- dbSendQuery(wrds, "select distinct table_schema
                   from information_schema.tables
                   where table_schema ='revelio'")
data <- dbFetch(res, n=-1)
dbClearResult(res)
data


# check ceos
res <- dbSendQuery(wrds, "select * 
                          from revelio.individual_positions
                          where country='United States' and
                          onet_code='11-1011.00'")
data <- dbFetch(res, n=1000)
dbClearResult(res)
data

# get doctors
res <- dbSendQuery(wrds, "select * from revelio.job_postings")
docs <- dbFetch(res, n=1000)
dbClearResult(res)
docs

# check postings
res <- dbSendQuery(wrds, "select * from revelio.company_mapping 
                   where naics_code='622110' and hq_country='United States'")
hosps <- dbFetch(res, n=50000)
dbClearResult(res)
hosps

load_himss <- read_feather('/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos/_data/derived/himss_entities_contacts_0517_v1.feather')
check_info <- load_himss %>% distinct(entity_uniqueid, type, phone, entity_phone,website, email, entity_email) %>%
  filter(type == "General Medical & Surgical") %>%
  mutate(url = sub("www.", "", website))

# match 400 on phone number
check_hosps <- hosps %>% mutate(entity_phone = sub("^1", "", gsub("[^0-9]", "", phone_number))) %>%
  filter(!is.na(entity_phone)) %>%
  left_join(check_info %>% filter(!is.na(entity_phone)), by = "entity_phone") %>%
  filter(!is.na(entity_uniqueid))

# mat
check_website <- hosps %>% filter(!is.na(url)) %>%
  left_join(check_info %>% filter(!is.na(url)), by = "url") %>%
  filter(!is.na(entity_uniqueid))

## check positions based on rcids
codes <- check_website %>% distinct(rcid) %>% pull(rcid)
codes_sql <- paste0("('", paste(codes, collapse = "','"), "')")

res <- dbSendQuery(wrds, paste0("select * 
                                  from revelio.individual_positions
                                  where country='United States'
                                 and rcid in ", codes_sql))
pos_data <- dbFetch(res, n=50000)
dbClearResult(res)
pos_data


### test boardex
isins <- check_website %>% distinct(isin) %>% pull(isin)
isins_sql <- paste0("('", paste(isins, collapse = "','"), "')")

query <- paste0(
  "select * from boardex_na.na_wrds_dir_profile_emp
   where hocountryname='United States'
   and sector='Health'"
)

res <- dbSendQuery(wrds, query)
execs <- dbFetch(res, n=50000)
dbClearResult(res)
execs
