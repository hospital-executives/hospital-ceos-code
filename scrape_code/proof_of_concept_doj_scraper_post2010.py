#Importing packages
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup

import logging

# Creating log file
logging.basicConfig(
    filename=r'C:\Users\aggarw13\Dropbox\Hospital Exec\post2010log.log', #Change path
    filemode='w',
    level=logging.INFO,
)
#Mimicking browser

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}


# Creating functions

#Scrapes a website
def get_soup(url):
  response = requests.get(url, headers=headers)
  if response.status_code > 299:
    print(f'Something went wrong when fetching {url}. Status code: {response.status_code}.')
    logging.error(f'Something went wrong when fetching {url}. Status code: {response.status_code}.')
    return "error" + ' ' + str(response.status_code)
  return BeautifulSoup(response.text, 'html.parser')


#Function for grabbing the date of an article.
def get_date(soup):
  date_tag = soup.find('div', class_='field-formatter--datetime-default field_date')
  if date_tag:
    date_text = date_tag.get_text(strip=True)
    return date_text
  return None

# Function for grabbing the text of an article.
def get_fulltext(soup):
  fulltext_div = soup.find('div', class_='field-formatter--text-default field-text-format--wysiwyg text-formatted field_body')
  if fulltext_div:
    fulltext_text = fulltext_div.get_text(separator=' ', strip=True)
    fulltext_text = re.sub(r'\s+', ' ', fulltext_text).replace('\xa0', ' ').strip() #Standarizes whitespaces and removes \xa0 segments
    fulltext_text = fulltext_text.replace('—', ' ').replace('–', ' ')  # Converts em and en dashes to spaces.
    fulltext_text = fulltext_text.encode('ascii', 'ignore').decode('ascii')
    return fulltext_text
  return None

#Function for grabbing the title of an article.
def get_title(soup):
  title_tag = soup.find('span', class_='field-formatter--string')
  if title_tag:
    title_text = title_tag.get_text(strip=True)
    title_text = title_text.encode('ascii', 'ignore').decode('ascii')
    return title_text
  return None

#Function for grabbing the topic 'tag' of an article.
def get_topic(soup):
  topic_section = soup.find('div', class_='field-formatter--entity-reference-label field__items')
  topics = ""
  if topic_section:
    topic_tags = topic_section.find_all('div', class_='field__item')
    topics_list = [topic_tag.get_text(separator=' ', strip=True) for topic_tag in topic_tags]
    topics = ', '.join(topics_list)
  return topics

#Function for grabbing the Attorney General Office's region.
def get_region(soup):
  region = ""
  tags = soup.find_all('div', class_='field__item')
  tagtext = [tag.get_text(separator=' ', strip=True) for tag in tags]
  for item in tagtext:
    if item.startswith("USAO"):
      region = item
  return region

urloriginal = 'https://www.justice.gov/archives/press-releases-archive'
url = urloriginal
pagenum = 0 # indicates the page number of the DOJ website.
doj_archive = []
errors = []

# The outer loop iteratively loops over pages of the DOJ website, and saves links to press releases from each page of the archives.
# The inner loop loops over the link to press releases associated with a page of the DOJ website, and saves their URL,
# text, title, date, topic 'tags', and Attorney General office's region.

while pagenum < 1860:

  print(f"Fetching page number {pagenum}")
  logging.info(f'Fetching page number {pagenum}')
  soup = get_soup(url)
  h2s = soup.find_all('h2')
  link_data = []

  for el in h2s:
    if (el.get('class') == ['news-title']):
      link_data.append("https://www.justice.gov" + el.find_all('a', href=True)[0].get('href'))
  for link in link_data:
    print(f"Fetching {link}")
    linksoup = get_soup(link)
    if isinstance(linksoup, str): # Checks if errors occurred while scraping URL.
      if linksoup.startswith("error"):
        doj_archive.append({"URL": link,
                            "Title": linksoup,
                            "Date": linksoup,
                            "Topic": linksoup,
                            "Agency": linksoup,
                            "Full Text": linksoup})
        errors.append({"URL": link,
                       "Error": linksoup})
    else:
      full_text = get_fulltext(linksoup)
      date = get_date(linksoup)
      title = get_title(linksoup)
      topic = get_topic(linksoup)
      region = get_region(linksoup)
      doj_archive.append({"URL": link,
                          "Title": title,
                          "Date": date,
                          "Topic": topic,
                          "Agency": region,
                          "Full Text": full_text})
  pagenum += 1
  url = urloriginal + "?page=" + str(pagenum)

filename = r"C:\Users\aggarw13\Dropbox\Hospital Exec\hospital_ceos\_data\scrape_output\dojarchivepost2010.csv" # Saves to a specific file. Note change path to your personal folders.
errorfile = r"C:\Users\aggarw13\Dropbox\Hospital Exec\dojerrorspost2010.csv" #Note, change path.
df = pd.DataFrame(doj_archive)
df_error = pd.DataFrame(errors) # Creates errors dataframe

# Exports info to csv file
if not df.empty:
  df.to_csv(filename, index=False)
  print(f"Data saved to {filename}")
#Exports info to csv file for errors.
if not df_error.empty:
    df_error.to_csv(errorfile, index=False)
    print(f"Data saved to {errorfile}")








