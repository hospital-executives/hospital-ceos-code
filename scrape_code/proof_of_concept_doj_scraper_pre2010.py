# Importing packages
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup

# Creating functions

#Scrapes URL
def get_soup(url):
  response = requests.get(url)
  if response.status_code > 299:
    print(f'Something went wrong when fetching {url}. Status code: {response.status_code}.')
  return BeautifulSoup(response.text, 'html.parser')
# Function for grabbing the title of article
def get_title(soup):
    title_text = ''
    title_tag = soup.find('h1', class_='prtitle')
    if title_tag:
        title_text = title_tag.get_text(strip=True)
    return title_text
# Function for grabbing the date of an article.
def get_date(soup):
    date_text = ''
    date_tag = soup.find("title")
    if date_tag:
        date_text = date_tag.get_text(strip=True)
        date_text = re.findall(r'\d{1,4}[-/]\d{1,2}[-/]\d{1,4}', date_text) # Checks for date form and extracts the date.
        if date_text:
            date_text = date_text[0]
    return date_text
def get_fulltext(soup):
    paragraphs = soup.find_all('p')
    full_text = ' '.join(p.get_text(separator=' ') for p in paragraphs)
    full_text = re.sub(r'\s+', ' ', full_text).replace('\xa0', ' ').strip() #Standarizes whitespaces and removes \xa0 segments
    full_text = full_text.encode('ascii', 'ignore').decode('ascii') # Removes all non ascii characters.
    return full_text



urloriginal = 'https://www.justice.gov/archives/justice-news-archive'
soup = get_soup(urloriginal)
month_link_date = []

# Creating a list that contains links - each link is a page which lists articles for a particular month.
monthly_elements = soup.find_all('td', class_= 'text-align-center')
for element in monthly_elements:
    monthlink = element.find('a', href=True)
    if monthlink:
        monthlink = monthlink['href']
        month_link_date.append(monthlink)

baseurl = 'https://www.justice.gov'
doj_archive = []

# This loops through the first 30 monthly links, where each monthly link connects to a page listing articles for a specific month.
# In each iteration of the loop, each article associated with the given monthly linked is scraped, and the URL, text,
# title and date are recorded.
for el in month_link_date[:30]:
    article_links = []
    soup = get_soup(el)
    link_segment = el
    link_segment = link_segment.replace('index-archive.html', '')
    link_segment = link_segment.replace(baseurl, '')
    for link_tag in soup.find_all('a', href=True):
        if link_tag:
            link_tag = link_tag['href']
            if link_tag.startswith(link_segment):
                url = baseurl + link_tag
                article_links.append(url)
    for article in article_links:
        article_soup = get_soup(article)
        print(f"Fetching {article}")
        full_text = get_fulltext(article_soup)
        date = get_date(article_soup)
        title = get_title(article_soup)
        doj_archive.append({"URL": article,
                            "Title": title,
                            "Date": date,
                            "Full Text": full_text})

filename = r"C:\Users\aggarw13\Dropbox\hospital_ceos\_data\scrape_output\dojarchivepre2010.csv" # Saves to a specific file. Note change path to your personal folders.
df = pd.DataFrame(doj_archive)

# Exports info to csv file.
if not df.empty:
  df.to_csv(filename, index=False)
  print(f"Data saved to {filename}")






