# Importing packages
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from bs4 import NavigableString

import logging

# Creating log file
logging.basicConfig(
    filename=r'C:\Users\aggarw13\Dropbox\Hospital Exec\pre2010log.log', #Change path
    filemode='w',
    level=logging.INFO,
)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

# Creating functions

#Scrapes URL, returns error and status code if there ae problems
def get_soup(url):
  response = requests.get(url, headers=headers)
  if response.status_code > 299:
    print(f'Something went wrong when fetching {url}. Status code: {response.status_code}.')
    logging.error(f'Something went wrong when fetching {url}. Status code: {response.status_code}.')
    return "error" + ' ' + str(response.status_code)
  return BeautifulSoup(response.text, 'html.parser')
# Function for grabbing the title of article. If site is in .txt and not .html format, then it checks each line
# and grabs the title by checking for uppercase.
def get_title(soup):
    title_tag = soup.find('h1', class_='prtitle')
    if title_tag:
        title_text = title_tag.get_text(strip=True)
    elif soup.title:
        title_text = soup.title.get_text(strip=True)
    else:
        titlesegment = []
        soup_text = soup.get_text()
        souplines = [line.strip() for line in soup_text.splitlines() if line.strip()]
        for line in souplines:
            if line.isupper() and len(line.split()) > 2:
                titlesegment.append(line)
        titlesegment = titlesegment[3:] # Drops the first 3 lines since this is typically extra info before the title.
        title_text = ' '.join(titlesegment)
    return title_text
# Function for grabbing the date of an article.
def get_date(soup):
    date_text = ''
    date_tag = soup.find("title")
    if date_tag:
        date_tag = date_tag.get_text(strip=True)
        date_tag = re.findall(r'\d{1,4}[-/]\d{1,2}[-/]\d{1,4}', date_tag) # Checks for date form and extracts the date.
        if len(date_tag) > 0:
            date_text = date_tag[0]
    if date_text == '': # This is run if no date was extracted
        souptext = soup.get_text(strip=True)
        date_regex = r'\b(?:MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY),\s+[A-Z]+\s+\d{1,2},\s+\d{4}\b' #Checks if date in format Monday, Month, day, year
        match = re.search(date_regex, souptext)
        if match:
            date_text = match.group() # Saves date_text as match of date.
    return date_text
def get_fulltext(soup):
    paragraphs = soup.find_all('p')
    full_text = ' '.join(p.get_text(separator=' ') for p in paragraphs)
    full_text = re.sub(r'\s+', ' ', full_text).replace('\xa0', ' ').strip() #Standarizes whitespaces and removes \xa0 segments
    full_text = full_text.encode('ascii', 'ignore').decode('ascii') # Removes all non ascii characters.
    if len(full_text) < 20: # If full text wasn't successfully extracted - too small
        text_pieces = []
        text_tag = soup.find("h2", class_="prtitle")
        if text_tag:
            for element in text_tag.next_siblings: #Looping over elements at the same level as in text_tag.
                if isinstance(element, NavigableString): #Checks whether an element is a text_node
                    text_part = element.strip()
                    if text_part:
                        text_pieces.append(text_part) #appends text to the text_pieces list
                elif element.name in ['p']: # Stops iterating once a <p> tag is reached
                    break
            full_text = ' '.join(text_piece for text_piece in text_pieces) #Joins text_pieces together
        else: #If text_tag does not exist, then checks for 'pre' tags.
            pre_tag = soup.find('pre')
            if pre_tag is not None:
                full_text = pre_tag.get_text(strip=True)
        full_text = re.sub(r'\s+', ' ', full_text).replace('\xa0', ' ').strip()  # Standarizes whitespaces and removes \xa0 segments
        full_text = full_text.replace('—', ' ').replace('–', ' ')  # Converts em and en dashes to spaces.
        full_text = full_text.encode('ascii', 'ignore').decode('ascii')  # Removes all non ascii characters.
    if len(full_text) < 20: #Futher checks if full text was extracted - if not, assumes that we have a text output and not html.
        full_text = soup.get_text(strip=True)
        full_text = re.sub(r'\s+', ' ', full_text).replace('\xa0', ' ').strip()  # Standarizes whitespaces and removes \xa0 segments
        full_text = full_text.replace('—', ' ').replace('–', ' ')  # Converts em and en dashes to spaces.
        full_text = full_text.encode('ascii', 'ignore').decode('ascii')  # Removes all non ascii characters.
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
errors = []

# This loops through the first 30 monthly links, where each monthly link connects to a page listing articles for a specific month.
# In each iteration of the loop, each article associated with the given monthly linked is scraped, and the URL, text,
# title and date are recorded.
for el in month_link_date:
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
            elif link_tag not in ["/", "/archive/justice-news-archive.html", "#top"]:
                url = baseurl + link_segment + '/' + link_tag
                article_links.append(url)
    for article in article_links:
        article_soup = get_soup(article)
        if isinstance(article_soup, str):
            if article_soup.startswith("error"): # If there was an error extracting url, then indicates so, and appends data to errors dictionary.
                doj_archive.append({"URL": article,
                                    "Title": article_soup,
                                    "Date": article_soup,
                                    "Full Text": article_soup})
                errors.append({"URL": article,
                               "Error": article_soup})
        else:
            print(f"Fetching {article}")
            full_text = get_fulltext(article_soup)
            date = get_date(article_soup)
            title = get_title(article_soup)
            doj_archive.append({"URL": article,
                                "Title": title,
                                "Date": date,
                                "Full Text": full_text})


filename = r"C:\Users\aggarw13\Dropbox\Hospital Exec\hospital_ceos\_data\scrape_output\dojarchivepre2010.csv" # Saves to a specific file. Note change path to your personal folders.
errorfile = r"C:\Users\aggarw13\Dropbox\Hospital Exec\dojerrorspre2010.csv" #Note, change path.
df = pd.DataFrame(doj_archive)
df_error = pd.DataFrame(errors) # Creates errors dataframe

# Exports info to csv file.
if not df.empty:
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
#Exports info to csv file for errors.
if not df_error.empty:
    df_error.to_csv(errorfile, index=False)
    print(f"Data saved to {errorfile}")






