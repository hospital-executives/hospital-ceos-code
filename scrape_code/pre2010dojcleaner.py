import pandas as pd

df = pd.read_csv(r'C:\Users\aggarw13\Dropbox\Hospital Exec\hospital_ceos\_data\scrape_output\dojarchivepre2010.csv') # Reading csv file, note change path.


missing_indices = df[df['Title'].isna() | (df['Title'].astype(str).str.strip() == '')].index # Getting row numbers for where there are missing titles

# List of missing titles
missing_titles = [
    """ATTORNEY GENERAL ASHCROFT TO HOLD NEWS CONFERENCE""",
    """TENTATIVE PUBLIC SCHEDULE FOR ATTORNEY GENERAL JANET RENO""",
    """ENVIRONMENT DIVISION HIGHLIGHTS ACCOMPLISHMENTS, INITIATIVES""",
    """GEORGIA CARPET MAKER SENTENCED FOR MISLABELING CARPETING""",
    """MEDIA ADVISORY""" ]

# iterates over rows with missing titles and adds titles to dataframe.

for i, title in zip(missing_indices, missing_titles):
    df.at[i, 'Title'] = title

missing_indices = df[df['Date'].isna() | (df['Date'].astype(str).str.strip() == '')].index # Getting row numbers for where there are missing indices.

# List of specific missing dates.

missing_dates = [
    """11-4-2002""",
    """WEDNESDAY, FEBRUARY 28, 2001""",
    """WEDNESDAY, FEBRUARY 14, 2001""",
    """FRIDAY, OCTOBER 12, 2001""",
    """THURSDAY, JANUARY 13, 2000""",
    """THURSDAY, JANUARY 13, 2000""",
    """THURSDAY, JANUARY 13, 2000""",
    """WEDNESDAY, JANUARY 12, 1999""",
    """MONDAY, JANUARY 10, 2000""",
    """MONDAY, JANUARY 10, 2000""",
    """FRIDAY, JANUARY 7, 2000""",
    """WEDNESDAY, MARCH 1, 2000""",
    """WEDNESDAY, SEPTEMBER 27, 2000""",
    """TUESDAY, SEPTEMBER 19, 2000""",
    """TUESDAY, MARCH 30, 1999""",
    """THURSDAY, MARCH 18, 1999""",
    """THURSDAY, APRIL 29, 1999""",
    """FRIDAY, APRIL 9, 1999""",
    """FRIDAY, MAY 7, 1999""",
    """THURSDAY, JUNE 24, 1999""",
    """WEDNESDAY, DECEMBER 1, 1999""",
    """WEDNESDAY, DECEMBER 1, 1999""",
    """TUESDAY, FEBRUARY 17, 1998""",
    """TUESDAY, JUNE 23, 1998""",
    """THURSDAY, JULY 16, 1998""",
    """THURSDAY, JULY 16, 1998""",
    """THURSDAY, JULY 16, 1998""",
    """WEDNESDAY, JULY 15, 1998""",
    """FRIDAY, AUGUST 14, 1998""",
    """THURSDAY, AUGUST 13, 1998""",
    """THURSDAY, AUGUST 13, 1998""",
    """MONDAY, AUGUST 10, 1998""",
    """THURSDAY, AUGUST 6, 1998""",
    """MONDAY, NOVEMBER 2, 1998""",
    """MARCH 14, 1996""",
    """April 30, 1996""",
    """TUESDAY, APRIL 2, 1996""",
    """FRIDAY, JANUARY, 27, 1995""",
    """JANUARY 6, 1995""",
    """JANUARY 5, 1995""",
    """JANUARY 4, 1995""",
    """FEBRUARY 17, 1995""",
    """FEBRUARY 14, 1995""",
    """FEBRUARY 2, 1995""",
    """APRIL 11, 1995""",
    """WEDNESDAY, MAY 10, 1995""",
    """MONDAY, MAY 1, 1995""",
    """JUNE 15, 1995""",
    """MONDAY, JULY 31, 1995""",
    """FRIDAY, JULY, 7, 1995""",
    """Monday, November 20, 1995""",
    """Saturday, December 2, 1995""",
    """September, 1994""",
    """SEPTEMBER 16, 1994""",
    """September, 1994""",
    """September, 1994""",
    """September, 1994""",
    """September, 1994""",
    """September, 1994""",
    """OCTOBER 27, 1994""",
    """NOVEMBER 30, 1994""",
    """NOVEMBER 17, 1994""",
    """NOVEMBER, 15, 1994""",
    """NOVEMBER 4, 1994""" ]

#Iterates over rows with missing dates and adds dates to dataframe.

for i, date in zip(missing_indices, missing_dates):
    df.at[i, 'Date'] = date


filename = r"C:\Users\aggarw13\Dropbox\Hospital Exec\hospital_ceos\_data\scrape_output\dojarchivepre2010cleaned.csv" #Path for saving csv, note change path.

if not df.empty:
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

