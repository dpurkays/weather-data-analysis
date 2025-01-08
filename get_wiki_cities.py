import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd

url = "https://en.wikipedia.org/wiki/List_of_cities_by_international_visitors"
response = requests.get(url)
if response.status_code != 200:
    print("Failed to fetch the webpage.")
    exit(1)
html_content = response.text

soup = BeautifulSoup(html_content, 'html.parser')

city_country_data = []
table = soup.find('table', class_='wikitable')
rows = table.find_all('tr')[1:]  # Skip the header row
for row in rows:
    cells = row.find_all('td')
    euromonitor_rank = cells[0].text.strip()
    # mastercard_rank = cells[1].text.strip()
    city_name = cells[2].text.strip()
    country_name = cells[3].text.strip()
    arrivals_2018 = cells[4].text.strip()
    city_country_data.append((euromonitor_rank, city_name, country_name, arrivals_2018))

top_cities = pd.DataFrame(city_country_data, columns=['Euromonitor-Rank', 'City', 'Country/Territory', 'Arrivals'])

top_cities.replace('', pd.NA, inplace=True)
top_cities = top_cities.dropna()


def extract_numeric(s):
    return ''.join(filter(str.isdigit, s))

# Remove non-numeric characters from the 'Arrivals' column
top_cities['Arrivals'] = top_cities['Arrivals'].apply(extract_numeric)

top_cities['Euromonitor-Rank'] = pd.to_numeric(top_cities['Euromonitor-Rank']).astype(int)
top_cities['Arrivals'] = pd.to_numeric(top_cities['Arrivals']).astype(int)

top_cities.sort_values(by='Euromonitor-Rank', ascending=True, inplace=True)

top_65_cities = top_cities[top_cities['Euromonitor-Rank'] <= 65]

top_65_cities.to_csv('top_65_cities.csv', index=False)
