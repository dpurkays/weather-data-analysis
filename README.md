# CMPT353 Summer2023 Final Project

#### AWSOMEGROUP101

## Required libraries

1. pandas
2. pyspark
3. scipy
4. matplotlib
5. seaborn
6. numpy
7. sys
8. sklearn

## Order of execution

### 1. Cleaning the Data (Optional)

- All of commands below in this section is optional. If you download the datasets from GoogleDrive we posted then you can run these python files.
- python3 get_wiki_cities.py
  > To see the patterns of weather in popular destinations for tourists, we created get_wiki_cities.py to obtain a list of top 65 popular destinations around the world
  >
  > > This will create top_65_cities.csv
- python3 get_popular_cities_data.py ghcn_tmax ghcn_tmin ghcn_prcp popular_cities_out
  > Used to collect the weather data of 40 popular cities in the ranking
  >
  > > this will produce a directory called populr_cities_out which has json file with cleaned data
- python3 get_unpopular_cities_data.py ghcn_tmax ghcn_tmin ghcn_prcp popular_cities_out
  > Used to collect the weather data of 40 unpopular cities in the ranking
  >
  > > this will produce a directory called unpopulr_cities_out which has json file with cleaned data
- python3 get_random_cities_data.py ghcn_tmax ghcn_tmin ghcn_prcp popular_cities_out
  > Used to obtain test data
  >
  > > this will produce a directory called random_cities_out which has json file with cleaned data
- Here is the link to get datasets we used for this project
  > https://drive.google.com/drive/u/0/folders/14xJ0ovBUhjv7vwWhb4bW8CSHxaiuyYZ-

### 2. Running the Analysis

- python3 stats.py popular_cities_out unpopular_cities_out random_cities_out
  > Used to see means and standard diviations of popular, unpopular, and random cities
- python3 infer_stats.py popular_cities_out unpopular_cities_out random_cities_out
  > Used to do all the statistical tests After you run this code, this file will create several images of how our data looks like. They will be in the "figures" folder
- python3 modeling.py popular_cities_out unpopular_cities_out random_cities_out
  > Used to train the data using some models
