#  Weather of Popular Destinations Analysis
A CMPT353 Summer 2023 Final Project\
AWSOMEGROUP101

## Summary 
We present an analysis of the weather between top tourist destinations and cities that did not make it on the coveted list. Our curiosity led us to explore whether there exists a trend in weather conditions among popular tourist destinations and how significantly they differ from their less traveled counterparts.

### Problem 
The project idea was originally to find anything that we could say about the weather in popular cities to travel. Although people may not choose a travel destination only based on the weather of the destination, we are interested in finding any relationships between weather and popular destinations. We can find cities that can potentially be a popular travel desitination based on the weather trend we find by analyzing weather data. By investigating weather data for both popular and unpopular tourist destinations, we aim to compare weather conditions between these two groups and discern whether there are any distinctive weather patterns that make certain destinations more appealing to travelers. Through data analysis, modeling, and visualization, we will delve deeper into the impact of weather on destination popularity and provide valuable insights for travelers, tourism stakeholders, and decision-makers.


Have a look at our [report](https://github.com/dpurkays/weather-data-analysis/blob/master/Report.pdf) for our analysis and findings.

## Required Python libraries

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

- All of the commands in this section are optional. If you download the datasets from Google Drive we posted, then you can run these Python files.
- python3 get_wiki_cities.py
  > To see the patterns of weather in popular destinations for tourists, we created get_wiki_cities.py to obtain a list of the top 65 popular destinations around the world
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
  > Used to see means and standard deviations of popular, unpopular, and random cities
- python3 infer_stats.py popular_cities_out unpopular_cities_out random_cities_out
  > Used to do all the statistical tests. After you run this code, this file will create several images of what our data looks like. They will be in the "figures" folder
- python3 modeling.py popular_cities_out unpopular_cities_out random_cities_out
  > Used to train the data using some models
