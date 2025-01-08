# Code by Greg Baker to extract GHCN data from cluster
# https://coursys.sfu.ca/2023su-cmpt-353-d1/pages/WeatherData

import sys
from pyspark.sql import SparkSession, functions, types, Row

spark = SparkSession.builder.appName('GHCN extracter').getOrCreate()

ghcn_path = '/courses/datasets/ghcn-splits'
ghcn_stations = '/courses/datasets/ghcn-more/ghcnd-stations.txt'
output = 'ghcn-subset'


observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    # becomes a types.DateType in the output
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
])


station_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('latitude', types.FloatType(), False),
    types.StructField('longitude', types.FloatType(), False),
    types.StructField('elevation', types.FloatType(), False),
    types.StructField('name', types.StringType(), False),
])


def station_data(line):
    return [line[0:11].strip(), float(line[12:20]), float(line[21:30]), float(line[31:37]), line[41:71].strip()]


def main():
    sc = spark.sparkContext

    # Stations data...
    stations_rdd = sc.textFile(ghcn_stations).map(station_data)
    stations = spark.createDataFrame(
        stations_rdd, schema=station_schema).hint('broadcast')

    # Observations data...
    obs = spark.read.csv(ghcn_path, header=None, schema=observation_schema)
    
    # Filter as we like...
    # keep only some years: still a string comparison here
    obs = obs.filter((obs['date'] >= '2015') & (obs['date'] <= '2019'))

    obs = obs.filter(functions.isnull(obs['qflag']))
    obs = obs.drop(obs['mflag']).drop(obs['qflag']).drop(
        obs['sflag']).drop(obs['obstime'])
    obs = obs.filter(obs['observation'].isin('TMAX','TMIN','PRCP'))

    # parse the date string into a real date object
    obs = obs.withColumn('newdate', functions.to_date(obs['date'], 'yyyyMMdd'))
    obs = obs.drop('date').withColumnRenamed('newdate', 'date')

    # optional, if you want the station data joined...
    obs = obs.join(stations, on='station')
    
    # To get TMAX, TMIN & PRCP
    # obs = obs.groupBy('station', 'date', 'latitude', 'longitude', 'elevation', 'name').pivot('observation').agg(functions.first('value'))

    # print(obs.show())

    obs.write.json(output + '/ghcn', mode='overwrite', compression='gzip')


main()
