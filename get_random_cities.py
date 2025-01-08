import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('GHCN extracter').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+

observation_schema = types.StructType([
    types.StructField('date', types.StringType()),
    types.StructField('elevation', types.DoubleType()),
    types.StructField('latitude', types.DoubleType()),
    types.StructField('longitude', types.DoubleType()),
    types.StructField('name', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('station', types.StringType()),
    types.StructField('value', types.IntegerType()),
])

def main(in_directory_1, in_directory_2, in_directory_3, out_directory):
    # fileter tmax data
    tmax_data = spark.read.json(in_directory_1, schema=observation_schema)
    tmax_data = tmax_data.groupBy("name", "station").agg(functions.avg("value").alias("avg_tmax"))

    # filter tmin data
    tmin_data = spark.read.json(in_directory_2, schema=observation_schema)
    tmin_data = tmin_data.groupBy("name", "station").agg(functions.avg("value").alias("avg_tmin"))
   
    # filter prcp data
    prcp_data = spark.read.json(in_directory_3, schema=observation_schema)
    prcp_data = prcp_data.groupBy("name", "station").agg(functions.avg("value").alias("avg_prcp"))

    # join tmax, tmin, prcp dataframes
    joined_data = tmax_data.join(tmin_data, on=["name", "station"])
    joined_data = joined_data.join(prcp_data, on=["name", "station"])
    
    # gets random data and take 20 of them
    random_cities_data = joined_data.sample(fraction=0.05, seed=42).limit(20)
    random_cities_data = random_cities_data.select(
        random_cities_data['name'],
        random_cities_data['station'],
        (random_cities_data['avg_tmax'] / 10).alias("avg_tmax"),
        (random_cities_data['avg_tmin'] / 10).alias("avg_tmin"),
        random_cities_data['avg_prcp']
    )

    random_cities_data.write.json(out_directory, compression=None, mode='overwrite')

if __name__ == "__main__":
    # python3 get_random_cities.py ghcn_tmax ghcn_tmin ghcn_prcp random_cities_out
    in_directory_1 = sys.argv[1] 
    in_directory_2 = sys.argv[2]
    in_directory_3 = sys.argv[3]
    out_directory = sys.argv[4]

    main(in_directory_1, in_directory_2, in_directory_3, out_directory) 

