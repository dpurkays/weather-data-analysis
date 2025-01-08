import sys
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import col

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

def main(in_directory_1, in_directory_2, in_directory_3, out_directory): # , in_directory_2, in_directory_3, out_directory):
    # fileter tmax data
    tmax_data = spark.read.json(in_directory_1, schema=observation_schema)
    tmax_data = tmax_data.groupBy("name", "station").agg(functions.avg("value").alias("avg_tmax"))
    #tmax_data.show()

    # filter tmin data
    tmin_data = spark.read.json(in_directory_2, schema=observation_schema)
    tmin_data = tmin_data.groupBy("name", "station").agg(functions.avg("value").alias("avg_tmin"))
    #tmin_data.show()
    #print((tmin_data.count(), len(tmin_data.columns)))

    # filter prcp data
    prcp_data = spark.read.json(in_directory_3, schema=observation_schema)
    prcp_data = prcp_data.groupBy("name", "station").agg(functions.avg("value").alias("avg_prcp"))
    #prcp_data.show()

    # join tmax, tmin, prcp dataframes
    joined_data = tmax_data.join(tmin_data, on=["name", "station"])
    joined_data = joined_data.join(prcp_data, on=["name", "station"])
    #joined_data.show()
    #print((joined_data.count(), len(joined_data.columns)))

    sample_data = joined_data.sample(0.25)
    # sample_data.show(100)
    print((sample_data.count(), len(sample_data.columns)))

    # extract 25 unpopular cities 
    unpopular_cities_data = joined_data.filter(
        (col('station') == "LAM00048930") | \
        (col('station') == "USC00133509") | \
        (col('station') == "IN006012800") | \
        (col('station') == "CA007022800") | \
        (col('station') == "TUM00017150") | \
        (col('station') == "SWE00140864") | \
        (col('station') == "BGM00041950") | \
        (col('station') == "EIE00107808") | \
        (col('station') == "USW00013871") | \
        (col('station') == "USC00130203") | \
        (col('station') == "COM00080035") | \
        (col('station') == "SWE00138560") | \
        (col('station') == "USS0012M26S") | \
        (col('station') == "GME00125434") | \
        (col('station') == "CHM00056172") | \
        (col('station') == "SP000008181") | \
        (col('station') == "RSM00029328") | \
        (col('station') == "USS0005M07S") | \
        (col('station') == "AGM00060360") | \
        (col('station') == "TUM00017130") | \
        (col('station') == "IN012030700") | \
        (col('station') == "GME00122410") | \
        (col('station') == "ARM00087582") | \
        (col('station') == "USC00200106") | \
        (col('station') == "TX000038895") | \
        (col('station') == "USC00411911") | \
        (col('station') == "CA004012300") | \
        (col('station') == "GME00131998") | \
        (col('station') == "UKE00105875") | \
        (col('station') == "RPM00098328") | \
        (col('station') == "BLM00085175") | \
        (col('station') == "BL000085041") | \
        (col('station') == "IRM00040736") | \
        (col('station') == "PKM00041515") | \
        (col('station') == "TI000038734") | \
        (col('station') == "AGM00060452") | \
        (col('station') == "FIE00144542") | \
        (col('station') == "CA001100120") | \
        (col('station') == "ASN00041175") | \
        (col('station') == "UPM00033915") \
    )

    #unpopular_cities_data.show()

    unpopular_cities_data = unpopular_cities_data.select(
        unpopular_cities_data['name'],
        unpopular_cities_data['station'],
        (unpopular_cities_data['avg_tmax'] / 10).alias("avg_tmax"),
        (unpopular_cities_data['avg_tmin'] / 10).alias("avg_tmin"),
        unpopular_cities_data['avg_prcp']
    )
    # unpopular_cities_data.show(40)
    # print((unpopular_cities_data.count(), len(unpopular_cities_data.columns)))

    #unpopular_cities_data.write.json(out_directory, compression='gzip', mode='overwrite')
    unpopular_cities_data.write.json(out_directory, compression=None, mode='overwrite')


if __name__ == "__main__":
    # python3 get_unpopular_cities.py ghcn_tmax ghcn_tmin ghcn_prcp unpopular_cities_out/
    in_directory_1 = sys.argv[1] 
    in_directory_2 = sys.argv[2]
    in_directory_3 = sys.argv[3]
    out_directory = sys.argv[4]

    main(in_directory_1, in_directory_2, in_directory_3, out_directory) # , in_directory_2, in_directory_3, out_directory)


