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

def main(in_directory_1, in_directory_2, in_directory_3, out_directory):

    tmax_data = spark.read.json(in_directory_1, schema=observation_schema)
    tmax_data = tmax_data.withColumn('value', tmax_data['value'] / 10)
    tmax_data = tmax_data.groupBy("name", "station").agg(functions.avg("value").alias("avg_tmax"))

    tmin_data = spark.read.json(in_directory_2, schema=observation_schema)
    tmin_data = tmin_data.withColumn('value', tmin_data['value'] / 10)
    tmin_data = tmin_data.groupBy("name", "station").agg(functions.avg("value").alias("avg_tmin"))

    prcp_data = spark.read.json(in_directory_3, schema=observation_schema)
    prcp_data = prcp_data.groupBy("name", "station").agg(functions.avg("value").alias("avg_prcp"))

    weather_data = tmax_data.join(tmin_data, on=["name", "station"])
    weather_data = weather_data.join(prcp_data, on=["name", "station"])

    # weather_data.show()

    popular_cities_data = weather_data.filter(
        # Hong Kong NO DATA
        # Bankok
        (col('station') == "TH000048455") | \
        # London
        (col('station') == "UKM00003772") | \
        # Macau
        (col('station') == "MCM00045011") | \
        #Singapore
        (col('station') == "SNM00048698") | \
        # Paris
        (col('station') == "FRE00104907") | \
        # Dubai
        (col('station') == "AEM00041194") | \
        # NY
        (col('station') == "USW00014732") | \
        # Kuala Lumpur
        (col('station') == "MYM00048650") | \
        # Instanbul
        (col('station') == "TUM00017064") | \
        # Dehlhi
        (col('station') == "IN022021900") | \
        # Antalya 
        (col('station') == "TUM00017300") | \
        # Shenzhen # NO DATA
        # Munbai # NO DATA
        # Phuket 
        (col('station') == "TH000048564") | \
        # Rome
        (col('station') == "IT000016239") | \
        # Tokyo
        (col('station') == "JA000047662") | \
        # Pattaya NO DATA
        # Taipei NO DATA
        # Mecca 
        (col('station') == "SAM00041030") | \
        # Guanzhou NO DATA
        # Prahue
        (col('station') == "EZM00011520") | \
        # Medina 
        (col('station') == "SA000040430") | \
        # Seoul
        (col('station') == "KSM00047108") | \
        # Amsterdam NO DATA
        # Agra NO DATA
        # Miami
        (col('station') == "USW00012839") | \
        # Osaka
        (col('station') == "JAM00047772") | \
        # Las Vegas 
        (col('station') == "USW00053123") | \
        # BARCELONA
        (col('station') == "SPE00155991") | \
        # WIEN
        (col('station') == "AU000005901") | \
        # SHANGHAI
        (col('station') == "CHM00058362") | \
        # LA
        (col('station') == "USW00023174") | \

        # JAIPUR
        (col('station') == "IN019131301") | \
        # ORLANDO
        (col('station') == "USW00012815") | \
        # MOSCOW
        (col('station') == "RSM00027612") | \
        # MADRID
        (col('station') == "SP000003195") | \
        # DUBLIN
        (col('station') == "EI000003969") | \
        # JERESALEM
        (col('station') == "IS000006771") | \
        # JOHANNESBURG
        (col('station') == "SFM00068368") | \
        # SYDNEY
        (col('station') == "ASN00066062") | \
        # JAKARTA
        (col('station') == "ID000096745") | \
        # MUNICH
        (col('station') == "GM000004199") | \
        # TRONTO
        (col('station') == "CA006158355") | \
        # BUDAPEST
        (col('station') == "HUE00100247") | \
        # BEIJING
        (col('station') == "CHM00054511") | \
        # ST. PETERSBURG
        (col('station') == "RSM00026063") | \
        # LISBON
        (col('station') == "PO000008535") \
    )
    
    popular_cities_data.write.json(out_directory, compression=None, mode='overwrite')

if __name__ == "__main__":
    # python3 get_popular_cities_data.py ghcn_tmax ghcn_tmin ghcn_prcp popular_cities_out
    in_directory_1 = sys.argv[1] 
    in_directory_2 = sys.argv[2]
    in_directory_3 = sys.argv[3]
    out_directory = sys.argv[4]

    main(in_directory_1, in_directory_2, in_directory_3, out_directory)
