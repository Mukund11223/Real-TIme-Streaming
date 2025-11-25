from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder \
    .appName('fruit_data') \
    .master('local[3]') \
    .config('spark.streaming.stopGracefullyOnShutdown','true') \
    .config('spark.sql.shuffle.partitions','2') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

df=spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers','localhost:9092') \
    .option('subscribe','fruit_data') \
    .option('startingoffsets','latest') \
    .load()

schema = StructType([
    StructField('id',IntegerType()),
    StructField('value',StringType()),
    StructField('timestamp',TimestampType())
])

df=df.select(from_json(col('value').cast('string'),schema).alias('data')).select('data.*')

## read the static data
df_static = spark.read.format('csv').option('inferschema','true').option('header','true').load('/tmp/input/fruit_data.csv')

## join it with the df to pruduce category in the data as fruit_dim
df_joined=df.join(df_static,df.id==df_static.id,'inner').drop(df_static.id)

query=df_joined.writeStream \
    .outputMode('append') \
    .format('console') \
    .option('truncate','false') \
    .start()

query.awaitTermination()
